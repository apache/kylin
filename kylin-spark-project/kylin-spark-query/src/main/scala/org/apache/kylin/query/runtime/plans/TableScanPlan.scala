/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.query.runtime.plans

import java.util
import java.util.concurrent.ConcurrentHashMap

import org.apache.calcite.DataContext
import org.apache.kylin.common.QueryContextFacade
import org.apache.kylin.cube.CubeInstance
import org.apache.kylin.metadata.model._
import org.apache.kylin.metadata.tuple.TupleInfo
import org.apache.kylin.query.SchemaProcessor
import org.apache.kylin.query.exception.UnsupportedQueryException
import org.apache.kylin.query.relnode.{OLAPRel, OLAPTableScan}
import org.apache.kylin.query.runtime.{DerivedProcess, RuntimeHelper, SparderLookupManager}
import org.apache.kylin.storage.hybrid.HybridInstance
import org.apache.kylin.storage.spark.HadoopFileStorageQuery
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}
import org.apache.spark.sql.utils.SparkTypeUtil
import org.apache.spark.sql.{DataFrame, SparderContext, _}
import org.apache.spark.utils.LogEx

import scala.collection.JavaConverters._

// scalastyle:off
object TableScanPlan extends LogEx {

  val cacheDf: ThreadLocal[ConcurrentHashMap[String, DataFrame]] = new ThreadLocal[ConcurrentHashMap[String, DataFrame]] {
    override def initialValue: ConcurrentHashMap[String, DataFrame] = {
      new ConcurrentHashMap[String, DataFrame]
    }
  }

  def createOLAPTable(rel: OLAPRel, dataContext: DataContext): DataFrame = logTime("table scan", info = true) {
    val olapContext = rel.getContext
    val realization = olapContext.realization
    QueryContextFacade.current()
      .setContextRealization(olapContext.id, realization.getName, realization.getStorageType)
    val cubeInstance: CubeInstance = realization match {
      case instance: CubeInstance => instance
      case instance: HybridInstance =>
        instance.getRealizations.toList.head.asInstanceOf[CubeInstance]
      case instance =>
        throw new UnsupportedQueryException("unsupported instance: " + instance)
    }

    olapContext.resetSQLDigest()
    val query = new HadoopFileStorageQuery(cubeInstance)
    val returnTupleInfo = olapContext.returnTupleInfo
    val request = query.getStorageQueryRequest(
      olapContext,
      returnTupleInfo)
    val cuboid = request.getCuboid
    val gridTableMapping = cuboid.getCuboidToGridTableMapping

    val columnIndex = gridTableMapping.getDimIndexes(request.getDimensions) ++
      gridTableMapping.getMetricsIndexes(request.getMetrics)
    val factTableAlias = olapContext.firstTableScan.getBackupAlias
    val schemaNames = SchemaProcessor.buildGTSchema(cuboid, factTableAlias)
    import org.apache.kylin.query.implicits.implicits._
    var df = SparderContext.getSparkSession.kylin
      .format("parquet")
      .cuboidTable(cubeInstance, cuboid)
      .toDF(schemaNames: _*)
    // may have multi TopN measures.
    val topNMeasureIndexes = df.schema.fields.map(_.dataType).zipWithIndex.filter(_._1.isInstanceOf[ArrayType]).map(_._2)
    val tuple = DerivedProcess.process(olapContext, cuboid, cubeInstance, df, request)
    df = tuple._1
    var topNMapping: Map[Int, Column] = Map.empty
    val tupleIdx = getTupleIdx(request.getDimensions, request.getMetrics, olapContext.returnTupleInfo)
    // query will only has one Top N measure.
    val topNMetric = request.getMetrics.asScala.collectFirst {
      case x: FunctionDesc if x.getReturnType.startsWith("topn") => x
    }
    if (topNMetric.isDefined) {
      val topNFieldIndex = cuboid.getCuboidToGridTableMapping.getMetricsIndexes(List(topNMetric.get).asJava).head
      val tp = processTopN(topNMetric.get, df, topNFieldIndex, olapContext.returnTupleInfo, factTableAlias)
      df = tp._1
      topNMapping = tp._2
    }
    val columns = RuntimeHelper.gtSchemaToCalciteSchema(cuboid.getCuboidToGridTableMapping.getPrimaryKey,
      tuple._2,
      factTableAlias,
      rel.getColumnRowType.getAllColumns.asScala.toList,
      df.schema,
      columnIndex,
      tupleIdx,
      topNMapping,
      topNMeasureIndexes)
    df.select(columns: _*)
  }

  private def processTopN(topNMetric: FunctionDesc, df: DataFrame, topNFieldIndex: Int, tupleInfo: TupleInfo, tableName: String): (DataFrame, Map[Int, Column]) = {
    // support TopN measure
    val topNField = df.schema.fields
      .zipWithIndex
      .filter(_._1.dataType.isInstanceOf[ArrayType])
      .map(_.swap)
      .toMap
      .get(topNFieldIndex)
    require(topNField.isDefined)
    // data like this:
    //   [2012-01-01,4972.2700,WrappedArray([623.45,[10000392,7,2012-01-01]],[47.49,[10000029,4,2012-01-01]])]

    // inline array, one record may output multi records:
    //   [2012-01-01, 4972.2700, 623.45,[10000392,7,2012-01-01]]
    //   [2012-01-01, 4972.2700, 47.49,[10000029,4,2012-01-01]]

    val inlinedSelectExpr = df.schema.fields.filter(_ != topNField.get).map(_.name) :+ s"inline(${topNField.get.name})"
    val inlinedDF = df.selectExpr(inlinedSelectExpr: _*)

    // flatten multi dims in TopN measure, will not increase record number, a total flattened struct:
    //   [2012-01-01, 4972.2700, 623.45, 10000392, 7, 2012-01-01]
    val flattenedSelectExpr = inlinedDF.schema.fields.dropRight(1).map(_.name) :+ s"${inlinedDF.schema.fields.last.name}.*"
    val flattenedDF = inlinedDF.selectExpr(flattenedSelectExpr: _*)

    val topNLiteralColumn = getTopNLiteralColumn(topNMetric)

    val literalTupleIdx = topNLiteralColumn.filter(tupleInfo.hasColumn).map(tupleInfo.getColumnIndex)

    val numericCol = getTopNNumericColumn(topNMetric)
    val numericTupleIdx: Int =
      if (numericCol != null) {
        // for TopN, the aggr must be SUM
        val sumFunc = FunctionDesc.newInstance(FunctionDesc.FUNC_SUM,
          ParameterDesc.newInstance(numericCol), numericCol.getType.toString)
        tupleInfo.getFieldIndex(sumFunc.getRewriteFieldName)
      } else {
        val countFunction = FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT,
          ParameterDesc.newInstance("1"), "bigint")
        tupleInfo.getFieldIndex(countFunction.getRewriteFieldName)
      }

    val dimCols = topNLiteralColumn.toArray
    val dimWithType = literalTupleIdx.zipWithIndex.map(index => {
      val column = dimCols(index._2)
      (SchemaProcessor.genTopNSchema(tableName,
        index._1, column.getIdentity.replaceAll("\\.", "_")),
        SparkTypeUtil.toSparkType(column.getType))
    })

    val sumCol = tupleInfo.getAllColumns.get(numericTupleIdx)
    val sumColName = s"A_SUM_${sumCol.getName}_$numericTupleIdx"
    val measureSchema = StructField(sumColName, DoubleType)
    // flatten schema.
    val newSchema = StructType(df.schema.filter(_.name != topNField.get.name)
      ++ dimWithType.map(tp => StructField(tp._1, tp._2)).+:(measureSchema))

    val topNMapping = literalTupleIdx.zipWithIndex.map(index => {
      (index._1, col(SchemaProcessor.genTopNSchema(tableName,
        index._1, dimCols(index._2).getIdentity.replaceAll("\\.", "_"))))
    }).+:(numericTupleIdx, col(sumColName)).toMap

    (flattenedDF.toDF(newSchema.fieldNames: _*), topNMapping)
  }

  private def getTopNLiteralColumn(functionDesc: FunctionDesc): List[TblColRef] = {
    if (!functionDesc.getParameter.isColumnType) {
      return functionDesc.getParameter.getColRefs.asScala.toList
    }
    functionDesc.getParameter.getColRefs.asScala.drop(1).toList
  }

  private def getTopNNumericColumn(functionDesc: FunctionDesc): TblColRef = {
    if (functionDesc.getParameter.isColumnType) {
      return functionDesc.getParameter.getColRef
    }
    null
  }

  // copy from NCubeTupleConverter
  def getTupleIdx(selectedDimensions: util.Set[TblColRef],
                  selectedMetrics: util.Set[FunctionDesc],
                  tupleInfo: TupleInfo): Array[Int] = {
    val tupleIdx: Array[Int] =
      new Array[Int](selectedDimensions.size + selectedMetrics.size)

    var i = 0
    // pre-calculate dimension index mapping to tuple
    selectedDimensions.asScala.foreach(
      dim => {
        tupleIdx(i) =
          if (tupleInfo.hasColumn(dim)) tupleInfo.getColumnIndex(dim) else -1
        i += 1
      }
    )

    selectedMetrics.asScala.foreach(
      metric => {
        if (metric.needRewrite) {
          val rewriteFieldName = metric.getRewriteFieldName
          tupleIdx(i) =
            if (tupleInfo.hasField(rewriteFieldName))
              tupleInfo.getFieldIndex(rewriteFieldName)
            else -1
        } else { // a non-rewrite metrics (like sum, or dimension playing as metrics) is like a dimension column
          val col = metric.getParameter.getColRef
          tupleIdx(i) =
            if (tupleInfo.hasColumn(col)) tupleInfo.getColumnIndex(col) else -1
        }
        i += 1
      }
    )
    tupleIdx
  }

  def createLookupTable(rel: OLAPTableScan, dataContext: DataContext): DataFrame = {
    val start = System.currentTimeMillis()
    val olapContext = rel.getContext
    var cube: CubeInstance = null
    olapContext.realization match {
      case cube1: CubeInstance => cube = cube1
      case hybridInstance: HybridInstance =>
        val latestRealization = hybridInstance.getLatestRealization
        // scalastyle:off
        latestRealization match {
          case cube1: CubeInstance => cube = cube1
          case _ => throw new IllegalStateException
        }
      case _ =>
    }
    val lookupTableName = olapContext.firstTableScan.getTableName
    val snapshotResPath = cube.getLatestReadySegment.getSnapshotResPath(lookupTableName)
    val config = cube.getConfig
    val dataFrameTableName = cube.getProject + "@" + lookupTableName
    val lookupDf = SparderLookupManager.getOrCreate(dataFrameTableName,
      snapshotResPath,
      config)

    val olapTable = olapContext.firstTableScan.getOlapTable
    val alisTableName = olapContext.firstTableScan.getBackupAlias
    val newNames = lookupDf.schema.fieldNames.map { name =>
      val gTInfoSchema = SchemaProcessor.parseDeriveTableSchemaName(name)
      SchemaProcessor.generateDeriveTableSchemaName(alisTableName,
        gTInfoSchema.columnId,
        gTInfoSchema.columnName)
    }.array
    val newNameLookupDf = lookupDf.toDF(newNames: _*)
    val colIndex = olapTable.getSourceColumns.asScala
      .map(
        column =>
          if (column.isComputedColumn || column.getZeroBasedIndex < 0) {
            RuntimeHelper.literalOne.as(column.toString)
          } else {
            col(
              SchemaProcessor
                .generateDeriveTableSchemaName(
                  alisTableName,
                  column.getZeroBasedIndex,
                  column.getName
                )
                .toString
            )
          })
    val df = newNameLookupDf.select(colIndex: _*)
    logInfo(s"Gen lookup table scan cost Time :${System.currentTimeMillis() - start} ")
    df
  }

  def createSingleRow(rel: OLAPRel, dataContext: DataContext): DataFrame = {
    val session = SparderContext.getSparkSession
    val rows = List.fill(1)(Row.fromSeq(List[Object]()))
    val rdd = session.sparkContext.makeRDD(rows)
    session.createDataFrame(rdd, StructType(List[StructField]()))
  }
}
