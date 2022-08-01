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
package org.apache.kylin.query.runtime.plan

import com.google.common.base.Joiner
import com.google.common.collect.{Lists, Sets}
import org.apache.kylin.common.{KapConfig, KylinConfig, QueryContext}
import org.apache.kylin.engine.spark.utils.{LogEx, LogUtils}
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate
import org.apache.kylin.metadata.cube.gridtable.NLayoutToGridTableMapping
import org.apache.kylin.metadata.cube.model.{LayoutEntity, NDataSegment, NDataflow}
import org.apache.kylin.metadata.cube.realization.HybridRealization
import org.apache.kylin.metadata.model.NTableMetadataManager
import org.apache.kylin.query.util.{RuntimeHelper, SparderDerivedUtil}
import org.apache.kylin.metadata.model._
import org.apache.kylin.metadata.tuple.TupleInfo
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.manager.SparderLookupManager
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{DataFrame, _}

import java.util.concurrent.ConcurrentHashMap
import java.{lang, util}
import org.apache.kylin.metadata.realization.IRealization
import org.apache.kylin.query.implicits.sessionToQueryContext
import org.apache.kylin.query.relnode.{KapRel, OLAPContext}

import scala.collection.JavaConverters._

// scalastyle:off
object TableScanPlan extends LogEx {

  def listSegmentsForQuery(cube: NDataflow): util.List[NDataSegment] = {
    val r = new util.ArrayList[NDataSegment]
    import scala.collection.JavaConversions._
    for (seg <- cube.getQueryableSegments) {
      r.add(seg)
    }
    r
  }

  private[runtime] val cacheDf: ThreadLocal[ConcurrentHashMap[String, DataFrame]] = new ThreadLocal[ConcurrentHashMap[String, DataFrame]] {
    override def initialValue: ConcurrentHashMap[String, DataFrame] = {
      new ConcurrentHashMap[String, DataFrame]
    }
  }

  def createOLAPTable(rel: KapRel): DataFrame = logTime("table scan", debug = true) {
    val session: SparkSession = SparderEnv.getSparkSession
    val olapContext = rel.getContext
    val context = olapContext.storageContext
    val prunedSegments = context.getPrunedSegments
    val prunedStreamingSegments = context.getPrunedStreamingSegments
    val realizations = olapContext.realization.getRealizations.asScala.toList
    realizations.map(_.asInstanceOf[NDataflow])
      .filter(dataflow => (!dataflow.isStreaming && !context.isBatchCandidateEmpty) ||
        (dataflow.isStreaming && !context.isStreamCandidateEmpty))
      .map(dataflow => {
        if (dataflow.isStreaming) {
          tableScan(rel, dataflow, olapContext, session, prunedStreamingSegments, context.getStreamingCandidate)
        } else {
          tableScan(rel, dataflow, olapContext, session, prunedSegments, context.getCandidate)
        }
      }).reduce(_.union(_))
  }

  def tableScan(rel: KapRel, dataflow: NDataflow, olapContext: OLAPContext,
                session: SparkSession, prunedSegments: util.List[NDataSegment], candidate: NLayoutCandidate): DataFrame = {
    val prunedPartitionMap = olapContext.storageContext.getPrunedPartitions
    olapContext.resetSQLDigest()
    //TODO: refactor
    val cuboidLayout = candidate.getLayoutEntity
    if (cuboidLayout.getIndex.isTableIndex) {
      QueryContext.current().getQueryTagInfo.setTableIndex(true)
    }
    val tableName = olapContext.firstTableScan.getBackupAlias
    val mapping = new NLayoutToGridTableMapping(cuboidLayout)
    val columnNames = SchemaProcessor.buildGTSchema(cuboidLayout, mapping, tableName)

    /////////////////////////////////////////////
    val kapConfig = KapConfig.wrap(dataflow.getConfig)
    val basePath = kapConfig.getReadParquetStoragePath(dataflow.getProject)
    val fileList = prunedSegments.asScala.map(
      seg => toLayoutPath(dataflow, cuboidLayout.getId, basePath, seg, prunedPartitionMap)
    )
    val path = fileList.mkString(",") + olapContext.isExactlyFastBitmap()
    printLogInfo(basePath, dataflow.getId, cuboidLayout.getId, prunedSegments, prunedPartitionMap)

    val cached = cacheDf.get().getOrDefault(path, null)
    val df = if (cached != null && !cached.sparkSession.sparkContext.isStopped) {
      logInfo(s"Reuse df: ${cuboidLayout.getId}")
      cached
    } else {
      val pruningInfo = prunedSegments.asScala.map { seg =>
        if (prunedPartitionMap != null) {
          val partitions = prunedPartitionMap.get(seg.getId)
          seg.getId + ":" + Joiner.on("|").join(partitions)
        } else {
          seg.getId
        }
      }.mkString(",")
      val newDf = session.kylin
        .isFastBitmapEnabled(olapContext.isExactlyFastBitmap())
        .bucketingEnabled(bucketEnabled(olapContext, cuboidLayout))
        .cuboidTable(dataflow, cuboidLayout, pruningInfo)
        .toDF(columnNames: _*)
      logInfo(s"Cache df: ${cuboidLayout.getId}")
      cacheDf.get().put(path, newDf)
      newDf
    }

    val (schema, newDF) = buildSchema(df, tableName, cuboidLayout, rel, olapContext, dataflow)
    newDF.select(schema: _*)
  }

  def bucketEnabled(context: OLAPContext, layout: LayoutEntity): Boolean = {
    if (!KylinConfig.getInstanceFromEnv.isShardingJoinOptEnabled) {
      return false
    }
    // no extra agg is allowed
    if (context.isHasAgg && !context.isExactlyAggregate) {
      return false
    }

    // check if outer join key matches shard by key
    (context.getOuterJoinParticipants.size() == 1
      && layout.getShardByColumnRefs.size() == 1
      && context.getOuterJoinParticipants.iterator().next() == layout.getShardByColumnRefs.get(0))
  }

  def buildSchema(df: DataFrame, tableName: String, cuboidLayout: LayoutEntity, rel: KapRel,
                  olapContext: OLAPContext, dataflow: NDataflow): (Seq[Column], DataFrame) = {
    var newDF = df
    val isBatchOfHybrid = olapContext.realization.isInstanceOf[HybridRealization] && dataflow.getModel.isFusionModel && !dataflow.isStreaming
    val mapping = new NLayoutToGridTableMapping(cuboidLayout, isBatchOfHybrid)
    val context = olapContext.storageContext
    /////////////////////////////////////////////
    val groups: util.Collection[TblColRef] =
      olapContext.getSQLDigest.groupbyColumns
    val otherDims = Sets.newHashSet(context.getDimensions)
    otherDims.removeAll(groups)
    // expand derived (xxxD means contains host columns only, derived columns were translated)
    val groupsD = expandDerived(context.getCandidate, groups)
    val otherDimsD: util.Set[TblColRef] =
      expandDerived(context.getCandidate, otherDims)
    otherDimsD.removeAll(groupsD)

    // identify cuboid
    val dimensionsD = new util.LinkedHashSet[TblColRef]
    dimensionsD.addAll(groupsD)
    dimensionsD.addAll(otherDimsD)
    val model = context.getCandidate.getLayoutEntity.getModel
    context.getCandidate.getDerivedToHostMap.asScala.toList.foreach(m => {
      if (m._2.`type` == DeriveInfo.DeriveType.LOOKUP && !m._2.isOneToOne) {
        m._2.columns.asScala.foreach(derivedId => {
          if (mapping.getIndexOf(model.getColRef(derivedId)) != -1) {
            dimensionsD.add(model.getColRef(derivedId))
          }
        })
      }
    })
    val gtColIdx = mapping.getDimIndices(dimensionsD) ++ mapping
      .getMetricsIndices(context.getMetrics)

    val derived = SparderDerivedUtil(tableName,
      dataflow.getLatestReadySegment,
      gtColIdx,
      olapContext.returnTupleInfo,
      context.getCandidate)
    if (derived.hasDerived) {
      newDF = derived.joinDerived(newDF)
    }
    var topNMapping: Map[Int, Column] = Map.empty
    // query will only has one Top N measure.
    val topNMetric = context.getMetrics.asScala.collectFirst {
      case x: FunctionDesc if x.getReturnType.startsWith("topn") => x
    }
    if (topNMetric.isDefined) {
      val topNFieldIndex = mapping.getMetricsIndices(List(topNMetric.get).asJava).head
      val tp = processTopN(topNMetric.get, newDF, topNFieldIndex, olapContext.returnTupleInfo, tableName)
      newDF = tp._1
      topNMapping = tp._2
    }
    val tupleIdx = getTupleIdx(dimensionsD,
      context.getMetrics,
      olapContext.returnTupleInfo)
    (RuntimeHelper.gtSchemaToCalciteSchema(
      mapping.getPrimaryKey,
      derived,
      tableName,
      rel.getColumnRowType.getAllColumns.asScala.toList,
      df.schema,
      gtColIdx,
      tupleIdx,
      topNMapping), newDF)
  }

  def toLayoutPath(dataflow: NDataflow, cuboidId: Long, basePath: String, seg: NDataSegment): String = {
    s"$basePath${dataflow.getUuid}/${seg.getId}/$cuboidId"
  }

  def toLayoutPath(dataflow: NDataflow, layoutId: Long, basePath: String, seg: NDataSegment, partitionsMap: util.Map[String, util.List[lang.Long]]): List[String] = {
    if (partitionsMap == null) {
      List(toLayoutPath(dataflow, layoutId, basePath, seg))
    } else {
      partitionsMap.get(seg.getId).asScala.map(part => {
        val bucketId = dataflow.getSegment(seg.getId).getBucketId(layoutId, part)
        val childDir = if (bucketId == null) part else bucketId
        toLayoutPath(dataflow, layoutId, basePath, seg) + "/" + childDir
      }).toList
    }
  }

  def printLogInfo(basePath: String, dataflowId: String, cuboidId: Long, prunedSegments: util.List[NDataSegment], partitionsMap: util.Map[String, util.List[lang.Long]]) {
    if (partitionsMap == null) {
      val segmentIDs = LogUtils.jsonArray(prunedSegments.asScala)(e => s"${e.getId} [${e.getSegRange.getStart}, ${e.getSegRange.getEnd})")
      logInfo(s"""Path is: {"base":"$basePath","dataflow":"${dataflowId}","segments":$segmentIDs,"layout": ${cuboidId}""")
    } else {
      val prunedSegmentInfo = partitionsMap.asScala.map {
        case (segmentId, partitionList) => {
          "[" + segmentId + ": " + partitionList.asScala.mkString(",") + "]"
        }
      }.mkString(",")
      logInfo(s"""Path is: {"base":"$basePath","dataflow":"${dataflowId}","segments":{$prunedSegmentInfo},"layout": ${cuboidId}""")
    }
    logInfo(s"size is ${cacheDf.get().size()}")
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
          Lists.newArrayList(ParameterDesc.newInstance(numericCol)), numericCol.getType.toString)
        tupleInfo.getFieldIndex(sumFunc.getRewriteFieldName)
      } else {
        val countFunction = FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT,
          Lists.newArrayList(ParameterDesc.newInstance("1")), "bigint")
        tupleInfo.getFieldIndex(countFunction.getRewriteFieldName)
      }

    val dimCols = topNLiteralColumn.toArray
    val dimWithType = literalTupleIdx.zipWithIndex.map(index => {
      val column = dimCols(index._2)
      (SchemaProcessor.genTopNSchema(tableName,
        index._1, column.getIdentity.replaceAll("\\.", "_")),
        SparderTypeUtil.toSparkType(column.getType))
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
    val allCols = functionDesc.getColRefs
    if (!functionDesc.getParameters.get(0).isColumnType) {
      return allCols.asScala.toList
    }
    allCols.asScala.drop(1).toList
  }

  private def getTopNNumericColumn(functionDesc: FunctionDesc): TblColRef = {
    if (functionDesc.getParameters.get(0).isColumnType) {
      return functionDesc.getColRefs.get(0)
    }
    null
  }

  // copy from NCubeTupleConverter
  def getTupleIdx(
                   selectedDimensions: util.Set[TblColRef],
                   selectedMetrics: util.Set[FunctionDesc],
                   tupleInfo: TupleInfo): Array[Int] = {
    var tupleIdx: Array[Int] =
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
          val col = metric.getColRefs.get(0)
          tupleIdx(i) =
            if (tupleInfo.hasColumn(col)) tupleInfo.getColumnIndex(col) else -1
        }
        i += 1
      }
    )
    tupleIdx
  }

  def createLookupTable(rel: KapRel): DataFrame = {
    val start = System.currentTimeMillis()

    val session = SparderEnv.getSparkSession
    val olapContext = rel.getContext
    var instance: IRealization = null
    if (olapContext.realization.isInstanceOf[NDataflow])
    {
      instance = olapContext.realization.asInstanceOf[NDataflow]
    } else {
      instance = olapContext.realization.asInstanceOf[HybridRealization]
    }

    val tableMetadataManager = NTableMetadataManager.getInstance(instance.getConfig, instance.getProject)
    val lookupTableName = olapContext.firstTableScan.getTableName
    val snapshotResPath = tableMetadataManager.getTableDesc(lookupTableName).getLastSnapshotPath
    val config = instance.getConfig
    val dataFrameTableName = instance.getProject + "@" + lookupTableName
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
            )
          })
    val df = newNameLookupDf.select(colIndex: _*)
    logInfo(s"Gen lookup table scan cost Time :${System.currentTimeMillis() - start} ")
    df
  }

  private def expandDerived(layoutCandidate: NLayoutCandidate,
                            cols: util.Collection[TblColRef]): util.Set[TblColRef] = {
    val expanded = new util.HashSet[TblColRef]
    val model = layoutCandidate.getLayoutEntity.getModel
    val tblIdMap = model.getEffectiveCols.inverse()
    cols.asScala.foreach(
      col => {
        val hostInfo = layoutCandidate.getDerivedToHostMap.get(tblIdMap.get(col))
        if (hostInfo != null) {
          for (hostCol <- hostInfo.columns.asScala) {
            expanded.add(model.getColRef(hostCol))
          }
        } else {
          expanded.add(col)
        }
      }
    )
    expanded
  }

  def createSingleRow(): DataFrame = {
    val session = SparderEnv.getSparkSession
    val rows = List.fill(1)(Row.fromSeq(List[Object]()))
    val rdd = session.sparkContext.makeRDD(rows)
    session.createDataFrame(rdd, StructType(List[StructField]()))
  }
}
