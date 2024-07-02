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

import java.util.concurrent.ConcurrentHashMap
import java.{lang, util}

import org.apache.commons.collections.CollectionUtils
import org.apache.kylin.common.util.ClassUtil
import org.apache.kylin.common.{KapConfig, KylinConfig, QueryContext}
import org.apache.kylin.engine.spark.utils.{LogEx, LogUtils}
import org.apache.kylin.guava30.shaded.common.base.Joiner
import org.apache.kylin.guava30.shaded.common.collect.{Lists, Sets}
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate
import org.apache.kylin.metadata.cube.gridtable.NLayoutToGridTableMapping
import org.apache.kylin.metadata.cube.model.{LayoutEntity, NDataSegment, NDataflow}
import org.apache.kylin.metadata.model._
import org.apache.kylin.metadata.realization.HybridRealization
import org.apache.kylin.metadata.tuple.TupleInfo
import org.apache.kylin.query.implicits.sessionToQueryContext
import org.apache.kylin.query.plugin.runtime.MppOnTheFlyProvider
import org.apache.kylin.query.relnode.{OlapContext, OlapRel, OlapTableScan}
import org.apache.kylin.query.util.{RuntimeHelper, SparderDerivedUtil}
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Union}
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.manager.SparderLookupManager
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{Column, DataFrame, Row, SparderEnv, SparkInternalAgent, SparkOperation, SparkSession}

import java.util.concurrent.ConcurrentHashMap
import java.{lang, util}
import scala.collection.JavaConverters._


// scalastyle:off
object TableScanPlan extends LogEx {

  private[runtime] val cachePlan: ThreadLocal[ConcurrentHashMap[String, LogicalPlan]] = new ThreadLocal[ConcurrentHashMap[String, LogicalPlan]] {
    override def initialValue: ConcurrentHashMap[String, LogicalPlan] = {
      new ConcurrentHashMap[String, LogicalPlan]
    }
  }

  def createOlapTable(rel: OlapRel): LogicalPlan = logTime("table scan", debug = true) {
    val session: SparkSession = SparderEnv.getSparkSession
    val olapContext = rel.getContext
    val storage = olapContext.getStorageContext
    val batchSeg = storage.getBatchCandidate.getPrunedSegments
    val streamSeg = storage.getStreamCandidate.getPrunedSegments
    val realizations = olapContext.getRealization.getRealizations.asScala.toList
    val plans = realizations.map(_.asInstanceOf[NDataflow])
      .filter(dataflow => {
        (!dataflow.isStreaming && !storage.isBatchCandidateEmpty) ||
          (dataflow.isStreaming && !storage.isStreamCandidateEmpty) ||
          isSegmentsEmpty(batchSeg, streamSeg)
      }).map(dataflow => {
        if (dataflow.isStreaming) {
          tableScan(rel, dataflow, olapContext, session, streamSeg, storage.getStreamCandidate)
        } else {
          tableScan(rel, dataflow, olapContext, session, batchSeg, storage.getBatchCandidate)
        }
      })

    // The reason why we use Project to package it here is because the output method of Union needs to be analyzed,
    // so it needs to be packaged by Project so that subsequent operators can easily obtain the output information of nodes.
    if (plans.size == 1) {
      plans.head
    } else {
      Project(plans.head.output, Union(plans))
    }
  }

  def createMetadataTable(rel: OlapRel): LogicalPlan = {
    val session: SparkSession = SparderEnv.getSparkSession
    val olapContext = rel.getContext
    val allFields: util.List[TblColRef] = new util.ArrayList[TblColRef]
    olapContext.getAllTableScans.forEach(tableScan => {
      val columns = tableScan.getColumnRowType.getAllColumns
      allFields.addAll(columns)
    })
    // convert data
    val dataSet = olapContext.getColValuesRange
    val result = new util.ArrayList[Row]
    dataSet.forEach(rowData => {
      val sparderData = new Array[Any](rowData.length)
      sparderData.indices.foreach(index => {
        val dataType = allFields.get(index).getColumnDesc.getUpgradedType
        sparderData(index) = SparderTypeUtil.convertStringToResultValueBasedOnKylinSQLType(rowData.apply(index), dataType)
      })
      result.add(Row.fromSeq(sparderData))
    })

    // create schema
    val structTypes = new util.ArrayList[StructField]()
    allFields.forEach(col => {
      try {
        val dataType = col.getColumnDesc.getUpgradedType
        val spaType = if (dataType.isDate) {
          DataTypes.DateType
        } else if (dataType.isDateTimeFamily) {
          DataTypes.TimestampType
        } else {
          SparderTypeUtil.kylinTypeToSparkResultType(dataType)
        }
        structTypes.add(StructField(col.getIdentity.replace(".", "_"), spaType))
      } catch {
        // some dataTypes are not support in sparder, such as 'any',
        // but these types can not be used in min/max, just return stringType
        case e: IllegalArgumentException => {
          structTypes.add(StructField(col.getIdentity.replace(".", "_"), StringType))
          logInfo(e.toString)
        }
      }
    })
    val schema: StructType = StructType(structTypes)

    session.createDataFrame(result, schema).queryExecution.logical
  }

  // prunedSegments is null
  private def tableScanEmptySegment(rel: OlapRel): LogicalPlan = {
    logInfo("prunedSegments is null")
    // KE-41874 DataFrame convert Logical Plan
    SparkOperation.createEmptyDataFrame(
      StructType(rel.getColumnRowType
        .getAllColumns.asScala
        .map(column => StructField(
          column.toString.replaceAll("\\.", "_"),
          SparderTypeUtil.toSparkType(column.getType))
        )
      )
    ).queryExecution.logical
  }

  def isSegmentsEmpty(prunedSegments: java.util.List[NDataSegment], prunedStreamingSegments: java.util.List[NDataSegment]): Boolean = {
    val isPrunedSegmentsEmpty = prunedSegments == null || prunedSegments.size() == 0
    val isPrunedStreamingSegmentsEmpty = prunedStreamingSegments == null || prunedStreamingSegments.size() == 0
    isPrunedSegmentsEmpty && isPrunedStreamingSegmentsEmpty
  }

  def tableScan(rel: OlapRel, dataflow: NDataflow, olapContext: OlapContext,
                session: SparkSession, prunedSegments: util.List[NDataSegment],
                candidate: NLayoutCandidate): LogicalPlan = {
    val prunedPartitionMap = olapContext.getStorageContext.getPrunedPartitions
    olapContext.resetSQLDigest()
    //TODO: refactor
    val cuboidLayout = candidate.getLayoutEntity
    if (cuboidLayout.getIndex != null && cuboidLayout.getIndex.isTableIndex) {
      QueryContext.current().getQueryTagInfo.setTableIndex(true)
    }
    val tableName = olapContext.getFirstTableScan.getBackupAlias
    val mapping = new NLayoutToGridTableMapping(cuboidLayout)
    val columnNames = SchemaProcessor.buildGTSchema(cuboidLayout, mapping, tableName)

    val kapConfig = KapConfig.wrap(dataflow.getConfig)
    val mppOnTheFly = getMppOnTheFlyProvider(dataflow.getConfig)

    /////////////////////////////////////////////
    val basePath = kapConfig.getReadParquetStoragePath(dataflow.getProject)
    if (prunedSegments == null || prunedSegments.size() == 0) {
      return tableScanEmptySegment(rel: OlapRel)
    }
    val fileList = prunedSegments.asScala.map(
      seg => toLayoutPath(dataflow, cuboidLayout.getId, basePath, seg, prunedPartitionMap)
    )
    val path = fileList.mkString(",") + olapContext.isExactlyFastBitmap
    printLogInfo(basePath, dataflow.getId, cuboidLayout.getId, prunedSegments, prunedPartitionMap)

    val pruningInfo = prunedSegments.asScala.map { seg =>
      if (prunedPartitionMap != null && CollectionUtils.isNotEmpty(prunedPartitionMap.get(seg.getId))) {
        val partitions = prunedPartitionMap.get(seg.getId)
        seg.getId + ":" + Joiner.on("|").join(partitions)
      } else {
        seg.getId
      }
    }.mkString(",")

    val cached = cachePlan.get().getOrDefault(path, null)
    var plan = if (cached != null && !SparderEnv.getSparkSession.sparkContext.isStopped) {
      logInfo(s"Reuse plan: ${cuboidLayout.getId}")
      cached
    } else {
      val newPlan = session.kylin
        .isFastBitmapEnabled(olapContext.isExactlyFastBitmap)
        .bucketingEnabled(bucketEnabled(olapContext, cuboidLayout))
        .cuboidTable(dataflow, cuboidLayout, pruningInfo)

      val mppPlan = mppOnTheFly.computeMissingLayout(prunedSegments, cuboidLayout.getId, session)

      val cuboidAndMppPlan =
        if (mppPlan == null) newPlan
        else newPlan.union(mppPlan)

      cachePlan.get().put(path, cuboidAndMppPlan)
      cuboidAndMppPlan
    }

    plan = SparkOperation.projectAsAlias(columnNames, plan)
    val (schema, newPlan) = buildSchema(plan, tableName, cuboidLayout, rel, olapContext, dataflow)
    SparkOperation.project(schema, newPlan)
  }

  private def getMppOnTheFlyProvider(config: KylinConfig): MppOnTheFlyProvider = {
    var ret: MppOnTheFlyProvider = null
    if (config.isMppOnTheFlyLayoutsEnabled) {
      try {
        ret = ClassUtil.newInstance(config.getMppOnTheFlyLayoutsProvider).asInstanceOf[MppOnTheFlyProvider]
      } catch {
        case e: Exception => logError("failed to instantiate MppOnTheFlyProvider", e)
      }
    }
    if (ret == null)
      new MppOnTheFlyProvider {
        override def computeMissingLayout(prunedSegments: util.List[NDataSegment], layoutId: Long, ss: SparkSession): LogicalPlan = {
          null
        }
      }
    else
      ret
  }

  def bucketEnabled(context: OlapContext, layout: LayoutEntity): Boolean = {
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

  def buildSchema(plan: LogicalPlan, tableName: String, cuboidLayout: LayoutEntity, rel: OlapRel,
                  olapContext: OlapContext, dataflow: NDataflow): (Seq[Column], LogicalPlan) = {
    var newPlan = plan
    val isBatchOfHybrid = (olapContext.getRealization.isInstanceOf[HybridRealization]
      && dataflow.getModel.isFusionModel && !dataflow.isStreaming)
    val mapping = new NLayoutToGridTableMapping(cuboidLayout, isBatchOfHybrid)
    val context = olapContext.getStorageContext
    /////////////////////////////////////////////
    val groups: util.Collection[TblColRef] = olapContext.getSQLDigest.getGroupByColumns
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
      olapContext.getReturnTupleInfo,
      context.getCandidate)
    if (derived.hasDerived) {
      newPlan = derived.joinDerived(newPlan)
    }
    var topNMapping: Map[Int, Column] = Map.empty
    // query will only has one Top N measure.
    val topNMetric = context.getMetrics.asScala.collectFirst {
      case x: FunctionDesc if x.getReturnType.startsWith("topn") => x
    }
    if (topNMetric.isDefined) {
      val topNFieldIndex = mapping.getMetricsIndices(List(topNMetric.get).asJava).head

      val df = SparkInternalAgent.getDataFrame(SparderEnv.getSparkSession, newPlan)
      val tp = processTopN(topNMetric.get, df, topNFieldIndex, olapContext.getReturnTupleInfo, tableName)
      newPlan = tp._1.queryExecution.analyzed
      topNMapping = tp._2
    }
    val tupleIdx = getTupleIdx(dimensionsD,
      context.getMetrics,
      olapContext.getReturnTupleInfo)
    (RuntimeHelper.gtSchemaToCalciteSchema(
      mapping.getPrimaryKey,
      derived,
      tableName,
      rel.getColumnRowType.getAllColumns.asScala.toList,
      newPlan,
      (gtColIdx, tupleIdx),
      topNMapping), newPlan)
  }

  def toLayoutPath(dataflow: NDataflow, cuboidId: Long, basePath: String, seg: NDataSegment): String = {
    s"$basePath${dataflow.getUuid}/${seg.getId}/$cuboidId"
  }

  def toLayoutPath(dataflow: NDataflow, layoutId: Long, basePath: String,
                   seg: NDataSegment, partitionsMap: util.Map[String, util.List[lang.Long]]): List[String] = {
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
    logInfo(s"size is ${cachePlan.get().size()}")
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

  def createLookupTable(rel: OlapRel): LogicalPlan = {
    val start = System.currentTimeMillis()

    val olapContext = rel.getContext
    val config = olapContext.getOlapSchema.getConfig
    val project = olapContext.getOlapSchema.getProject

    val tableMetadataManager = NTableMetadataManager.getInstance(config, project)
    val lookupTableName = olapContext.getFirstTableScan.getTableName
    val snapshotResPath = tableMetadataManager.getTableDesc(lookupTableName).getLastSnapshotPath
    val dataFrameTableName = project + "@" + lookupTableName
    val lookupPlan = SparderLookupManager.getOrCreate(dataFrameTableName, snapshotResPath, config)

    val olapTable = olapContext.getFirstTableScan.getOlapTable
    val alisTableName = olapContext.getFirstTableScan.getBackupAlias
    val newNames = lookupPlan.output.map { c =>
      val gTInfoSchema = SchemaProcessor.parseDeriveTableSchemaName(c.name)
      val name = SchemaProcessor.generateDeriveTableSchemaName(alisTableName,
        gTInfoSchema.columnId,
        gTInfoSchema.columnName)
      name
    }
    val newNameLookupPlan = SparkOperation.projectAsAlias(newNames, lookupPlan)
    val colIndex = olapTable.getSourceColumns.asScala
      .map(
        column =>
          if (column.isComputedColumn || column.getZeroBasedIndex < 0) {
            RuntimeHelper.intOne.as(column.toString)
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
    val plan = SparkOperation.project(colIndex, newNameLookupPlan)
    logInfo(s"Gen lookup table scan cost Time :${System.currentTimeMillis() - start} ")
    plan
  }

  def createInternalTable(rel: OlapRel): LogicalPlan = {
    val sparkSession = SparderEnv.getSparkSession
    val tableScan = rel.asInstanceOf[OlapTableScan]
    val project = QueryContext.current().getProject
    val tableIdentity = tableScan.getTableName
    val allColumns = tableScan.getOlapTable.getSourceColumns.asScala
      .map(
        column =>
          if (column.isComputedColumn || column.getZeroBasedIndex < 0) {
            "1"
          } else {
            column.getBackTickName
          })
    val sql = f"select ${allColumns.mkString(",")} from INTERNAL_CATALOG.$project.$tableIdentity"
    sparkSession.sql(sql).queryExecution.analyzed
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

  def createSingleRow(): LogicalPlan = {
    val session = SparderEnv.getSparkSession
    val rows = List.fill(1)(Row.fromSeq(List[Object]()))
    val rdd = session.sparkContext.makeRDD(rows)
    session.createDataFrame(rdd, StructType(List[StructField]())).queryExecution.logical
  }
}
