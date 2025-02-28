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

package org.apache.kylin.engine.spark.job.step.build

import java.math.BigInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.{Locale, Objects, Timer, TimerTask}

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.engine.spark.application.SparkApplication
import org.apache.kylin.engine.spark.builder.DFBuilderHelper._
import org.apache.kylin.engine.spark.builder._
import org.apache.kylin.engine.spark.builder.v3dict.DictionaryBuilder
import org.apache.kylin.engine.spark.job.step.{ParamPropagation, StageExec}
import org.apache.kylin.engine.spark.job.{FiltersUtil, SegmentJob, TableMetaManager}
import org.apache.kylin.engine.spark.model.SegmentFlatTableDesc
import org.apache.kylin.engine.spark.model.planner.{CuboIdToLayoutUtils, FlatTableToCostUtils}
import org.apache.kylin.engine.spark.smarter.IndexDependencyParser
import org.apache.kylin.engine.spark.utils.{LogEx, SparkConfHelper, SparkDataSource}
import org.apache.kylin.guava30.shaded.common.collect.Sets
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree.AdaptiveTreeBuilder
import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.kylin.metadata.cube.planner.CostBasePlannerUtils
import org.apache.kylin.metadata.model._
import org.apache.spark.sql.KapFunctions.dict_encode_v3
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.utils.ProxyThreadUtils

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.duration.{Duration, MILLISECONDS}
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.{Failure, Success, Try}

abstract class FlatTableStage(private val jobContext: SegmentJob,
                              private val dataSegment: NDataSegment,
                              private val params: ParamPropagation)
  extends BuildStage(jobContext, dataSegment, params) with StageExec {
  protected lazy final val indexPlan = tableDesc.getIndexPlan
  protected lazy final val segmentRange = tableDesc.getSegmentRange
  protected lazy final val flatTablePath = tableDesc.getFlatTablePath

  import FlatTableStage._

  protected lazy final val factViewPath = tableDesc.getFactTableViewPath
  protected lazy final val workingDir = tableDesc.getWorkingDir
  protected lazy final val sampleRowCount = tableDesc.getSampleRowCount
  protected lazy final val FLAT_TABLE = params.getFlatTable
  // light flat-table dataset: eg. without encoding.
  private lazy final val LIGHT_FLAT_TABLE = params.getLightFlatTable
  protected lazy val factTable: Dataset[Row] = params.getFactTable
  // By design, COMPUTED-COLUMN could only be defined on fact table.
  protected lazy val factTableCCs: Set[TblColRef] = factTableRef.getColumns.asScala
    .filter(_.getColumnDesc.isComputedColumn)
    .toSet
  // Flat table.
  private lazy val canPersistFlatTable = tableDesc.shouldPersistFlatTable()
  private lazy val isReady4FlatTable = dataSegment.isFlatTableReady && tableDesc.buildFilesSeparationPathExists(flatTablePath)
  // Fact table view.
  private lazy val isFactView = factTableRef.getTableDesc.isView
  private lazy val canPersistFactView = tableDesc.shouldPersistView()
  private lazy val isReady4FactView = dataSegment.isFactViewReady && HadoopUtil.getWorkingFileSystem.exists(factViewPath)
  private lazy val canJoinLookups = {
    val canJoin = tableDesc.shouldJoinLookupTables
    logInfo(s"Segment $segmentId flat table need join: $canJoin")
    canJoin
  }
  // part of flat table, eg. partitioned, filtered.
  private lazy val partFactTable = params.getPartFactTable
  protected lazy val factTableRef: TableRef = dataModel.getRootFactTable
  protected lazy val joinTableSeq: Seq[JoinTableDesc] = dataModel.getJoinTables.asScala.filter(isTableToBuild)

  override def getJobContext: SparkApplication = jobContext

  def getLightFlatTable: Dataset[Row] = {
    LIGHT_FLAT_TABLE
  }

  def getFlatTable: Dataset[Row] = {
    FLAT_TABLE
  }

  def gatherStats(): Statistics = {
    val stepDesc = s"Segment $segmentId collect flat table statistics."
    logInfo(stepDesc)
    sparkSession.sparkContext.setJobDescription(stepDesc)
    val stats = gatherStats(FLAT_TABLE)
    logInfo(s"Segment $segmentId collect flat table statistics $stats.")
    sparkSession.sparkContext.setJobDescription(null)
    stats
  }

  protected final def gatherStats(table: Dataset[Row]): Statistics = {
    val totalRowCount = table.count()
    if (!canPersistFlatTable) {
      // By design, evaluating column bytes should be based on existed flat table.
      logInfo(s"Flat table not persisted, only compute row count.")
      return Statistics(totalRowCount, Map.empty[String, Long])
    }
    // zipWithIndex before filter
    val canonicalIndices = table.columns //
      .zipWithIndex //
      .filterNot(_._1.endsWith(ENCODE_SUFFIX)) //
      .map { case (name, index) =>
        val canonical = tableDesc.getCanonicalName(Integer.parseInt(name))
        (canonical, index)
      }.filterNot(t => Objects.isNull(t._1))
    logInfo(s"CANONICAL INDICES ${canonicalIndices.mkString("[", ", ", "]")}")
    // By design, action-take is not sampling.
    val sampled = table.take(sampleRowCount).flatMap(row => //
      canonicalIndices.map { case (canonical, index) => //
        val bytes = utf8Length(row.get(index))
        (canonical, bytes) //
      }).groupBy(_._1).mapValues(_.map(_._2).sum)
    val evaluated = evaluateColumnBytes(totalRowCount, sampled)
    Statistics(totalRowCount, evaluated)
  }

  protected def generateCostTable(): (java.util.Map[BigInteger, java.lang.Long], Long) = {
    val rowkeyCount = indexPlan.getRuleBasedIndex.countOfIncludeDimension()
    val stepDesc = s"Segment $segmentId generate the cost for the planner from the flat table, " +
      s"rowkey count is $rowkeyCount"
    logInfo(stepDesc)
    sparkSession.sparkContext.setJobDescription(stepDesc)
    // get the cost from the flat table
    val javaRddFlatTable = FLAT_TABLE.javaRDD
    // log dimension and table desc
    logInfo(s"Segment $segmentId calculate the cost, the dimension in rule index is: " +
      s"${indexPlan.getRuleBasedIndex.getDimensions}, " +
      s"the column in flat table is: ${tableDesc.getColumnIds}")
    val cuboIdsCost = FlatTableToCostUtils.generateCost(javaRddFlatTable, config, indexPlan.getRuleBasedIndex, tableDesc)
    // get the count for the flat table
    val sourceCount = FLAT_TABLE.count()
    logInfo(s"The total source count is $sourceCount")
    sparkSession.sparkContext.setJobDescription(null)
    val cuboIdToRowCount = FlatTableToCostUtils.getCuboidRowCountMapFromSampling(cuboIdsCost)
    (cuboIdToRowCount, sourceCount)
  }

  protected def getRecommendedLayoutAndUpdateMetadata(cuboIdToRowCount: java.util.Map[BigInteger, java.lang.Long],
                                                      sourceCount: Long): Unit = {
    logDebug(s"Segment $segmentId get the row count cost $cuboIdToRowCount")
    val cuboIdToSize = FlatTableToCostUtils.
      getCuboidSizeMapFromSampling(cuboIdToRowCount, sourceCount, indexPlan.getRuleBasedIndex, config, tableDesc)
    logDebug(s"Segment $segmentId get the size cost $cuboIdToSize")
    val cuboids = CostBasePlannerUtils.
      getRecommendCuboidList(indexPlan.getRuleBasedIndex, config, dataModel.getAlias, cuboIdToRowCount, cuboIdToSize)
    logDebug(s"Segment $segmentId get the recommended cuboid ${cuboids.keySet()}")
    val allRecommendedAggColOrders = CuboIdToLayoutUtils.convertCuboIdsToAggIndexColOrders(cuboids, indexPlan.getRuleBasedIndex)
    logInfo(s"Segment $segmentId get ${allRecommendedAggColOrders.size()} recommended layouts with duplicate layouts removed.")
    jobContext.setRecommendAggColOrders(allRecommendedAggColOrders)
  }

  private[build] def evaluateColumnBytes(totalCount: Long, //
                                         sampled: Map[String, Long]): Map[String, Long] = {
    val tableMetadataManager = NTableMetadataManager.getInstance(config, project)
    val tableSizeMap: mutable.Map[String, Long] = mutable.Map()

    sampled.map {
      case (column, bytes) =>
        var total: Long = totalCount
        val tableName = tableDesc.getTableName(column)
        try {
          if (!dataModel.isFactTable(tableName)) {
            if (tableSizeMap.contains(tableName)) {
              // get from map cache , in case of calculating same table several times
              total = tableSizeMap(tableName).longValue()
              logInfo(s"Find $column's table $tableName count $total from cache")
            } else {
              total = evaluateColumnTotalFromTableDesc(tableMetadataManager, totalCount, tableName, column)
            }
          }
        } catch {
          case throwable: Throwable =>
            logWarning(s"Calculate $column's table $tableName count exception", throwable)
        } finally {
          if (total > totalCount) {
            logInfo(s"Table $tableName's count $total is large than flat table count $totalCount, " +
              s"use flat table count as table count")
            total = totalCount
          }
          tableSizeMap(tableName) = total
        }

        val realSampledCount = if (totalCount < sampleRowCount) totalCount else sampleRowCount
        val multiple = 1.0 * total / realSampledCount
        column -> (bytes * multiple).toLong
    }
  }

  def evaluateColumnTotalFromTableDesc(tableMetadataManager: NTableMetadataManager, totalCount: Long, //
                                       tableName: String, column: String): Long = {
    var total: Long = totalCount
    val tableMetadataDesc = tableMetadataManager.getTableDesc(tableDesc.getTableName(column))
    if (tableMetadataDesc != null) {
      val tableExtDesc = tableMetadataManager.getTableExtIfExists(tableMetadataDesc)
      if (tableExtDesc != null && tableExtDesc.getTotalRows > 0) {
        total = tableExtDesc.getTotalRows
        logInfo(s"Find $column's table $tableName count $total from table ext")
      } else if (tableMetadataDesc.getLastSnapshotPath != null) {
        val baseDir = KapConfig.getInstanceFromEnv.getMetadataWorkingDirectory
        val fs = HadoopUtil.getWorkingFileSystem
        val path = new Path(baseDir, tableMetadataDesc.getLastSnapshotPath)
        if (fs.exists(path)) {
          total = sparkSession.read.parquet(path.toString).count()
          logInfo(s"Calculate $column's table $tableName count $total " +
            s"from parquet ${tableMetadataDesc.getLastSnapshotPath}")
        }
      }
    }
    total
  }

  // Copied from DFChooser.
  private def utf8Length(value: Any): Long = {
    if (Objects.isNull(value)) {
      return 0L
    }
    var i = 0
    var bytes = 0L
    val sequence = value.toString
    while (i < sequence.length) {
      val c = sequence.charAt(i)
      if (c <= 0x7F) bytes += 1
      else if (c <= 0x7FF) bytes += 2
      else if (Character.isHighSurrogate(c)) {
        bytes += 4
        i += 1
      }
      else bytes += 3
      i += 1
    }
    bytes
  }

  def buildDictIfNeed(): Dataset[Row] = {
    /**
     * If need to build and encode dict columns, then
     * 1. try best to build in fact-table.
     * 2. try best to build in lookup-tables (without cc dict).
     * 3. try to build in fact-table.
     *
     * CC in lookup-tables MUST be built in flat-table.
     */
    val (dictCols, encodeCols, dictColsWithoutCc, encodeColsWithoutCc) = prepareForDict()
    val factTable = buildDictIfNeed(partFactTable, dictCols, encodeCols)

    var flatTable = if (canJoinLookups) {

      val joinTableMap = createJoinTableMap(joinTableSeq, createSnapshot)
        .map(lookupTableMap =>
          (lookupTableMap._1, buildDictIfNeed(lookupTableMap._2, dictColsWithoutCc, encodeColsWithoutCc)))
      if (joinTableMap.nonEmpty) {
        generateLookupTableMeta(project, joinTableMap)
      }
      if (inferFiltersEnabled) {
        FiltersUtil.initFilters(tableDesc, joinTableMap)
      }

      val jointTable = joinAsFlatTable(factTable, joinTableMap, dataModel)
      buildDictIfNeed(concatCCs(jointTable, factTableCCs),
        selectColumnsNotInTables(factTable, joinTableMap.values.toSeq, dictCols),
        selectColumnsNotInTables(factTable, joinTableMap.values.toSeq, encodeCols))
    } else {
      factTable
    }

    DFBuilderHelper.checkPointSegment(dataSegment, (copied: NDataSegment) => copied.setDictReady(true))

    flatTable = applyFilterCondition(flatTable)
    flatTable = changeSchemeToColumnId(flatTable, tableDesc)
    flatTable
  }

  protected def prepareForDict(): (Set[TblColRef], Set[TblColRef], Set[TblColRef], Set[TblColRef]) = {
    val dictCols = DictionaryBuilderHelper.extractTreeRelatedGlobalDictToBuild(dataSegment, spanningTree.getIndices).asScala.toSet
    val encodeCols = DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(dataSegment, spanningTree.getIndices).asScala.toSet
    val dictColsWithoutCc = dictCols.filter(!_.getColumnDesc.isComputedColumn)
    val encodeColsWithoutCc = encodeCols.filter(!_.getColumnDesc.isComputedColumn)
    (dictCols, encodeCols, dictColsWithoutCc, encodeColsWithoutCc)
  }

  // These parameters can be changed when running the cube planner, should use the
  // `def` to get the latest data
  protected def spanningTree: AdaptiveSpanningTree = params.getSpanningTree

  protected def createLightFlatTable(): Dataset[Row] = {
    val recovered = recoverFlatTable()
    recovered.getOrElse {
      var flatTable = if (canJoinLookups) {
        val joinTableMap = createJoinTableMap(joinTableSeq, createSnapshot)
        if (inferFiltersEnabled) {
          FiltersUtil.initFilters(tableDesc, joinTableMap)
        }

        val jointDS = joinAsFlatTable(partFactTable, joinTableMap, dataModel)
        concatCCs(jointDS, factTableCCs)
      } else {
        concatCCs(partFactTable, factTableCCs)
      }
      flatTable = applyFilterCondition(flatTable)
      changeSchemeToColumnId(flatTable, tableDesc)
    }
  }

  def createSnapshot(tableRef: TableRef): Dataset[Row] = {
    val snapshotResPath = tableRef.getTableDesc.getLastSnapshotPath
    val baseDir = KapConfig.getInstanceFromEnv.getMetadataWorkingDirectory
    val snapshotResFilePath = new Path(baseDir + snapshotResPath)
    val fs = HadoopUtil.getWorkingFileSystem
    if (snapshotResPath == null
      || !fs.exists(snapshotResFilePath)
      || !config.isPersistFlatUseSnapshotEnabled) {
      createTable(tableRef)(sparkSession)
    } else {
      sparkSession.read.parquet(snapshotResFilePath.toString).alias(tableRef.getAlias)
    }
  }

  private def applyFilterCondition(originDS: Dataset[Row]): Dataset[Row] = {
    val filterCondition = dataModel.getFilterCondition
    if (StringUtils.isBlank(filterCondition)) {
      logInfo(s"No available FILTER-CONDITION segment $segmentId")
      return originDS
    }
    val converted = replaceDot(filterCondition, dataModel)
    val condition = s" (1=1) AND ($converted)"
    logInfo(s"Apply FILTER-CONDITION: $condition segment $segmentId")
    originDS.where(condition)
  }

  protected def buildDictIfNeed(table: Dataset[Row],
                                dictCols: Set[TblColRef],
                                encodeCols: Set[TblColRef]): Dataset[Row] = {
    if (dictCols.isEmpty && encodeCols.isEmpty) {
      return table
    }
    if (dataSegment.isDictReady) {
      logInfo(s"Skip DICTIONARY segment $segmentId")
    } else {
      // ensure at least one worker was registered before dictionary lock added.
      waitTillWorkerRegistered()
      buildDict(table, dictCols)
    }

    if (config.isV3DictEnable) {
      buildV3DictIfNeeded(table, encodeCols)
    } else {
      encodeColumn(table, encodeCols)
    }
  }

  protected def buildV3DictIfNeeded(table: Dataset[Row], dictCols: Set[TblColRef]): Dataset[Row] = {
    logInfo("Build v3 dict if needed.")
    val matchedCols = selectColumnsInTable(table, dictCols)
    val cols = matchedCols.map { dictColumn =>
      val wrapDictCol = DictionaryBuilder.wrapCol(dictColumn)
      val dbName = dictColumn.getTableRef.getTableDesc.getDatabase
      dict_encode_v3(col(wrapDictCol), dbName).alias(wrapDictCol + "_KYLIN_ENCODE")
    }.toSeq
    val dictPlan = table
      .select(table.schema.map(ty => col(ty.name)) ++ cols: _*)
      .queryExecution
      .analyzed
    val encodePlan = DictionaryBuilder.buildGlobalDict(project, sparkSession, dictPlan)
    SparkInternalAgent.getDataFrame(sparkSession, encodePlan)
  }

  def waitTillWorkerRegistered(timeout: Long = 20, unit: TimeUnit = TimeUnit.SECONDS): Unit = {
    val cdl = new CountDownLatch(1)
    val timer = new Timer("worker-starvation-timer", true)
    timer.scheduleAtFixedRate(new TimerTask {

      private def releaseAll(): Unit = {
        this.cancel()
        cdl.countDown()
      }

      override def run(): Unit = {
        try {
          if (sparkSession.sparkContext.statusTracker.getExecutorInfos.isEmpty) {
            logWarning("Ensure at least one worker has been registered before building dictionary.")
          } else {
            releaseAll()
          }
        } catch {
          case t: Throwable =>
            logError(s"Something unexpected happened, we wouldn't wait for resource ready.", t)
            releaseAll()
        }
      }
    }, 0, unit.toMillis(timeout))
    // At most waiting for 10 hours.
    cdl.await(10, TimeUnit.HOURS)
    timer.cancel()
  }

  private def buildDict(ds: Dataset[Row], dictCols: Set[TblColRef]): Unit = {
    if (config.isV2DictEnable) {
      logInfo("Build v2 dict default")
      var matchedCols = selectColumnsInTable(ds, dictCols)
      if (dataSegment.getIndexPlan.isSkipEncodeIntegerFamilyEnabled) {
        matchedCols = matchedCols.filterNot(_.getType.isIntegerFamily)
      }
      val builder = new DFDictionaryBuilder(ds, dataSegment, sparkSession, Sets.newHashSet(matchedCols.asJavaCollection))
      builder.buildDictSet(params.getGlobalDictBuildVersionMap)
    }
  }

  private def encodeColumn(ds: Dataset[Row], encodeCols: Set[TblColRef]): Dataset[Row] = {
    val matchedCols = selectColumnsInTable(ds, encodeCols)
    var encodeDs = ds
    if (matchedCols.nonEmpty) {
      encodeDs = DFTableEncoder.encodeTable(ds, dataSegment, matchedCols.asJava,
        params.getGlobalDictBuildVersionMap)
    }
    encodeDs
  }

  def initFactTable(): Unit = {
    val factTable: Dataset[Row] = createFactTable()
    params.setFactTable(factTable)
    val partFactTable: Dataset[Row] = createPartFactTable()
    params.setPartFactTable(partFactTable)
  }

  def persistFactView(): Unit = {
    initSpanningTree()
    initFlatTableDesc()
    initFactTable()
  }

  def initFlatTableOnDetectResource(): Unit = {
    persistFactView()
    val lightFlatTable: Dataset[Row] = createLightFlatTable()
    params.setLightFlatTable(lightFlatTable)
  }

  private def isTableToBuild(joinDesc: JoinTableDesc): Boolean = {
    !tableDesc.isPartialBuild || (tableDesc.isPartialBuild && tableDesc.getRelatedTables.contains(joinDesc.getAlias))
  }

  // ====================================== Dividing line, till the bottom. ====================================== //
  // Historical debt.
  // Waiting for reconstruction.

  protected def createFlatTable(): Dataset[Row] = {
    val recovered = recoverFlatTable()
    recovered.getOrElse(persistFlatTable(params.getDict))
  }

  private def persistFlatTable(table: Dataset[Row]): Dataset[Row] = {
    if (!canPersistFlatTable) {
      return table
    }
    if (table.schema.isEmpty) {
      logInfo("No available flat table schema.")
      return table
    }
    logInfo(s"Segment $segmentId persist flat table: $flatTablePath")
    sparkSession.sparkContext.setJobDescription(s"Segment $segmentId persist flat table.")
    SparkConfHelper.setLocalPropertyIfNeeded(sparkSession,
      config.isFlatTableRedistributionEnabled,
      "spark.sql.sources.repartitionWritingDataSource",
      "true");
    table.write.mode(SaveMode.Overwrite).parquet(flatTablePath.toString)
    SparkConfHelper.resetLocalPropertyIfNeeded(sparkSession,
      config.isFlatTableRedistributionEnabled,
      "spark.sql.sources.repartitionWritingDataSource");
    DFBuilderHelper.checkPointSegment(dataSegment, (copied: NDataSegment) => {
      copied.setFlatTableReady(true)
      if (dataSegment.isFlatTableReady) {
        // KE-14714 if flat table is updated, there might be some data inconsistency across indexes
        copied.setStatus(SegmentStatusEnum.WARNING)
      }
    })
    val after = sparkSession.read.parquet(flatTablePath.toString)
    sparkSession.sparkContext.setJobDescription(null)
    after
  }

  private def recoverFlatTable(): Option[Dataset[Row]] = {
    if (tableDesc.isPartialBuild) {
      logInfo(s"Segment $segmentId no need reuse flat table for partial build.")
      return None
    } else if (!isReady4FlatTable) {
      logInfo(s"Segment $segmentId  no available flat table.")
      return None
    }
    // +----------+---+---+---+---+-----------+-----------+
    // |         0|  2|  3|  4|  1|2_KYLIN_ENCODE|4_KYLIN_ENCODE|
    // +----------+---+---+---+---+-----------+-----------+
    val table: DataFrame = Try(sparkSession.read.parquet(flatTablePath.toString)) match {
      case Success(df) => df
      case Failure(f) =>
        logInfo(s"Handled AnalysisException: Unable to infer schema for Parquet. Flat table path $flatTablePath is empty.", f)
        sparkSession.emptyDataFrame
    }
    // ([2_KYLIN_ENCODE,4_KYLIN_ENCODE], [0,1,2,3,4])
    val (coarseEncodes, noneEncodes) = table.schema.map(sf => sf.name).partition(_.endsWith(ENCODE_SUFFIX))
    val encodes = coarseEncodes.map(_.stripSuffix(ENCODE_SUFFIX))

    val noneEncodesFieldMap: Map[String, StructField] = table.schema.map(_.name)
      .zip(table.schema.fields)
      .filter(p => noneEncodes.contains(p._1))
      .toMap

    val nones = tableDesc.getColumnIds.asScala //
      .zip(tableDesc.getColumns.asScala)
      .map(p => (String.valueOf(p._1), p._2)) //
      .filterNot(p => {
        val dataType = SparderTypeUtil.toSparkType(p._2.getType)
        noneEncodesFieldMap.contains(p._1) && (dataType == noneEncodesFieldMap(p._1).dataType)
      }) ++
      // [xx_KYLIN_ENCODE]
      tableDesc.getMeasures.asScala //
        .map(DictionaryBuilderHelper.needGlobalDict) //
        .filter(Objects.nonNull) //
        .map(colRef => dataModel.getColumnIdByColumnName(colRef.getIdentity)) //
        .map(String.valueOf) //
        .filterNot(encodes.contains)
        .map(id => id + ENCODE_SUFFIX)

    if (nones.nonEmpty) {
      // The previous flat table missed some columns.
      // Flat table would be updated at afterwards step.
      logInfo(s"Segment $segmentId update flat table, columns should have been included " + //
        s"${nones.mkString("[", ",", "]")}")
      return None
    }
    // The previous flat table could be reusable.
    logInfo(s"Segment $segmentId skip build flat table.")
    Some(table)
  }

  protected def tableDesc = params.getFlatTableDesc

  protected def createPartFactTable(): Dataset[Row] = {
    val partFactTable = createPartitionedFactTable(true)
    fulfillDS(partFactTable, factTableCCs, factTableRef)
  }

  protected def createFactTable(): Dataset[Row] = {
    val partDS = createPartitionedFactTable(false)
    fulfillDS(partDS, factTableCCs, factTableRef)
  }

  protected def applyPartitionDesc(originDS: Dataset[Row]): Dataset[Row] = {
    // Date range partition.
    val descDRP = dataModel.getPartitionDesc
    if (PartitionDesc.isEmptyPartitionDesc(descDRP) //
      || Objects.isNull(segmentRange) //
      || segmentRange.isInfinite) {
      logInfo(s"No available PARTITION-CONDITION segment $segmentId")
      return originDS
    }

    val condition = descDRP.getPartitionConditionBuilder //
      .buildDateRangeCondition(descDRP, tableDesc.getDataSegment, segmentRange)
    logInfo(s"Apply PARTITION-CONDITION $condition segment $segmentId")
    originDS.where(condition)
  }

  protected def initSpanningTree(): Unit = {
    val spanTree = new AdaptiveSpanningTree(config, new AdaptiveTreeBuilder(dataSegment, readOnlyLayouts))
    params.setSpanningTree(spanTree)
  }

  protected def initFlatTableDesc(): Unit = {
    val flatTableDesc: SegmentFlatTableDesc = if (jobContext.isPartialBuild) {
      val parser = new IndexDependencyParser(dataModel)
      val relatedTableAlias =
        parser.getRelatedTablesAlias(jobContext.getReadOnlyLayouts)
      new SegmentFlatTableDesc(config, dataSegment, spanningTree, relatedTableAlias)
    } else {
      new SegmentFlatTableDesc(config, dataSegment, spanningTree)
    }
    params.setFlatTableDesc(flatTableDesc)
  }

  private def createPartitionedFactTable(partOnly: Boolean): Dataset[Row] = {
    if (isReady4FactView) {
      logInfo(s"Skip FACT-TABLE-VIEW segment $segmentId.")
      return sparkSession.read.parquet(factViewPath.toString)
    }
    val table = createTable(factTableRef)(sparkSession)
    val partTable = applyPartitionDesc(table)
    if (partOnly || !isFactView) {
      return partTable
    }
    persistFactView(partTable)
  }

  private def persistFactView(view: Dataset[Row]): Dataset[Row] = {
    if (!canPersistFactView) {
      params.setSkipPersistFactView(true)
      return view
    }
    logInfo(s"Persist FACT-TABLE-VIEW $factViewPath")
    sparkSession.sparkContext.setJobDescription("Persist FACT-TABLE-VIEW.")
    view.write.mode(SaveMode.Overwrite).parquet(factViewPath.toString)
    // Checkpoint fact table view.
    DFBuilderHelper.checkPointSegment(dataSegment, (copied: NDataSegment) => copied.setFactViewReady(true))
    val after = sparkSession.read.parquet(factViewPath.toString)
    sparkSession.sparkContext.setJobDescription(null)
    after
  }
}

object FlatTableStage extends LogEx {

  import org.apache.kylin.engine.spark.job.NSparkCubingUtil._

  private val conf = KylinConfig.getInstanceFromEnv
  var inferFiltersEnabled: Boolean = conf.inferFiltersEnabled()

  def concatCCs(table: Dataset[Row], computColumns: Set[TblColRef]): Dataset[Row] = {
    val matchedCols = selectColumnsInTable(table, computColumns)
    var tableWithCcs = table
    matchedCols.foreach(m =>
      tableWithCcs = tableWithCcs.withColumn(convertFromDot(m.getBackTickIdentity),
        expr(convertFromDotWithBackTick(m.getBackTickExp))))
    tableWithCcs
  }

  def fulfillDS(originDS: Dataset[Row], cols: Set[TblColRef], tableRef: TableRef): Dataset[Row] = {
    // wrap computed columns, filter out valid columns
    val computedColumns = chooseSuitableCols(originDS, cols)
    // wrap alias
    val newDS = wrapAlias(originDS, tableRef.getAlias)
    val selectedColumns = newDS.schema.fields.map(tp => col(tp.name)) ++ computedColumns
    logInfo(s"Table SCHEMA ${tableRef.getTableIdentity} ${newDS.schema.treeString}")
    newDS.select(selectedColumns: _*)
  }

  def wrapAlias(originDS: Dataset[Row], alias: String, needLog: Boolean = true): Dataset[Row] = {
    val newFields = originDS.schema.fields
      .map(f => {
        val aliasDotColName = "`" + alias + "`" + "." + "`" + f.name + "`"
        convertFromDot(aliasDotColName)
      }).toSeq
    val newDS = originDS.toDF(newFields: _*)
    if (needLog) {
      logInfo(s"Wrap ALIAS ${originDS.schema.treeString} TO ${newDS.schema.treeString}")
    }
    newDS
  }

  def wrapAlias(originPlan: LogicalPlan, alias: String, needLog: Boolean): LogicalPlan = {
    val newFields = originPlan.output
      .map(f => {
        val aliasDotColName = "`" + alias + "`" + "." + "`" + f.name + "`"
        convertFromDot(aliasDotColName)
      })
    val newDS = SparkOperation.projectAsAlias(newFields, originPlan)
    if (needLog) {
      logInfo(s"Wrap ALIAS ${originPlan.schema.treeString} TO ${newDS.schema.treeString}")
    }
    newDS
  }


  def joinAsFlatTable(factTable: Dataset[Row],
                      joinTableMap: mutable.Map[JoinTableDesc, Dataset[Row]],
                      model: NDataModel,
                      needLog: Boolean = true): Dataset[Row] = {
    joinTableMap.foldLeft(factTable)(
      (joinedDataset: Dataset[Row], tuple: (JoinTableDesc, Dataset[Row])) =>
        joinTableDataset(model.getRootFactTable.getTableDesc, tuple._1, joinedDataset, tuple._2, needLog))
  }

  def joinTableDataset(rootFactDesc: TableDesc,
                       lookupDesc: JoinTableDesc,
                       rootFactDataset: Dataset[Row],
                       lookupDataset: Dataset[Row],
                       needLog: Boolean = true): Dataset[Row] = {
    var afterJoin = rootFactDataset
    val join = lookupDesc.getJoin
    if (join != null && !StringUtils.isEmpty(join.getType)) {
      val joinType = join.getType.toUpperCase(Locale.ROOT)
      val pk = join.getPrimaryKeyColumns
      val fk = join.getForeignKeyColumns
      if (pk.length != fk.length) {
        throw new RuntimeException(
          s"Invalid join condition of fact table: $rootFactDesc,fk: ${fk.mkString(",")}," +
            s" lookup table:$lookupDesc, pk: ${pk.mkString(",")}")
      }
      if (needLog) {
        logInfo(s"Lookup table schema ${lookupDataset.schema.treeString}")
      }

      val condition = getCondition(join)
      if (needLog) {
        val nonEquiv = if (join.getNonEquiJoinCondition == null) "" else "non-equi "
        logInfo(s"Root table ${rootFactDesc.getIdentity},"
          + s" join table ${lookupDesc.getAlias},"
          + s" ${nonEquiv} condition: ${condition.toString()}")
      }

      if (join.getNonEquiJoinCondition == null && inferFiltersEnabled) {
        afterJoin = afterJoin.join(FiltersUtil.inferFilters(pk, lookupDataset), condition, joinType)
      } else {
        afterJoin = afterJoin.join(lookupDataset, condition, joinType)
      }
    }
    afterJoin
  }

  def createJoinTableMap(joinTableSeq: Seq[JoinTableDesc],
                         joinTableFunc: TableRef => Dataset[Row]): mutable.LinkedHashMap[JoinTableDesc, Dataset[Row]] = {
    val ret = mutable.LinkedHashMap[JoinTableDesc, Dataset[Row]]()
    val antiFlattenTableSet = mutable.Set[String]()
    joinTableSeq.foreach { joinDesc =>
      val fkTableRef = joinDesc.getJoin.getFKSide
      if (fkTableRef == null) {
        throw new IllegalArgumentException("FK table cannot be null")
      }
      val fkTable = fkTableRef.getTableDesc.getIdentity
      if (!joinDesc.isFlattenable || antiFlattenTableSet.contains(fkTable)) {
        antiFlattenTableSet.add(joinDesc.getTable)
      }
      if (joinDesc.isFlattenable && !antiFlattenTableSet.contains(joinDesc.getTable)) {
        val tableRef = joinDesc.getTableRef
        val tableDS = joinTableFunc.apply(tableRef)
        ret.put(joinDesc, fulfillDS(tableDS, Set.empty, tableRef))
      }
    }
    ret
  }

  def getCondition(join: JoinDesc): Column = {
    val pk = join.getPrimaryKeyColumns
    val fk = join.getForeignKeyColumns

    val equalPairs = fk.zip(pk).map(joinKey => {
      val fkIdentity = convertFromDot(joinKey._1.getBackTickIdentity)
      val pkIdentity = convertFromDot(joinKey._2.getBackTickIdentity)
      col(fkIdentity).equalTo(col(pkIdentity))
    }).reduce(_ && _)

    if (join.getNonEquiJoinCondition == null) {
      equalPairs
    } else {
      NonEquiJoinConditionBuilder.convert(join.getNonEquiJoinCondition) && equalPairs
    }
  }

  def joinFactTableWithLookupTables(rootFactPlan: LogicalPlan,
                                    lookupTableDatasetMap: mutable.Map[JoinTableDesc, LogicalPlan],
                                    model: NDataModel,
                                    needLog: Boolean): LogicalPlan = {
    lookupTableDatasetMap.foldLeft(rootFactPlan)(
      (joinedDataset: LogicalPlan, tuple: (JoinTableDesc, LogicalPlan)) =>
        joinTableLogicalPlan(model.getRootFactTable.getTableDesc, tuple._1, joinedDataset, tuple._2, needLog))
  }

  def joinTableLogicalPlan(rootFactDesc: TableDesc,
                           lookupDesc: JoinTableDesc,
                           rootFactPlan: LogicalPlan,
                           lookupPlan: LogicalPlan,
                           needLog: Boolean = true): LogicalPlan = {
    var afterJoin = rootFactPlan
    val join = lookupDesc.getJoin
    if (join != null && !StringUtils.isEmpty(join.getType)) {
      val joinType = join.getType.toUpperCase(Locale.ROOT)
      val pk = join.getPrimaryKeyColumns
      val fk = join.getForeignKeyColumns
      if (pk.length != fk.length) {
        throw new RuntimeException(
          s"Invalid join condition of fact table: $rootFactDesc,fk: ${fk.mkString(",")}," +
            s" lookup table:$lookupDesc, pk: ${pk.mkString(",")}")
      }
      val equiConditionColPairs = fk.zip(pk).map(joinKey =>
        col(convertFromDot(joinKey._1.getBackTickIdentity))
          .equalTo(col(convertFromDot(joinKey._2.getBackTickIdentity))))

      if (join.getNonEquiJoinCondition != null) {
        val condition: Column = getCondition(join)
        logInfo(s"Root table ${rootFactDesc.getIdentity}, join table ${lookupDesc.getAlias}, non-equi condition: ${condition.toString()}")
        afterJoin = Join(afterJoin, lookupPlan, JoinType.apply(joinType), Option.apply(condition.expr), JoinHint.NONE)
      } else {
        val condition = equiConditionColPairs.reduce(_ && _)
        logInfo(s"Root table ${rootFactDesc.getIdentity}, join table ${lookupDesc.getAlias}, condition: ${condition.toString()}")
        afterJoin = Join(afterJoin, lookupPlan, JoinType.apply(joinType), Option.apply(condition.expr), JoinHint.NONE)
      }
    }
    afterJoin
  }

  def changeSchemeToColumnId(ds: Dataset[Row], tableDesc: SegmentFlatTableDesc): Dataset[Row] = {
    val structType = ds.schema
    val columnIds = tableDesc.getColumnIds.asScala
    val columnName2Id = tableDesc.getColumns
      .asScala
      .map(column => convertFromDot(column.getBackTickIdentity))
      .zip(columnIds)
    val columnName2IdMap = columnName2Id.toMap
    val encodeSeq = structType.filter(_.name.endsWith(ENCODE_SUFFIX)).map {
      tp =>
        val columnName = tp.name.stripSuffix(ENCODE_SUFFIX)
        val columnId = columnName2IdMap.apply(columnName)
        col(tp.name).alias(columnId.toString + ENCODE_SUFFIX)
    }
    val columns = columnName2Id.map(tp => expr("`" + tp._1 + "`").alias(tp._2.toString))
    logInfo(s"Select model column is ${columns.mkString(",")}")
    logInfo(s"Select model encoding column is ${encodeSeq.mkString(",")}")
    val selectedColumns = columns ++ encodeSeq

    logInfo(s"Select model all column is ${selectedColumns.mkString(",")}")
    ds.select(selectedColumns: _*)
  }

  def replaceDot(original: String, model: NDataModel): String = {
    val sb = new StringBuilder(original)

    for (namedColumn <- model.getAllNamedColumns.asScala) {
      val colName = namedColumn.getAliasDotColumn.toLowerCase(Locale.ROOT)
      doReplaceDot(sb, colName, namedColumn.getAliasDotColumn)

      // try replacing quoted identifiers if any
      val quotedColName = colName.split('.').mkString("`", "`.`", "`")
      if (quotedColName.nonEmpty) {
        doReplaceDot(sb, quotedColName, namedColumn.getAliasDotColumn.split('.').mkString("`", "`.`", "`"))
      }
    }
    sb.toString()
  }

  private def doReplaceDot(sb: StringBuilder, namedCol: String, colAliasDotColumn: String): Unit = {
    var start = sb.toString.toLowerCase(Locale.ROOT).indexOf(namedCol)
    while (start != -1) {
      sb.replace(start,
        start + namedCol.length,
        "`" + convertFromDot(colAliasDotColumn) + "`")
      start = sb.toString.toLowerCase(Locale.ROOT)
        .indexOf(namedCol)
    }
  }

  private def generateLookupTableMeta(project: String,
                                      lookupTables: mutable.LinkedHashMap[JoinTableDesc, Dataset[Row]]): Unit = {
    val config = KapConfig.getInstanceFromEnv
    if (config.isRecordSourceUsage) {
      lookupTables.keySet.foreach { joinTable =>
        val tableManager = NTableMetadataManager.getInstance(config.getKylinConfig, project)
        val table = tableManager.getOrCreateTableExt(joinTable.getTable)
        if (table.getTotalRows > 0) {
          TableMetaManager.putTableMeta(joinTable.getTable, 0, table.getTotalRows)
          logInfo(s"put meta table: ${joinTable.getTable}, count: ${table.getTotalRows}")
        }
      }
    }
    val noStatLookupTables = lookupTables.filterKeys(table => TableMetaManager.getTableMeta(table.getTable).isEmpty)
    if (config.getKylinConfig.isNeedCollectLookupTableInfo && noStatLookupTables.nonEmpty) {
      val lookupTablePar = noStatLookupTables.par
      lookupTablePar.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(lookupTablePar.size))
      lookupTablePar.foreach { case (joinTableDesc, dataset) =>
        val tableIdentity = joinTableDesc.getTable
        logTime(s"count $tableIdentity") {
          val maxTime = Duration(config.getKylinConfig.getCountLookupTableMaxTime, MILLISECONDS)
          val defaultCount = config.getKylinConfig.getLookupTableCountDefaultValue
          val rowCount = countTableInFiniteTimeOrDefault(dataset, tableIdentity, maxTime, defaultCount)
          TableMetaManager.putTableMeta(tableIdentity, 0L, rowCount)
          logInfo(s"put meta table: $tableIdentity , count: $rowCount")
        }
      }
    }
  }

  def countTableInFiniteTimeOrDefault(dataset: Dataset[Row], tableName: String,
                                      duration: Duration, defaultCount: Long): Long = {
    val countTask = dataset.rdd.countAsync()
    try {
      ProxyThreadUtils.awaitResult(countTask, duration)
    } catch {
      case e: Exception =>
        countTask.cancel()
        logInfo(s"$tableName count fail, and return defaultCount $defaultCount", e)
        defaultCount
    }
  }

  def createTable(tableRef: TableRef)(implicit spark: SparkSession): Dataset[Row] = {
    // By design, why not try recovering from table snapshot.
    // If fact table is a view and its snapshot exists, that will benefit.
    logInfo(s"Load source table ${tableRef.getTableIdentity}")
    import SparkDataSource.SparkSource
    // without any computed column
    spark.table(tableRef.getTableDesc).alias(tableRef.getAlias)
  }

  case class Statistics(totalCount: Long, columnBytes: Map[String, Long])

}
