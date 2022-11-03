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

package org.apache.kylin.engine.spark.builder

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.{Locale, Objects, Timer, TimerTask}
import com.google.common.collect.Sets
import io.kyligence.kap.query.util.KapQueryUtil
import org.apache.kylin.engine.spark.builder.DFBuilderHelper._
import org.apache.kylin.engine.spark.job.NSparkCubingUtil._
import org.apache.kylin.engine.spark.job.{FiltersUtil, TableMetaManager}
import org.apache.kylin.engine.spark.model.SegmentFlatTableDesc
import org.apache.kylin.engine.spark.utils.LogEx
import org.apache.kylin.engine.spark.utils.SparkDataSource._
import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.kylin.metadata.model.{NDataModel, NTableMetadataManager}
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.metadata.model._
import org.apache.spark.sql._
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

class SegmentFlatTable(private val sparkSession: SparkSession, //
                       private val tableDesc: SegmentFlatTableDesc) extends LogEx {

  import SegmentFlatTable._

  protected final val project = tableDesc.getProject
  protected final val spanningTree = tableDesc.getSpanningTree
  protected final val dataSegment = tableDesc.getDataSegment
  protected final val dataModel = tableDesc.getDataModel
  protected final val indexPlan = tableDesc.getIndexPlan
  protected final val segmentRange = tableDesc.getSegmentRange
  protected lazy final val flatTablePath = tableDesc.getFlatTablePath
  protected lazy final val factTableViewPath = tableDesc.getFactTableViewPath
  protected lazy final val workingDir = tableDesc.getWorkingDir
  protected lazy final val sampleRowCount = tableDesc.getSampleRowCount

  protected final val segmentId = dataSegment.getId

  protected lazy final val FLAT_TABLE = generateFlatTable()
  // flat-table without dict columns
  private lazy final val FLAT_TABLE_PART = generateFlatTablePart()

  protected val rootFactTable: TableRef = dataModel.getRootFactTable

  // Flat table.
  private lazy val shouldPersistFT = tableDesc.shouldPersistFlatTable()
  private lazy val isFTReady = dataSegment.isFlatTableReady && tableDesc.buildFilesSeparationPathExists(flatTablePath)

  // Fact table view.
  private lazy val isFTV = rootFactTable.getTableDesc.isView
  private lazy val shouldPersistFTV = tableDesc.shouldPersistView()
  private lazy val isFTVReady = dataSegment.isFactViewReady && HadoopUtil.getWorkingFileSystem.exists(factTableViewPath)

  private lazy val needJoin = {
    val join = tableDesc.shouldJoinLookupTables
    logInfo(s"Segment $segmentId flat table need join: $join")
    join
  }

  protected lazy val factTableDS: Dataset[Row] = newFactTableDS()
  private lazy val fastFactTableDS = newFastFactTableDS()

  // By design, COMPUTED-COLUMN could only be defined on fact table.
  protected lazy val factTableCCs: Set[TblColRef] = rootFactTable.getColumns.asScala
    .filter(_.getColumnDesc.isComputedColumn)
    .toSet

  def getFlatTablePartDS: Dataset[Row] = {
    FLAT_TABLE_PART
  }

  def getFlatTableDS: Dataset[Row] = {
    FLAT_TABLE
  }

  def gatherStatistics(): Statistics = {
    val stepDesc = s"Segment $segmentId collect flat table statistics."
    logInfo(stepDesc)
    sparkSession.sparkContext.setJobDescription(stepDesc)
    val statistics = gatherStatistics(FLAT_TABLE)
    logInfo(s"Segment $segmentId collect flat table statistics $statistics.")
    sparkSession.sparkContext.setJobDescription(null)
    statistics
  }

  protected def generateFlatTablePart(): Dataset[Row] = {
    val recoveredDS = tryRecoverFTDS()
    if (recoveredDS.nonEmpty) {
      return recoveredDS.get
    }
    var flatTableDS = if (needJoin) {
      val lookupTableDSMap = generateLookupTables()
      if (inferFiltersEnabled) {
        FiltersUtil.initFilters(tableDesc, lookupTableDSMap)
      }
      val jointDS = joinFactTableWithLookupTables(fastFactTableDS, lookupTableDSMap, dataModel, sparkSession)
      concatCCs(jointDS, factTableCCs)
    } else {
      fastFactTableDS
    }
    flatTableDS = applyFilterCondition(flatTableDS)
    changeSchemeToColumnId(flatTableDS, tableDesc)
  }

  protected def generateFlatTable(): Dataset[Row] = {
    val recoveredDS = tryRecoverFTDS()
    if (recoveredDS.nonEmpty) {
      return recoveredDS.get
    }

    /**
      * If need to build and encode dict columns, then
      * 1. try best to build in fact-table.
      * 2. try best to build in lookup-tables (without cc dict).
      * 3. try to build in fact-table.
      *
      * CC in lookup-tables MUST be built in flat-table.
      */
    val (dictCols, encodeCols, dictColsWithoutCc, encodeColsWithoutCc) = prepareForDict()
    val factTable = buildDictIfNeed(factTableDS, dictCols, encodeCols)

    var flatTable = if (needJoin) {

      val lookupTables = generateLookupTables()
        .map(lookupTableMap =>
          (lookupTableMap._1, buildDictIfNeed(lookupTableMap._2, dictColsWithoutCc, encodeColsWithoutCc)))
      if (lookupTables.nonEmpty) {
        generateLookupTableMeta(project, lookupTables)
      }
      if (inferFiltersEnabled) {
        FiltersUtil.initFilters(tableDesc, lookupTables)
      }

      val jointTable = joinFactTableWithLookupTables(factTable, lookupTables, dataModel, sparkSession)
      buildDictIfNeed(concatCCs(jointTable, factTableCCs),
        selectColumnsNotInTables(factTable, lookupTables.values.toSeq, dictCols),
        selectColumnsNotInTables(factTable, lookupTables.values.toSeq, encodeCols))
    } else {
      factTable
    }

    DFBuilderHelper.checkPointSegment(dataSegment, (copied: NDataSegment) => copied.setDictReady(true))

    flatTable = applyFilterCondition(flatTable)
    flatTable = changeSchemeToColumnId(flatTable, tableDesc)
    tryPersistFTDS(flatTable)
  }

  private def prepareForDict(): (Set[TblColRef], Set[TblColRef], Set[TblColRef], Set[TblColRef]) = {
    val dictCols = DictionaryBuilderHelper.extractTreeRelatedGlobalDictToBuild(dataSegment, spanningTree.getIndices).asScala.toSet
    val encodeCols = DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(dataSegment, spanningTree.getIndices).asScala.toSet
    val dictColsWithoutCc = dictCols.filter(!_.getColumnDesc.isComputedColumn)
    val encodeColsWithoutCc = encodeCols.filter(!_.getColumnDesc.isComputedColumn)
    (dictCols, encodeCols, dictColsWithoutCc, encodeColsWithoutCc)
  }

  private def newFastFactTableDS(): Dataset[Row] = {
    val partDS = newPartitionedFTDS(needFast = true)
    fulfillDS(partDS, factTableCCs, rootFactTable)
  }

  private def newFactTableDS(): Dataset[Row] = {
    val partDS = newPartitionedFTDS()
    fulfillDS(partDS, factTableCCs, rootFactTable)
  }

  private def newPartitionedFTDS(needFast: Boolean = false): Dataset[Row] = {
    if (isFTVReady) {
      logInfo(s"Skip FACT-TABLE-VIEW segment $segmentId.")
      return sparkSession.read.parquet(factTableViewPath.toString)
    }
    val tableDS = newTableDS(rootFactTable)
    val partDS = applyPartitionDesc(tableDS)
    if (needFast || !isFTV) {
      return partDS
    }
    tryPersistFTVDS(partDS)
  }

  protected def generateLookupTables(): mutable.LinkedHashMap[JoinTableDesc, Dataset[Row]] = {
    val ret = mutable.LinkedHashMap[JoinTableDesc, Dataset[Row]]()
    val normalizedTableSet = mutable.Set[String]()
    dataModel.getJoinTables.asScala
      .filter(isTableToBuild)
      .foreach { joinDesc =>
        val fkTableRef = joinDesc.getJoin.getFKSide
        if (fkTableRef == null) {
          throw new IllegalArgumentException("FK table cannot be null")
        }
        val fkTable = fkTableRef.getTableDesc.getIdentity
        if (!joinDesc.isFlattenable || normalizedTableSet.contains(fkTable)) {
          normalizedTableSet.add(joinDesc.getTable)
        }
        if (joinDesc.isFlattenable && !dataSegment.getExcludedTables.contains(joinDesc.getTable)
          && !dataSegment.getExcludedTables.contains(fkTable)
          && !normalizedTableSet.contains(joinDesc.getTable)) {
          val tableRef = joinDesc.getTableRef
          val tableDS = newTableDS(tableRef)
          ret.put(joinDesc, fulfillDS(tableDS, Set.empty, tableRef))
        }
      }
    ret
  }

  private def isTableToBuild(joinDesc: JoinTableDesc): Boolean = {
    !tableDesc.isPartialBuild || (tableDesc.isPartialBuild && tableDesc.getRelatedTables.contains(joinDesc.getAlias))
  }

  protected def applyPartitionDesc(originDS: Dataset[Row]): Dataset[Row] = {
    // Date range partition.
    val descDRP = dataModel.getPartitionDesc
    if (Objects.isNull(descDRP) //
      || Objects.isNull(descDRP.getPartitionDateColumn) //
      || Objects.isNull(segmentRange) //
      || segmentRange.isInfinite) {
      logInfo(s"No available PARTITION-CONDITION segment $segmentId")
      return originDS
    }

    val condition = descDRP.getPartitionConditionBuilder //
      .buildDateRangeCondition(descDRP, null, segmentRange)
    logInfo(s"Apply PARTITION-CONDITION $condition segment $segmentId")
    originDS.where(condition)
  }

  private def applyFilterCondition(originDS: Dataset[Row]): Dataset[Row] = {
    if (StringUtils.isBlank(dataModel.getFilterCondition)) {
      logInfo(s"No available FILTER-CONDITION segment $segmentId")
      return originDS
    }
    val expression = KapQueryUtil.massageExpression(dataModel, project, //
      dataModel.getFilterCondition, null)
    val converted = replaceDot(expression, dataModel)
    val condition = s" (1=1) AND ($converted)"
    logInfo(s"Apply FILTER-CONDITION: $condition segment $segmentId")
    originDS.where(condition)
  }

  private def tryPersistFTVDS(tableDS: Dataset[Row]): Dataset[Row] = {
    if (!shouldPersistFTV) {
      return tableDS
    }
    logInfo(s"Persist FACT-TABLE-VIEW $factTableViewPath")
    sparkSession.sparkContext.setJobDescription("Persist FACT-TABLE-VIEW.")
    tableDS.write.mode(SaveMode.Overwrite).parquet(factTableViewPath.toString)
    // Checkpoint fact table view.
    DFBuilderHelper.checkPointSegment(dataSegment, (copied: NDataSegment) => copied.setFactViewReady(true))
    val newDS = sparkSession.read.parquet(factTableViewPath.toString)
    sparkSession.sparkContext.setJobDescription(null)
    newDS
  }

  private def tryPersistFTDS(tableDS: Dataset[Row]): Dataset[Row] = {
    if (!shouldPersistFT) {
      return tableDS
    }
    if (tableDS.schema.isEmpty) {
      logInfo("No available flat table schema.")
      return tableDS
    }
    logInfo(s"Segment $segmentId persist flat table: $flatTablePath")
    sparkSession.sparkContext.setJobDescription(s"Segment $segmentId persist flat table.")
    val coalescePartitionNum = tableDesc.getFlatTableCoalescePartitionNum
    if (coalescePartitionNum > 0) {
      logInfo(s"Segment $segmentId flat table coalesce partition num $coalescePartitionNum")
      tableDS.coalesce(coalescePartitionNum) //
        .write.mode(SaveMode.Overwrite).parquet(flatTablePath.toString)
    } else {
      tableDS.write.mode(SaveMode.Overwrite).parquet(flatTablePath.toString)
    }
    DFBuilderHelper.checkPointSegment(dataSegment, (copied: NDataSegment) => {
      copied.setFlatTableReady(true)
      if (dataSegment.isFlatTableReady) {
        // KE-14714 if flat table is updated, there might be some data inconsistency across indexes
        copied.setStatus(SegmentStatusEnum.WARNING)
      }
    })
    val newDS = sparkSession.read.parquet(flatTablePath.toString)
    sparkSession.sparkContext.setJobDescription(null)
    newDS
  }

  private def tryRecoverFTDS(): Option[Dataset[Row]] = {
    if (tableDesc.isPartialBuild) {
      logInfo(s"Segment $segmentId no need reuse flat table for partial build.")
      return None
    } else if (!isFTReady) {
      logInfo(s"Segment $segmentId  no available flat table.")
      return None
    }
    // +----------+---+---+---+---+-----------+-----------+
    // |         0|  2|  3|  4|  1|2_KE_ENCODE|4_KE_ENCODE|
    // +----------+---+---+---+---+-----------+-----------+
    val tableDS: DataFrame = Try(sparkSession.read.parquet(flatTablePath.toString)) match {
      case Success(df) => df
      case Failure(f) =>
        logInfo(s"Handled AnalysisException: Unable to infer schema for Parquet. Flat table path $flatTablePath is empty.", f)
        sparkSession.emptyDataFrame
    }
    // ([2_KE_ENCODE,4_KE_ENCODE], [0,1,2,3,4])
    val (coarseEncodes, noneEncodes) = tableDS.schema.map(sf => sf.name).partition(_.endsWith(ENCODE_SUFFIX))
    val encodes = coarseEncodes.map(_.stripSuffix(ENCODE_SUFFIX))

    val noneEncodesFieldMap: Map[String, StructField] = tableDS.schema.map(_.name)
      .zip(tableDS.schema.fields)
      .filter(p => noneEncodes.contains(p._1))
      .toMap

    val nones = tableDesc.getColumnIds.asScala //
      .zip(tableDesc.getColumns.asScala)
      .map(p => (String.valueOf(p._1), p._2)) //
      .filterNot(p => {
        val dataType = SparderTypeUtil.toSparkType(p._2.getType)
        noneEncodesFieldMap.contains(p._1) && (dataType == noneEncodesFieldMap(p._1).dataType)
      }) ++
      // [xx_KE_ENCODE]
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
    Some(tableDS)
  }

  def newTableDS(tableRef: TableRef): Dataset[Row] = {
    // By design, why not try recovering from table snapshot.
    // If fact table is a view and its snapshot exists, that will benefit.
    logInfo(s"Load source table ${tableRef.getTableIdentity}")
    val tableDescCopy = tableRef.getTableDesc
    if(tableDescCopy.isTransactional || tableDescCopy.isRangePartition) {
      val model = tableRef.getModel
      if(Objects.nonNull(model)) {
        tableDescCopy.setPartitionDesc(model.getPartitionDesc)
      }

      if(Objects.nonNull(segmentRange) && Objects.nonNull(segmentRange.getStart) && Objects.nonNull(segmentRange.getEnd)) {
        sparkSession.table(tableDescCopy, segmentRange.getStart.toString, segmentRange.getEnd.toString).alias(tableRef.getAlias)
      } else {
        sparkSession.table(tableDescCopy).alias(tableRef.getAlias)
      }
    } else {
      sparkSession.table(tableDescCopy).alias(tableRef.getAlias)
    }
  }

  protected final def gatherStatistics(tableDS: Dataset[Row]): Statistics = {
    val totalRowCount = tableDS.count()
    if (!shouldPersistFT) {
      // By design, evaluating column bytes should be based on existed flat table.
      logInfo(s"Flat table not persisted, only compute row count.")
      return Statistics(totalRowCount, Map.empty[String, Long])
    }
    // zipWithIndex before filter
    val canonicalIndices = tableDS.columns //
      .zipWithIndex //
      .filterNot(_._1.endsWith(ENCODE_SUFFIX)) //
      .map { case (name, index) =>
        val canonical = tableDesc.getCanonicalName(Integer.parseInt(name))
        (canonical, index)
      }.filterNot(t => Objects.isNull(t._1))
    logInfo(s"CANONICAL INDICES ${canonicalIndices.mkString("[", ", ", "]")}")
    // By design, action-take is not sampling.
    val sampled = tableDS.take(sampleRowCount).flatMap(row => //
      canonicalIndices.map { case (canonical, index) => //
        val bytes = utf8Length(row.get(index))
        (canonical, bytes) //
      }).groupBy(_._1).mapValues(_.map(_._2).sum)
    val evaluated = evaluateColumnBytes(totalRowCount, sampled)
    Statistics(totalRowCount, evaluated)
  }

  private def evaluateColumnBytes(totalCount: Long, //
                                  sampled: Map[String, Long]): Map[String, Long] = {
    val multiple = if (totalCount < sampleRowCount) 1f else totalCount.toFloat / sampleRowCount
    sampled.mapValues(bytes => (bytes * multiple).toLong)
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

  // ====================================== Dividing line, till the bottom. ====================================== //
  // Historical debt.
  // Waiting for reconstruction.

  protected def buildDictIfNeed(table: Dataset[Row],
                                dictCols: Set[TblColRef],
                                encodeCols: Set[TblColRef]): Dataset[Row] = {
    if (dictCols.isEmpty && encodeCols.isEmpty) {
      return table
    }
    if (dataSegment.isDictReady) {
      logInfo(s"Skip DICTIONARY segment $segmentId")
    } else {
      // KE-32076 ensure at least one worker was registered before dictionary lock added.
      waitTillWorkerRegistered()
      buildDict(table, dictCols)
    }
    encodeColumn(table, encodeCols)
  }

  def waitTillWorkerRegistered(): Unit = {
    val cdl = new CountDownLatch(1)
    val timer = new Timer("worker-starvation-timer", true)
    timer.scheduleAtFixedRate(new TimerTask {
      override def run(): Unit = {
        if (sparkSession.sparkContext.statusTracker.getExecutorInfos.isEmpty) {
          logWarning("Ensure at least one worker has been registered before building dictionary.")
        } else {
          this.cancel()
          cdl.countDown()
        }
      }
    }, 0, TimeUnit.SECONDS.toMillis(20))
    cdl.await()
    timer.cancel()
  }

  private def concatCCs(table: Dataset[Row], computColumns: Set[TblColRef]): Dataset[Row] = {
    val matchedCols = selectColumnsInTable(table, computColumns)
    var tableWithCcs = table
    matchedCols.foreach(m =>
      tableWithCcs = tableWithCcs.withColumn(convertFromDot(m.getBackTickIdentity),
        expr(convertFromDot(m.getBackTickExpressionInSourceDB))))
    tableWithCcs
  }

  private def buildDict(ds: Dataset[Row], dictCols: Set[TblColRef]): Unit = {
    var matchedCols = selectColumnsInTable(ds, dictCols)
    if (dataSegment.getIndexPlan.isSkipEncodeIntegerFamilyEnabled) {
      matchedCols = matchedCols.filterNot(_.getType.isIntegerFamily)
    }
    val builder = new DFDictionaryBuilder(ds, dataSegment, sparkSession, Sets.newHashSet(matchedCols.asJavaCollection))
    builder.buildDictSet()
  }

  private def encodeColumn(ds: Dataset[Row], encodeCols: Set[TblColRef]): Dataset[Row] = {
    val matchedCols = selectColumnsInTable(ds, encodeCols)
    var encodeDs = ds
    if (matchedCols.nonEmpty) {
      encodeDs = DFTableEncoder.encodeTable(ds, dataSegment, matchedCols.asJava)
    }
    encodeDs
  }

}

object SegmentFlatTable extends LogEx {

  import org.apache.kylin.engine.spark.job.NSparkCubingUtil._

  private val conf = KylinConfig.getInstanceFromEnv
  var inferFiltersEnabled: Boolean = conf.inferFiltersEnabled()

  def fulfillDS(originDS: Dataset[Row], cols: Set[TblColRef], tableRef: TableRef): Dataset[Row] = {
    // wrap computed columns, filter out valid columns
    val computedColumns = chooseSuitableCols(originDS, cols)
    // wrap alias
    val newDS = wrapAlias(originDS, tableRef.getAlias)
    val selectedColumns = newDS.schema.fields.map(tp => col(tp.name)) ++ computedColumns
    logInfo(s"Table SCHEMA ${tableRef.getTableIdentity} ${newDS.schema.treeString}")
    newDS.select(selectedColumns: _*)
  }

  def wrapAlias(originDS: Dataset[Row], alias: String): Dataset[Row] = {
    val newFields = originDS.schema.fields.map(f =>
      convertFromDot("`" + alias + "`" + "." + "`" + f.name + "`")).toSeq
    val newDS = originDS.toDF(newFields: _*)
    logInfo(s"Wrap ALIAS ${originDS.schema.treeString} TO ${newDS.schema.treeString}")
    newDS
  }


  def joinFactTableWithLookupTables(rootFactDataset: Dataset[Row],
                                    lookupTableDatasetMap: mutable.Map[JoinTableDesc, Dataset[Row]],
                                    model: NDataModel,
                                    ss: SparkSession): Dataset[Row] = {
    lookupTableDatasetMap.foldLeft(rootFactDataset)(
      (joinedDataset: Dataset[Row], tuple: (JoinTableDesc, Dataset[Row])) =>
        joinTableDataset(model.getRootFactTable.getTableDesc, tuple._1, joinedDataset, tuple._2, ss))
  }

  def joinTableDataset(rootFactDesc: TableDesc,
                       lookupDesc: JoinTableDesc,
                       rootFactDataset: Dataset[Row],
                       lookupDataset: Dataset[Row],
                       ss: SparkSession): Dataset[Row] = {
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
      val equiConditionColPairs = fk.zip(pk).map(joinKey =>
        col(convertFromDot(joinKey._1.getBackTickIdentity))
          .equalTo(col(convertFromDot(joinKey._2.getBackTickIdentity))))
      logInfo(s"Lookup table schema ${lookupDataset.schema.treeString}")

      if (join.getNonEquiJoinCondition != null) {
        var condition = NonEquiJoinConditionBuilder.convert(join.getNonEquiJoinCondition)
        if (!equiConditionColPairs.isEmpty) {
          condition = condition && equiConditionColPairs.reduce(_ && _)
        }
        logInfo(s"Root table ${rootFactDesc.getIdentity}, join table ${lookupDesc.getAlias}, non-equi condition: ${condition.toString()}")
        afterJoin = afterJoin.join(lookupDataset, condition, joinType)
      } else {
        val condition = equiConditionColPairs.reduce(_ && _)
        logInfo(s"Root table ${rootFactDesc.getIdentity}, join table ${lookupDesc.getAlias}, condition: ${condition.toString()}")
        if (inferFiltersEnabled) {
          afterJoin = afterJoin.join(FiltersUtil.inferFilters(pk, lookupDataset), condition, joinType)
        } else {
          afterJoin = afterJoin.join(lookupDataset, condition, joinType)
        }
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

  case class Statistics(totalCount: Long, columnBytes: Map[String, Long])

}
