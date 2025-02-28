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

package org.apache.spark.sql.datasource.storage

import java.lang

import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KapConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.engine.spark.job.NSparkCubingUtil
import org.apache.kylin.metadata.cube.model.{LayoutEntity, NDataLayoutDetailsManager, NDataSegment, NDataflow}
import org.apache.kylin.metadata.model.TableDesc
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.datasource.storage.KylinDeltaStorage.{doExecuteWithRetry, replaceFileIndex}
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.{DeltaTableUtils, KylinDeltaLogFileIndex, KylinDeltaTableScan, SubqueryTransformerHelper}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import io.delta.tables.DeltaTable
import jodd.util.ThreadUtil.sleep

class KylinDeltaStorage extends StorageStore with SubqueryTransformerHelper {

  override def getStoragePathWithoutPrefix(project: String, dataflowId: String, segmentId: String = null,
                                           layoutId: Long, bucketId: Long): String = {
    s"${project}/delta/${dataflowId}/${layoutId}"
  }

  override def saveSegmentLayout(
                                  layout: LayoutEntity,
                                  segment: NDataSegment,
                                  kapConfig: KapConfig,
                                  dataFrame: DataFrame, bucketId: Long): WriteTaskStats = {
    val outputPath = new Path(getStoragePath(segment, layout.getId, bucketId))
    val retryTimes = kapConfig.getKylinConfig.getKylinDeltaStorageWriteRetyTimes
    val dims = layout.getOrderedDimensions.keySet()
    val sortColumns = NSparkCubingUtil.getColumns(dims)
    var replaceCondition: String = null

    if (layout.getModel.isIncrementBuildOnExpertMode) {
      val partitionCol = layout.getModel.getPartitionDesc.getPartitionDateColumnRef
      val partitionColIndex = layout.getDimensionPos(partitionCol)
      replaceCondition =
        segment.getPartitionCondition.replace(partitionCol.getBackTickExp, s"`${partitionColIndex}`")
      doExecuteWithRetry(retryTimes) {
        dataFrame
          .repartition()
          .sortWithinPartitions(sortColumns: _*)
          .write
          .format("delta")
          .mode(SaveMode.Overwrite)
          .option("replaceWhere", replaceCondition)
          .save(outputPath.toString)
      }
      logInfo(s"Write layout ${layout.getId} to $outputPath finished.")
    } else {
      // Todo set maxRecordsPerFile
      doExecuteWithRetry(retryTimes) {
        dataFrame
          .repartition()
          .sortWithinPartitions(sortColumns: _*)
          .write
          .format("delta")
          .mode(SaveMode.Overwrite)
          .save(outputPath.toString)
      }
      logInfo(s"Full build $outputPath finished.")
    }
    if (segment.getDataflow.getConfig.isAutoOptimizeAfterBuildEnabled) {
      optimizeKylinTable(dataFrame.sparkSession, layout, replaceCondition)
    }
    null
  }

  private def optimizeKylinTable(spark: SparkSession, layout: LayoutEntity, condition: String): Unit = {
    logInfo(s"Start to optimize for ${layout.getId}")
    optimizeZOrder(spark, layout, condition)
    optimizeCompaction(spark, layout, condition)
  }

  def partitionByGeneratedCol(layout: LayoutEntity, originalCol: StructField, generatedCol: String): Unit = {
    val tablePath = getStoragePath(layout)
    logInfo(s"Set partition col $generatedCol for layout: ${tablePath}")
    logInfo(s"Before set partition for $tablePath , schema: ${DeltaTable.forPath(tablePath).toDF.schema.simpleString} ")
    val addCol = DeltaTable.columnBuilder(originalCol.name)
      .dataType(originalCol.dataType)
      .nullable(originalCol.nullable)
      .generatedAlwaysAs(generatedCol)
      .build

    val partitionTable = DeltaTable.replace()
      .location(tablePath)
      .addColumn(addCol)
      .partitionedBy(addCol.name)
      .execute()
    logInfo(s"After set partition for $tablePath , schema: ${partitionTable.toDF.schema.simpleString} ")
  }

  def optimizeCompaction(spark: SparkSession, layout: LayoutEntity, condition: String = null): Unit = {
    val tablePath = getStoragePath(layout)
    val model = layout.getModel
    val layoutDetails = NDataLayoutDetailsManager.getInstance(model.getConfig, model.getProject)
      .getNDataLayoutDetails(model.getId, layout.getId)
    if (layoutDetails != null && layoutDetails.getMaxCompactionFileSizeInBytes != 0) {
      spark.sessionState.conf.setLocalProperty("spark.databricks.delta.optimize.maxFileSize",
        layoutDetails.getMaxCompactionFileSizeInBytes.toString)
    }
    if (layoutDetails != null && layoutDetails.getMinCompactionFileSizeInBytes != 0) {
      spark.sessionState.conf.setLocalProperty("spark.databricks.delta.optimize.minFileSize",
        layoutDetails.getMinCompactionFileSizeInBytes.toString)
    }
    if (condition == null || layoutDetails == null
      || layoutDetails.getPartitionColumns == null || layoutDetails.getPartitionColumns.isEmpty) {
      logInfo(s"Optimize compaction for layout: ${tablePath}")
      DeltaTable.forPath(spark, tablePath)
        .optimize()
        .executeCompaction()
    } else {
      logInfo(s"Optimize compaction for layout: ${tablePath} with condition ${condition}")
      DeltaTable.forPath(tablePath)
        .optimize()
        .where(condition)
        .executeCompaction()
    }
  }

  def optimizeZOrder(spark: SparkSession, layout: LayoutEntity, condition: String = null): Unit = {
    val tablePath = getStoragePath(layout)
    val model = layout.getModel
    val layoutDetails = NDataLayoutDetailsManager.getInstance(model.getConfig, model.getProject)
      .getNDataLayoutDetails(model.getId, layout.getId)
    if (layoutDetails != null && layoutDetails.getZorderByColumns != null) {
      val zOrderByColumns = layoutDetails.getZorderByColumns
      if (condition == null || layoutDetails == null
        || layoutDetails.getPartitionColumns == null || layoutDetails.getPartitionColumns.isEmpty) {
        logInfo(s"do zorder optimize for layout: ${tablePath}")
        DeltaTable.forPath(tablePath).optimize().executeZOrderBy(zOrderByColumns.asScala: _*)
      } else {
        logInfo(s"do zorder optimize for layout: ${tablePath} with condition ${condition}")
        DeltaTable.forPath(spark, tablePath)
          .optimize()
          .where(condition)
          .executeZOrderBy(zOrderByColumns.asScala: _*)
      }
    }
  }

  override def read(
                     dataflow: NDataflow,
                     layout: LayoutEntity,
                     sparkSession: SparkSession,
                     extraOptions: Map[String, String]): LogicalPlan = {

    val plan = loadKylinTable(sparkSession, dataflow.getDataLayoutDetails(layout.getId).getLocation)
    val pruningMode = extraOptions.getOrElse("filePruningMode", "Local")
    if (pruningMode.equalsIgnoreCase("LOCAL")) {
      replaceFileIndex(plan)
    } else {
      plan
    }
  }

  private def loadKylinTable(spark: SparkSession, tablePath: String): LogicalPlan = {
    DeltaTable.forPath(spark, tablePath).toDF.queryExecution.logical
  }

  override def readSpecialSegment(segment: NDataSegment,
                                  layout: LayoutEntity,
                                  sparkSession: SparkSession,
                                  extraOptions: Map[String, String]): DataFrame = {
    val storagePath = getStoragePath(segment, layout.getId)
    if (layout.getModel.isIncrementBuildOnExpertMode) {
      val partitionCol = layout.getModel.getPartitionDesc.getPartitionDateColumnRef
      val partitionColIndex = layout.getDimensionPos(partitionCol)
      val replaceCondition =
        segment.getPartitionCondition.replace(partitionCol.getBackTickExp, s"`${partitionColIndex}`")
      DeltaTable.forPath(sparkSession, storagePath).toDF.where(replaceCondition)
    } else {
      DeltaTable.forPath(sparkSession, storagePath).toDF
    }
  }

  override def readSpecialSegment(segment: NDataSegment,
                                  layout: LayoutEntity,
                                  partitionId: lang.Long,
                                  sparkSession: SparkSession): DataFrame = {
    null
  }
}

object KylinDeltaStorage extends Logging {
  def getSnapshotPath(tableDesc: TableDesc): Path = {
    val hdfsWorkingDir = KapConfig.wrap(tableDesc.getConfig).getMetadataWorkingDirectory
    val pathStr = s"${hdfsWorkingDir}/${tableDesc.getProject}/table_snapshot/delta/${tableDesc.getIdentity}"
    new Path(pathStr)
  }

  @tailrec
  def doExecuteWithRetry(int: Int)(f: => Unit): Unit = {
    try {
      f
    } catch {
      case e: Throwable =>
        if (int > 0) {
          logWarning(s"delta storage execute failed, do retry, remain $int times retry times", e)
          sleep(100)
          doExecuteWithRetry(int - 1)(f)
        } else {
          throw e
        }
      case _ => throw new RuntimeException("unknown exception")
    }
  }

  def replaceFileIndex(target: LogicalPlan): LogicalPlan = {
    target transform {
      case scan@KylinDeltaTableScan(_, _, fileIndex, _, _) =>
        val kylinIndex = getKylinTahoeLogFileIndex(fileIndex)
        DeltaTableUtils.replaceFileIndex(scan, kylinIndex)
    }
  }

  private def getKylinTahoeLogFileIndex(fileIndex: TahoeLogFileIndex): KylinDeltaLogFileIndex = {
    KylinDeltaLogFileIndex(fileIndex.spark,
      fileIndex.deltaLog,
      fileIndex.path,
      fileIndex.snapshotAtAnalysis,
      fileIndex.partitionFilters,
      fileIndex.isTimeTravelQuery)
  }
}
