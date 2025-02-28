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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ContentSummary, Path}
import org.apache.kylin.common.KapConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.engine.spark.job.NSparkCubingUtil
import org.apache.kylin.engine.spark.utils.StorageUtils.{MB, findCountDistinctMeasure}
import org.apache.kylin.engine.spark.utils.{JobMetrics, Metrics, Repartitioner, StorageUtils}
import org.apache.kylin.metadata.cube.model.LayoutEntity
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

object LayoutFormatWriter extends Logging {

  protected val TEMP_FLAG = "_temp_"

  /** Describes how output files should be placed in the filesystem. */
  case class OutputSpec(metrics: JobMetrics,
                        rowCount: Long,
                        hadoopConf: Configuration,
                        bucketNum: Int)

  def write(dataFrame: DataFrame,
            layout: LayoutEntity,
            outputPath: Path,
            kapConfig: KapConfig,
            storageListener: Option[StorageListener]): OutputSpec = {
    val ss = dataFrame.sparkSession
    val hadoopConf = ss.sparkContext.hadoopConfiguration
    val fs = outputPath.getFileSystem(hadoopConf)

    val dims = layout.getOrderedDimensions.keySet()
    val sortColumns = NSparkCubingUtil.getColumns(dims)

    if (kapConfig.isAggIndexAdaptiveBuildEnabled
      && unNeedRepartitionByShardCols(layout)) {
      val df = dataFrame
        .repartition()
        .sortWithinPartitions(sortColumns: _*)
      val metrics = StorageUtils.writeWithMetrics(df, outputPath.toString)
      val rowCount = metrics.getMetrics(Metrics.CUBOID_ROWS_CNT)
      OutputSpec(metrics, rowCount, hadoopConf, -1)
    } else {
      val tempPath = outputPath.toString + TEMP_FLAG + System.currentTimeMillis()
      val df = dataFrame.sortWithinPartitions(sortColumns: _*)
      val metrics = StorageUtils.writeWithMetrics(df, tempPath)
      val rowCount = metrics.getMetrics(Metrics.CUBOID_ROWS_CNT)
      storageListener.foreach(_.onPersistBeforeRepartition(df, layout))

      val bucketNum = StorageUtils.calculateBucketNum(tempPath, layout, rowCount, kapConfig)
      val summary = HadoopUtil.getContentSummary(fs, new Path(tempPath))
      val repartitionThresholdSize = if (findCountDistinctMeasure(layout)) {
        kapConfig.getParquetStorageCountDistinctShardSizeRowCount
      } else {
        kapConfig.getParquetStorageShardSizeRowCount
      }

      val repartitioner = new Repartitioner(
        kapConfig.getParquetStorageShardSizeMB,
        kapConfig.getParquetStorageRepartitionThresholdSize,
        needResetRowGroup(rowCount, summary, kapConfig),
        kapConfig.getParquetBlockSize,
        kapConfig.getParquetPageSizeRowCheckMax,
        rowCount,
        repartitionThresholdSize,
        summary,
        layout.getShardByColumns,
        layout.getOrderedDimensions.keySet().asList(),
        kapConfig.optimizeShardEnabled()
      )
      repartitioner.doRepartition(outputPath.toString, tempPath, bucketNum, ss)
      storageListener.foreach(_.onPersistAfterRepartition(ss.read.parquet(outputPath.toString), layout))
      OutputSpec(metrics, rowCount, hadoopConf, bucketNum)
    }
  }

  def unNeedRepartitionByShardCols(layout: LayoutEntity): Boolean = {
    layout.getShardByColumns == null || layout.getShardByColumns.isEmpty
  }

  def needResetRowGroup(totalRowCount: Long, contentSummary: ContentSummary, kapConfig: KapConfig): Boolean = {
    // per file size < threshold file size
    kapConfig.isResetParquetBlockSize &&
      (totalRowCount / (contentSummary.getLength * 1.0 / MB)) > kapConfig.getParquetRowCountPerMb
  }
}
