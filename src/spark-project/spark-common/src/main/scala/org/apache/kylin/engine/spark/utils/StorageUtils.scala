/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.utils

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.kylin.common.KapConfig
import org.apache.kylin.common.util.{HadoopUtil, JsonUtil, RandomUtil}
import org.apache.kylin.engine.spark.filter.ParquetBloomFilter
import org.apache.kylin.measure.bitmap.BitmapMeasureType
import org.apache.kylin.metadata.cube.model.LayoutEntity
import org.apache.kylin.metadata.model.NDataModel

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object StorageUtils extends Logging {
  val MB: Long = 1024 * 1024

  def overwriteWithMessage(fs: FileSystem, src: Path, dst: Path): Unit = {
    if (fs.exists(dst)) {
      fs.delete(dst, true)
    }

    if (fs.rename(src, dst)) {
      logInfo(s"Rename src path ($src) to dst path ($dst) successfully.")
    } else {
      throw new RuntimeException(s"Rename src path ($src) to dst path ($dst) failed.")
    }
  }

  @deprecated
  // clean intermediate temp path after job or recover from an error job, usually with postfix '_temp'
  def cleanupPotentialTempFiles(fs: FileSystem, targetPath: Path, includeSelf: Boolean): Unit = {
    if (fs.exists(targetPath) && includeSelf) {
      fs.delete(targetPath, true)
      logInfo(s"Delete dir $targetPath")
    }
    if (fs.exists(targetPath.getParent)) {
      val namePrefix = targetPath.getName + "_temp"
      fs.listStatus(targetPath.getParent, new PathFilter {
        override def accept(destPath: Path): Boolean = {
          destPath.getName.startsWith(namePrefix)
        }
      }).map(_.getPath).foreach { tempPath =>
        if (fs.exists(tempPath)) {
          fs.delete(tempPath, true)
          logInfo(s"Delete temp dir $tempPath")
        }
      }
    }
  }

  def findCountDistinctMeasure(layout: LayoutEntity): Boolean =
    layout.getOrderedMeasures.values.asScala.exists((measure: NDataModel.Measure) =>
      measure.getFunction.getReturnType.equalsIgnoreCase(BitmapMeasureType.DATATYPE_BITMAP))

  def writeWithMetrics(data: DataFrame, path: String): JobMetrics = {
    withMetrics(data.sparkSession) {
      val writer = data.write.mode(SaveMode.Overwrite)
      ParquetBloomFilter.configBloomColumnIfNeed(data, writer)
      writer.parquet(path)
    }
  }

  def withMetrics(session: SparkSession)(body: => Unit): JobMetrics = {
    val queryExecutionId = RandomUtil.randomUUIDStr
    session.sparkContext.setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY, queryExecutionId)
    body
    val metrics = JobMetricsUtils.collectMetrics(queryExecutionId)
    session.sparkContext.setLocalProperty(QueryExecutionCache.N_EXECUTION_ID_KEY, null)
    QueryExecutionCache.removeQueryExecution(queryExecutionId)
    metrics
  }

  def calculateBucketNum(tempPath: String, layout: LayoutEntity, rowCount: Long, kapConfig: KapConfig): Int = {
    val fs = HadoopUtil.getWorkingFileSystem()
    if (fs.exists(new Path(tempPath))) {
      val summary = HadoopUtil.getContentSummary(fs, new Path(tempPath))
      val repartitionThresholdSize = if (findCountDistinctMeasure(layout)) {
        kapConfig.getParquetStorageCountDistinctShardSizeRowCount
      } else {
        kapConfig.getParquetStorageShardSizeRowCount
      }

      val partitionNumByStorage = getRepartitionNumByStorage(summary.getLength,
        kapConfig.getParquetStorageShardSizeMB,
        rowCount,
        repartitionThresholdSize)

      val extConfig = layout.getIndex.getModel.getProjectInstance.getConfig.getExtendedOverrides
      val configJson = extConfig.get("kylin.engine.shard-num-json")
      val shardByColumns = layout.getShardByColumns

      val repartitionNum = if (configJson != null) {
        try {
          val colToShardsNum = JsonUtil.readValueAsMap(configJson)
          // now we only has one shard by col
          val shardColIdentity = shardByColumns.asScala.map(layout.getIndex.getModel
            .getEffectiveDimensions.get(_).toString).mkString(",")
          val num = colToShardsNum.getOrDefault(shardColIdentity, String.valueOf(partitionNumByStorage)).toInt
          logInfo(s"Get  num in config, col identity is:$shardColIdentity, bucket num is $num.")
          num
        } catch {
          case th: Throwable =>
            logError("Error occurred when getting bucket num in config", th)
            partitionNumByStorage
        }
      } else {
        logInfo(s"Get partition num by file storage, partition num is $partitionNumByStorage.")
        partitionNumByStorage
      }
      repartitionNum
    } else {
      throw new RuntimeException(s"Temp path does not exist before repartition. Temp path: $tempPath.")
    }
  }

  private def getRepartitionNumByStorage(fileLength: Long, bucketSize: Int, totalRowCount: Long, rowCountSize: Long): Int = {
    val fileLengthRepartitionNum = Math.ceil(fileLength * 1.0 / MB / bucketSize).toInt
    val rowCountRepartitionNum = Math.ceil(1.0 * totalRowCount / rowCountSize).toInt
    val partitionSize = Math.ceil(1.0 * (fileLengthRepartitionNum + rowCountRepartitionNum) / 2).toInt
    logInfo(s"File length repartition num : $fileLengthRepartitionNum, row count Rpartition num: $rowCountRepartitionNum," +
      s" repartition num is : $partitionSize")
    partitionSize
  }

  def getCurrentYarnConfiguration: YarnConfiguration = {
    val conf = new YarnConfiguration()
    setSystemPropertiesToYarnYarnConfiguration(conf)
    conf
  }

  private def setSystemPropertiesToYarnYarnConfiguration(conf: YarnConfiguration): Unit = {
    System.getProperties.entrySet()
      .asScala
      .filter(_.getKey.asInstanceOf[String].startsWith("spark.hadoop."))
      .map(entry => (entry.getKey.asInstanceOf[String].substring("spark.hadoop.".length), entry.getValue.asInstanceOf[String]))
      .foreach(tp => conf.set(tp._1, tp._2))
  }

  /**
   * if write_hadoop_dir exists, Yarn Client Hadoop conf dir is write_hadoop_dir
   * if not exists, use system env: HADOOP_CONF_DIR
   * if HADOOP_CONF_DIR not exists. use system properties: kylin.hadoop.conf.dir
   */
  def getCurrentYarnConfigurationFromWriteCluster: YarnConfiguration = {
    val conf = new YarnConfiguration(HadoopUtil.getHadoopConfFromSparkEngine)
    setSystemPropertiesToYarnYarnConfiguration(conf)
    conf
  }
}
