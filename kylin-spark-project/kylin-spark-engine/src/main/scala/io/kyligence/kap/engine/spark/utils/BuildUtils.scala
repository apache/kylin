/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.kyligence.kap.engine.spark.utils

import java.io.IOException

import io.kyligence.kap.engine.spark.NSparkCubingEngine
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.kylin.common.util.{HadoopUtil, JsonUtil}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.metadata.cube.ManagerHub
import org.apache.kylin.engine.spark.metadata.cube.model.{CubeUpdate2, DataLayout, DataSegment, LayoutEntity, MeasureDesc}
import org.apache.kylin.measure.bitmap.BitmapMeasureType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object BuildUtils extends Logging {

  def findCountDistinctMeasure(layout: LayoutEntity): Boolean =
    layout.getOrderedMeasures.values.asScala.exists((measure: MeasureDesc) =>
      measure.getFunction.getReturnType.equalsIgnoreCase(BitmapMeasureType.DATATYPE_BITMAP))

  @throws[IOException]
  def repartitionIfNeed(
                         layout: LayoutEntity,
                         dataCuboid: DataLayout,
                         storage: NSparkCubingEngine.NSparkCubingStorage,
                         path: String,
                         tempPath: String,
                         config: KylinConfig,
                         sparkSession: SparkSession): Int = {
    val fs = HadoopUtil.getWorkingFileSystem()
    if (fs.exists(new Path(tempPath))) {
      val summary = HadoopUtil.getContentSummary(fs, new Path(tempPath))
      var repartitionThresholdSize = config.getParquetStorageShardSizeRowCount
      if (findCountDistinctMeasure(layout)) {
        repartitionThresholdSize = config.getParquetStorageCountDistinctShardSizeRowCount
      }
      val shardByColumns = layout.getShardByColumns
      val repartitioner = new Repartitioner(
        config.getParquetStorageShardSizeMB,
        config.getParquetStorageRepartitionThresholdSize,
        dataCuboid.getRows,
        repartitionThresholdSize,
        summary,
        shardByColumns
      )

      val sortCols = NSparkCubingUtil.getColumns(layout.getOrderedDimensions.keySet)

      val extConfig = layout.getIndex.getCube.getConfig.getExtendedOverrides
      val configJson = extConfig.get("kylin.engine.shard-num-json")

      val numByFileStorage = repartitioner.getRepartitionNumByStorage
      val repartitionNum = if (configJson != null) {
        try {
          val colToShardsNum = JsonUtil.readValueAsMap(configJson)

          // now we only has one shard by col
          val shardColIdentity = shardByColumns.asScala.map(layout.getIndex.getModel
            .getEffectiveDimenionsMap.get(_).toString).mkString(",")
          val num = colToShardsNum.getOrDefault(shardColIdentity, String.valueOf(numByFileStorage)).toInt
          logInfo(s"Get shard num in config, col identity is:$shardColIdentity, shard num is $num.")
          num
        } catch {
          case th: Throwable =>
            logError("Some thing went wrong when getting shard num in config", th)
            numByFileStorage
        }

      } else {
        logInfo(s"Get shard num by file size, shard num is $numByFileStorage.")
        numByFileStorage
      }

      repartitioner.doRepartition(storage, path, repartitionNum, sortCols, sparkSession)
      repartitionNum
    } else {
      throw new RuntimeException(
        String.format("Temp path does not exist before repartition. Temp path: %s.", tempPath))
    }
  }

  @throws[IOException]
  def fillCuboidInfo(cuboid: DataLayout): Unit = {
    val strPath = NSparkCubingUtil.getStoragePath(cuboid)
    val fs = HadoopUtil.getWorkingFileSystem
    if (fs.exists(new Path(strPath))) {
      val cs = HadoopUtil.getContentSummary(fs, new Path(strPath))
      cuboid.setFileCount(cs.getFileCount)
      cuboid.setByteSize(cs.getLength)
    } else {
      cuboid.setFileCount(0)
      cuboid.setByteSize(0)
    }
  }

  def updateDataFlow(seg: DataSegment, dataCuboid: DataLayout, conf: KylinConfig, project: String): Unit = {
    logInfo(s"Update layout ${dataCuboid.getLayoutId} in dataflow ${seg.getId}, segment ${seg.getId}")
    val update = new CubeUpdate2(seg.getCube.getUuid)
    update.setToAddOrUpdateLayouts(dataCuboid)
//    NDataflowManager.getInstance(conf, project).updateDataflow(update)
    ManagerHub.updateCube(conf, update)
  }

  def getCurrentYarnConfiguration: YarnConfiguration = {
    val conf = new YarnConfiguration()
    System.getProperties.entrySet()
      .asScala
      .filter(_.getKey.asInstanceOf[String].startsWith("spark.hadoop."))
      .map(entry => (entry.getKey.asInstanceOf[String].substring("spark.hadoop.".length), entry.getValue.asInstanceOf[String]))
      .foreach(tp => conf.set(tp._1, tp._2))
    conf
  }
}
