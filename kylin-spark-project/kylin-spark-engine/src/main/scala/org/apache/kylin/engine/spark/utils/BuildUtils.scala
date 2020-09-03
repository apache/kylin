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

package org.apache.kylin.engine.spark.utils

import java.io.IOException
import java.util.Locale

import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.engine.spark.NSparkCubingEngine.NSparkCubingStorage
import org.apache.kylin.engine.spark.job.NSparkCubingUtil
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity
import org.apache.kylin.engine.spark.metadata.{FunctionDesc, SegmentInfo}
import org.apache.kylin.measure.bitmap.BitmapMeasureType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object BuildUtils extends Logging {

  def findCountDistinctMeasure(layout: LayoutEntity): Boolean =
    layout.getOrderedMeasures.values.asScala.exists((function: FunctionDesc) =>
      function.returnType.dataType.equalsIgnoreCase(BitmapMeasureType.DATATYPE_BITMAP))

  @throws[IOException]
  def repartitionIfNeed(
							   layout: LayoutEntity,
							   storage: NSparkCubingStorage,
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
        layout,
        repartitionThresholdSize,
        summary,
        shardByColumns
      )

      val sortCols = NSparkCubingUtil.getColumns(layout.getOrderedDimensions.keySet)

      val repartitionNum = repartitioner.getRepartitionNumByStorage

      repartitioner.doRepartition(storage, path, repartitionNum, sortCols, sparkSession)
      repartitionNum
    } else {
      throw new RuntimeException(
        String.format(Locale.ROOT, "Temp path does not exist before repartition. Temp path: %s.", tempPath))
    }
  }

  @throws[IOException]
  def fillCuboidInfo(cuboid: LayoutEntity, strPath: String): Unit = {
    val fs = HadoopUtil.getWorkingFileSystem
    // when cuboid.getRows == 0, it means there is no data written, so set byte size to 0
    if (fs.exists(new Path(strPath)) && (cuboid.getRows > 0)) {
      val cs = HadoopUtil.getContentSummary(fs, new Path(strPath))
      cuboid.setFileCount(cs.getFileCount)
      cuboid.setByteSize(cs.getLength)
    } else {
      cuboid.setFileCount(0)
      cuboid.setByteSize(0)
    }
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

  def getColumnIndexMap(segInfo: SegmentInfo): Map[String, String] = {
    segInfo.allColumns.map(column => (column.id.toString, column.columnName)).toMap
  }
}
