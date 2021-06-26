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

import java.io.IOException
import java.util
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.lock.DistributedLock
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.engine.spark.builder.CubeBuilderHelper._
import org.apache.kylin.engine.spark.job.NSparkCubingUtil
import org.apache.kylin.engine.spark.metadata.{ColumnDesc, SegmentInfo}
import org.apache.spark.dict.{NGlobalDictBuilderAssist, NGlobalDictionary}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.utils.SparkVersionUtils

import scala.collection.JavaConverters._

class CubeDictionaryBuilder(val dataset: Dataset[Row],
                            val seg: SegmentInfo,
                            val ss: SparkSession,
                            val colRefSet: util.Set[ColumnDesc]) extends Logging with Serializable {

  @transient
  val lock: DistributedLock = KylinConfig.getInstanceFromEnv.getDistributedLockFactory.lockForCurrentThread

  @throws[IOException]
  def buildDictSet(): Unit = {
    if (SparkVersionUtils.isLessThanSparkVersion("2.4", true)) {
      assert(!ss.conf.get("spark.sql.adaptive.enabled", "false").toBoolean,
        "Parameter 'spark.sql.adaptive.enabled' must be false when building global dictionary.")
    }
    logInfo(s"Start building global dictionaries V2 for seg $seg")
    val m = s"Build global dictionaries V2 for seg $seg succeeded"
    time(m, colRefSet.asScala.foreach(col => safeBuild(col)))
  }

  @throws[IOException]
  private[builder] def safeBuild(ref: ColumnDesc): Unit = {
    val sourceColumn = ref.identity
    lock.lock(getLockPath(sourceColumn), Long.MaxValue)
    try
      if (lock.lock(getLockPath(sourceColumn))) {
        val dictColDistinct = dataset.select(CubeDictionaryBuilder.wrapCol(ref)).distinct
        ss.sparkContext.setJobDescription("Calculate bucket size " + ref.identity)
        val bucketPartitionSize = DictionaryBuilderHelper.calculateBucketSize(seg, ref, dictColDistinct)
        val m = s"Build global dictionaries V2 for column $sourceColumn succeeded"
        time(m, build(ref, bucketPartitionSize, dictColDistinct))
      }
    finally lock.unlock(getLockPath(sourceColumn))
  }

  @throws[IOException]
  private[builder] def build(ref: ColumnDesc, bucketPartitionSize: Int, afterDistinct: Dataset[Row]): Unit = {
    val columnName = ref.identity
    logInfo(s"Start building global dictionaries V2 for column $columnName.")

    val globalDict = new NGlobalDictionary(seg.project, ref.tableAliasName, ref.columnName, seg.kylinconf.getHdfsWorkingDirectory)
    globalDict.prepareWrite()
    val broadcastDict = ss.sparkContext.broadcast(globalDict)

    ss.sparkContext.setJobDescription("Build dict " + columnName)
    val dictCol = col(afterDistinct.schema.fields.head.name)
    afterDistinct
      .filter(dictCol.isNotNull)
      .repartition(bucketPartitionSize, dictCol)
      .foreachPartition {
        iter: Iterator[Row] =>
          DictHelper.genDict(columnName, broadcastDict, iter)
      }

    globalDict.writeMetaDict(bucketPartitionSize, seg.kylinconf.getGlobalDictV2MaxVersions, seg.kylinconf.getGlobalDictV2VersionTTL)

    // after writing global dict, check the uniqueness for global dict
    NGlobalDictBuilderAssist.checkGlobalDict(ref, seg, bucketPartitionSize, ss)
  }

  private def getLockPath(pathName: String) = s"/${seg.project}${HadoopUtil.GLOBAL_DICT_STORAGE_ROOT}/$pathName/lock"

}

object CubeDictionaryBuilder {

  def wrapCol(ref: ColumnDesc): Column = {
    val colName = NSparkCubingUtil.convertFromDot(ref.identity)
    expr(colName).cast(StringType)
  }
}
