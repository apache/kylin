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

package org.apache.spark.dict

import java.io.IOException
import java.util

import org.apache.kylin.engine.spark.metadata.{SegmentInfo, ColumnDesc}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConverters._

object NGlobalDictBuilderAssist extends Logging {

  @throws[IOException]
  def resize(ref: ColumnDesc, desc: SegmentInfo, bucketPartitionSize: Int, ss: SparkSession): Unit = {
    val globalDict = new NGlobalDictionary(desc.project, ref.tableAliasName, ref.columnName, desc.kylinconf.getHdfsWorkingDirectory)

    val broadcastDict = ss.sparkContext.broadcast(globalDict)
    globalDict.prepareWrite()

    import ss.implicits._
    val existsDictDs = ss.createDataset(0 to bucketPartitionSize)
      .flatMap {
        bucketId =>
          val gDict: NGlobalDictionary = broadcastDict.value
          val bucketDict: NBucketDictionary = gDict.loadBucketDictionary(bucketId)
          val tupleList = new util.ArrayList[(String, Long)](bucketDict.getAbsoluteDictMap.size)
          bucketDict.getAbsoluteDictMap.object2LongEntrySet.asScala
            .foreach(dictTuple => tupleList.add((dictTuple.getKey, dictTuple.getLongValue)))
          tupleList.asScala.iterator
      }

    ss.sparkContext.setJobDescription("Resize dict " + ref.identity)
    existsDictDs
      .repartition(bucketPartitionSize, col(existsDictDs.schema.head.name).cast(StringType))
      .foreachPartition {
        iter: Iterator[(String, Long)] =>
          val partitionID = TaskContext.get().partitionId()
          logInfo(s"Rebuild partition dict col: ${ref.identity}, partitionId: $partitionID")
          val d = broadcastDict.value
          val bucketDict = d.createNewBucketDictionary()
          while (iter.hasNext) {
            val dictTuple = iter.next
            bucketDict.addAbsoluteValue(dictTuple._1, dictTuple._2)
          }
          bucketDict.saveBucketDict(partitionID)
      }

    globalDict.writeMetaDict(bucketPartitionSize,
      desc.kylinconf.getGlobalDictV2MaxVersions, desc.kylinconf.getGlobalDictV2VersionTTL)
  }

  /**
   * check the global dict
   */
  @throws[IOException]
  def checkGlobalDict(ref: ColumnDesc, desc: SegmentInfo, bucketPartitionSize: Int,
                      ss: SparkSession): Unit = {
    if (desc.kylinconf.isCheckGlobalDictV2) {
      val globalDict = new NGlobalDictionary(desc.project, ref.tableAliasName, ref.columnName, desc.kylinconf.getHdfsWorkingDirectory)
      val broadcastDict = ss.sparkContext.broadcast(globalDict)
      import ss.implicits._
      val existsDictDs = ss.createDataset(0 to bucketPartitionSize)
        .flatMap {
          bucketId =>
            val gDict: NGlobalDictionary = broadcastDict.value
            val bucketDict: NBucketDictionary = gDict.loadBucketDictionary(bucketId)
            val tupleList = new util.ArrayList[(String, Long)](bucketDict.getAbsoluteDictMap.size)
            bucketDict.getAbsoluteDictMap.object2LongEntrySet.asScala
              .foreach(dictTuple => tupleList.add((dictTuple.getKey, dictTuple.getLongValue)))
            tupleList.asScala.iterator
        }
      val valueCount = existsDictDs.dropDuplicates("_1").count()
      val keyCount = existsDictDs.dropDuplicates("_2").count()
      if (valueCount != keyCount) {
        logError(s"Global dict build error on column ${ref.columnName}, " +
          s"key distinct count is ${keyCount}, and value distinct count is ${valueCount}.")
        throw new RuntimeException(s"Global dict build error on column ${ref.columnName}, " +
          s"key distinct count is ${keyCount}, and value distinct count is ${valueCount}.")
      }
    }
  }
}
