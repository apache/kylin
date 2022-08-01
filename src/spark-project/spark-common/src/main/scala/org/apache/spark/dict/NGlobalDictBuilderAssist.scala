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

import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

import scala.collection.JavaConverters._

object NGlobalDictBuilderAssist extends Logging {

  @throws[IOException]
  def resize(ref: TblColRef, seg: NDataSegment, bucketPartitionSize: Int, ss: SparkSession): Unit = {
    val globalDict = new NGlobalDictionaryV2(seg.getProject, ref.getTable, ref.getName, seg.getConfig.getHdfsWorkingDirectory)

    val broadcastDict = ss.sparkContext.broadcast(globalDict)
    globalDict.prepareWrite()

    import ss.implicits._
    val existsDictDs = ss.createDataset(0 to bucketPartitionSize)
      .flatMap {
        bucketId =>
          val gDict: NGlobalDictionaryV2 = broadcastDict.value
          val bucketDict: NBucketDictionary = gDict.loadBucketDictionary(bucketId)
          val tupleList = new util.ArrayList[(String, Long)](bucketDict.getAbsoluteDictMap.size)
          bucketDict.getAbsoluteDictMap.object2LongEntrySet.asScala
            .foreach(dictTuple => tupleList.add((dictTuple.getKey, dictTuple.getLongValue)))
          tupleList.asScala.iterator
      }

    ss.sparkContext.setJobDescription("Resize dict " + ref.getIdentity)
    existsDictDs.repartition(bucketPartitionSize, col(existsDictDs.schema.head.name).cast(StringType))
      // https://issues.apache.org/jira/browse/SPARK-32051
      .foreachPartition((iter: Iterator[(String, Long)]) => {
        val partitionID = TaskContext.get().partitionId()
        logInfo(s"Rebuild partition dict col: ${ref.getTable + "." + ref.getName}, partitionId: $partitionID")
        val d = broadcastDict.value
        val bucketDict = d.createNewBucketDictionary()
        iter.foreach(tpl => bucketDict.addAbsoluteValue(tpl._1, tpl._2))

        bucketDict.saveBucketDict(partitionID)
      })

    globalDict.writeMetaDict(bucketPartitionSize,
      seg.getConfig.getGlobalDictV2MaxVersions, seg.getConfig.getGlobalDictV2VersionTTL)
  }

}
