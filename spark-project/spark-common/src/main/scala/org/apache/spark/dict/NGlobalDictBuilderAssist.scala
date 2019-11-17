/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
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
 */

package org.apache.spark.dict

import java.io.IOException
import java.util

import io.kyligence.kap.metadata.cube.model.NDataSegment
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
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
    existsDictDs
      .repartition(bucketPartitionSize, col(existsDictDs.schema.head.name).cast(StringType))
      .mapPartitions {
        iter =>
          val partitionID = TaskContext.get().partitionId()
          logInfo(s"Rebuild partition dict col: ${ref.getTable + "." + ref.getName}, partitionId: $partitionID")
          val d = broadcastDict.value
          val bucketDict = d.createNewBucketDictionary()
          while (iter.hasNext) {
            val dictTuple = iter.next
            bucketDict.addAbsoluteValue(dictTuple._1, dictTuple._2)
          }
          bucketDict.saveBucketDict(partitionID)
          Iterator.empty
      }(RowEncoder.apply(existsDictDs.schema))
      .count()

    globalDict.writeMetaDict(bucketPartitionSize,
      seg.getConfig.getGlobalDictV2MaxVersions, seg.getConfig.getGlobalDictV2VersionTTL)
  }

}
