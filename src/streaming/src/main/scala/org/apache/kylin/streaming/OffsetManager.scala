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

package org.apache.spark.sql.kafka010

import org.apache.kafka.common.TopicPartition
import org.apache.kylin.common.KylinConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{Offset, StreamExecution, StreamingQueryWrapper}
import org.apache.spark.sql.kafka010.{JsonUtils, KafkaSourceOffset}

import java.util
import scala.collection.JavaConverters._

object OffsetRangeManager extends Logging {

  def getKafkaSourceOffset(offsetTuples: (String, Int, Long)*): KafkaSourceOffset = {
    KafkaSourceOffset(offsetTuples.map { case (t, p, o) => (new TopicPartition(t, p), o) }.toMap)
  }

  def awaitOffset(currentStream: StreamExecution, sourceIndex: Int, offset: Offset, timeoutMs: Long): Unit = {
    logInfo("compare offset")
    currentStream.awaitOffset(sourceIndex, offset, timeoutMs)
  }

  def currentOffsetRange(ss: SparkSession): (String, String) = {
    assert(ss.sessionState.streamingQueryManager.active.length == 1, "support only one active streaming query")
    val activeQuery = ss.sessionState.streamingQueryManager.active(0).asInstanceOf[StreamingQueryWrapper].streamingQuery
    val availableOffsets = activeQuery.availableOffsets.values.head.json()
    val committedOffsets = if (!activeQuery.committedOffsets.isEmpty) {
      activeQuery.committedOffsets.values.head.json()
    } else {
      availableOffsets.replaceAll("\\:\\d+", "\\:0")
    }
    (committedOffsets, availableOffsets)
  }

  def partitionOffsets(str: String): java.util.Map[Integer, java.lang.Long] = {

    if (KylinConfig.getInstanceFromEnv.isUTEnv) {
      val map = new util.HashMap[Integer, java.lang.Long]()
      map.put(new Integer(0), new java.lang.Long(0))
      map
    } else {
      JsonUtils.partitionOffsets(str).map { kv =>
        (Int.box(kv._1.partition()), Long.box(kv._2))
      }.asJava
    }

  }
}
