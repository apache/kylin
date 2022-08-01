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
package org.apache.kylin.streaming.common

import org.apache.kylin.metadata.cube.cuboid.NSpanningTree
import org.apache.kylin.metadata.cube.model.NDataflow
import org.apache.kylin.metadata.model.SegmentRange
import org.apache.kylin.streaming.CreateStreamingFlatTable
import org.apache.kylin.streaming.jobs.StreamingDFBuildJob
import org.apache.spark.sql.DataFrame

class MicroBatchEntry(val batchDF: DataFrame, val batchId: Long, val timeColumn: String,
                      val streamFlatTable: CreateStreamingFlatTable, val df: NDataflow,
                      val nSpanningTree: NSpanningTree, val builder: StreamingDFBuildJob,
                      var sr: SegmentRange.KafkaOffsetPartitionedSegmentRange) extends Serializable {

  def setSegmentRange(sr: SegmentRange.KafkaOffsetPartitionedSegmentRange): Unit = {
    this.sr = sr;
  }

}
