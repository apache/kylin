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

package org.apache.kylin.query.runtime.plan

import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.junit.Assert

import java.util

class SegmentEmptyTest extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

    val prunedSegment1 = null
    val prunedSegment2 = new util.LinkedList[NDataSegment]
    val prunedSegment3 = new util.LinkedList[NDataSegment]
    prunedSegment3.add(new NDataSegment())

    val prunedStreamingSegment1 = null
    val prunedStreamingSegment2 = new util.LinkedList[NDataSegment]
    val prunedStreamingSegment3 = new util.LinkedList[NDataSegment]
    prunedStreamingSegment3.add(new NDataSegment())

    Assert.assertTrue(TableScanPlan.isSegmentsEmpty(prunedSegment1, prunedStreamingSegment1))
    Assert.assertTrue(TableScanPlan.isSegmentsEmpty(prunedSegment1, prunedStreamingSegment2))
    Assert.assertFalse(TableScanPlan.isSegmentsEmpty(prunedSegment1, prunedStreamingSegment3))

    Assert.assertTrue(TableScanPlan.isSegmentsEmpty(prunedSegment2, prunedStreamingSegment1))
    Assert.assertTrue(TableScanPlan.isSegmentsEmpty(prunedSegment2, prunedStreamingSegment2))
    Assert.assertFalse(TableScanPlan.isSegmentsEmpty(prunedSegment2, prunedStreamingSegment3))

    Assert.assertFalse(TableScanPlan.isSegmentsEmpty(prunedSegment3, prunedStreamingSegment1))
    Assert.assertFalse(TableScanPlan.isSegmentsEmpty(prunedSegment3, prunedStreamingSegment2))
    Assert.assertFalse(TableScanPlan.isSegmentsEmpty(prunedSegment3, prunedStreamingSegment3))
}
