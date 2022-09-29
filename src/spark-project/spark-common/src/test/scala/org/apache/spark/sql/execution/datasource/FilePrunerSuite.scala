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

package org.apache.spark.sql.execution.datasource

import io.kyligence.kap.guava20.shaded.common.collect.Sets
import org.apache.kylin.common.exception.TargetSegmentNotFoundException
import org.apache.kylin.metadata.cube.model.{NDataSegment, NDataflow}
import org.apache.kylin.metadata.model.{SegmentStatusEnum, Segments}
import org.apache.spark.sql.common.SparderBaseFunSuite

import scala.collection.JavaConverters._

class FilePrunerSuite extends SparderBaseFunSuite {

  test("KE-37730: test check segment status") {
    val segment = new NDataSegment
    segment.setId("1")
    segment.setStatus(SegmentStatusEnum.READY)
    val segmentList = new Segments[NDataSegment]
    segmentList.add(segment)
    val mockDataFlow = new NDataflow
    mockDataFlow.setSegments(segmentList)

    val segDir1 = SegmentDirectory("1", List.empty[Long], null)
    val segDir2 = SegmentDirectory("2", List.empty[Long], null)

    FilePruner.checkSegmentStatus(Sets.newHashSet(Seq(segDir1).map(_.segmentID).asJavaCollection), mockDataFlow)
    val catchEx = intercept[TargetSegmentNotFoundException] {
      FilePruner.checkSegmentStatus(Sets.newHashSet(Seq(segDir1, segDir2).map(_.segmentID).asJavaCollection), mockDataFlow)
    }
    assert(catchEx.getMessage.equals("Cannot find target segment, and missing segment id: 2;"))
  }

}
