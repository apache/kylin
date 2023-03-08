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

package org.apache.kylin.engine.spark.job.stage.build.partition

import org.apache.kylin.engine.spark.job.SegmentJob
import org.apache.kylin.engine.spark.job.stage.BuildParam
import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.kylin.metadata.model.NDataModel
import org.junit.Assert
import org.mockito.Mockito
import org.scalatest.funsuite.AnyFunSuite

import com.google.common.collect.ImmutableBiMap

class PartitionRefreshColumnBytesTest extends AnyFunSuite {

  test("test PartitionRefreshColumnBytes getStageName") {
    val segmentJob = Mockito.mock(classOf[SegmentJob])
    val dataSegment = Mockito.mock(classOf[NDataSegment])
    val buildParam = Mockito.mock(classOf[BuildParam])

    val dataModel = Mockito.mock(classOf[NDataModel])
    Mockito.when(dataSegment.getModel).thenReturn(dataModel)
    val builder: ImmutableBiMap.Builder[Integer, NDataModel.Measure] = ImmutableBiMap.builder();
    Mockito.when(dataSegment.getModel.getEffectiveMeasures).thenReturn(builder.build())

    val partitionRefreshColumnBytes = new PartitionRefreshColumnBytes(segmentJob, dataSegment, buildParam)
    Assert.assertEquals("PartitionRefreshColumnBytes", partitionRefreshColumnBytes.getStageName)
  }
}
