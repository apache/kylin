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

import io.kyligence.kap.guava20.shaded.common.util.concurrent.RateLimiter
import org.apache.kylin.engine.spark.job.stage.BuildParam
import org.apache.kylin.engine.spark.job.{KylinBuildEnv, SegmentJob}
import org.apache.kylin.metadata.cube.model.{NBatchConstants, NDataSegment}

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

class PartitionBuildLayer(jobContext: SegmentJob, dataSegment: NDataSegment, buildParam: BuildParam)
  extends PartitionBuildStage(jobContext, dataSegment, buildParam) {

  private val rateLimiter: RateLimiter = createRateLimiter()

  override def execute(): Unit = {
    // Build layers.
    buildLayouts()
    // Drain results immediately after building.
    drain()
  }

  override def getStageName: String = "PartitionBuildLayer"

  override protected def drain(timeout: Long, unit: TimeUnit): Unit = {
    super.drain(timeout, unit)

    val buildJobInfos = KylinBuildEnv.get().buildJobInfos
    val layoutCountTotal = buildJobInfos.getSeg2cuboidsNumPerLayer.get(segmentId)
    if (rateLimiter.tryAcquire() && layoutCountTotal != null) {
      val layoutCount = layoutCountTotal.get() / partitions.size()
      updateStageInfo(null, null, mapAsJavaMap(Map(NBatchConstants.P_INDEX_SUCCESS_COUNT ->
        String.valueOf(layoutCount))))
    }
  }
}
