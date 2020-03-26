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

package org.apache.spark.sql.utils

import org.apache.kylin.engine.spark.metadata.SegmentInfo
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity

object CuboidLayoutChooser {
  def selectLayoutForBuild(segment: SegmentInfo, entity: LayoutEntity): LayoutEntity = {
    val candidate = segment.layouts
      .filter(index => index.fullyDerive(entity))
      .filter(layout => !segment.toBuildLayouts.contains(layout))
    if (candidate.isEmpty) {
      null
    } else {
      candidate.minBy(_.getRows)
    }
  }
}
