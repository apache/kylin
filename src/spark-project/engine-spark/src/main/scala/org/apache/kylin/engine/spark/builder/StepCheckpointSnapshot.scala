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

package org.apache.kylin.engine.spark.builder

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kylin.metadata.model.TableDesc

private[spark] class StepCheckpointSnapshot(val jobJobTmp: String, //
                                            val fs: FileSystem) extends StepCheckpoint {
  private lazy val basePath = s"$jobJobTmp/__step_checkpoint_snapshot/"

  def checkpoint(table: TableDesc): Unit = {
    // just create new empty file
    fs.createNewFile(getCheckpointPath(table))
  }

  def canSkip(table: TableDesc): Boolean = {
    fs.exists(getCheckpointPath(table))
  }

  private def getCheckpointPath(table: TableDesc): Path = {
    new Path(basePath + table.getIdentity)
  }
}
