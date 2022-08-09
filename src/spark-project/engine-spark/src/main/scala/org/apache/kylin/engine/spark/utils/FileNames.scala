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
package org.apache.kylin.engine.spark.utils

import org.apache.hadoop.fs.Path
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.metadata.model.TableDesc

object FileNames {
  /** Returns the path for a given snapshot file. */
  def snapshotFile(tableDesc: TableDesc): Path =
    new Path(tableDesc.getProject + HadoopUtil.SNAPSHOT_STORAGE_ROOT + "/" + tableDesc.getIdentity)

  def snapshotFileWithWorkingDir(tableDesc: TableDesc, workingDir: String): Path =
    new Path(workingDir, snapshotFile(tableDesc))

  def snapshotFile(project: String, identity: String): Path =
    new Path(project + HadoopUtil.SNAPSHOT_STORAGE_ROOT + "/" + identity)

  def snapshotFileWithWorkingDir(project: String, identity: String, workingDir: String): Path = {
    new Path(workingDir, snapshotFile(project, identity))
  }
}
