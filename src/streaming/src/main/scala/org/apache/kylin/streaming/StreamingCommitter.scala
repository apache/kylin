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

package org.apache.kylin.streaming

import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.metadata.cube.model.LayoutEntity
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.util.HashMap

object StreamingCommitter extends Logging {

  val fs = HadoopUtil.getWorkingFileSystem
  val TEMP_DIR_SUFFIX = "_temp"
  val cuboidRowCount = new HashMap[Long, Long]()

  def saveAndCachedataset(dataset: Dataset[Row], ss: SparkSession,
                          layout: LayoutEntity): Unit = {
    dataset.persist(StorageLevel.MEMORY_AND_DISK)
    var start = System.currentTimeMillis()
    val rowsNum = dataset.count()
    cuboidRowCount.put(layout.getId, rowsNum)
    logInfo(s"eval rowNum cost time ${System.currentTimeMillis() - start}")
  }

}
