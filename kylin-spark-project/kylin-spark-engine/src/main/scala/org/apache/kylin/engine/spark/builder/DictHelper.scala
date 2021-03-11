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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.apache.spark.TaskContext
import org.apache.spark.dict.NGlobalDictionary
import org.apache.spark.internal.Logging

object DictHelper extends Logging{
  
  def genDict(columnName: String, broadcastDict: Broadcast[NGlobalDictionary],
              iter: Iterator[Row]): Unit = {
    val partitionID = TaskContext.get().partitionId()
    logInfo(s"Build partition dict col: ${columnName}, partitionId: $partitionID")
    val broadcastGlobalDict = broadcastDict.value
    val bucketDict = broadcastGlobalDict.loadBucketDictionary(partitionID)
    iter.foreach(dic => bucketDict.addRelativeValue(dic.getString(0)))
    bucketDict.saveBucketDict(partitionID)
  }
}
