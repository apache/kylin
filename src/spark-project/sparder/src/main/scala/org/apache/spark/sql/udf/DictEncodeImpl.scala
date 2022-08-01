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
package org.apache.spark.sql.udf

import java.util

import org.apache.spark.TaskContext
import org.apache.spark.dict.{NBucketDictionary, NGlobalDictionaryV2}
import org.apache.spark.util.TaskCompletionListener

object DictEncodeImpl {

  @transient val cacheBucketDict: ThreadLocal[java.util.HashMap[String, NBucketDictionary]] =
    new ThreadLocal[java.util.HashMap[String, NBucketDictionary]] {
      override def initialValue(): util.HashMap[String, NBucketDictionary] = new util.HashMap[String, NBucketDictionary]()
    }

  def evaluate(inputValue: String, dictParams: String, bucketSize: String): Long = {
    var cachedBucketDict = DictEncodeImpl.cacheBucketDict.get().get(dictParams)
    if (cachedBucketDict == null) {
      cachedBucketDict = initBucketDict(dictParams, bucketSize)
    }
    cachedBucketDict.encode(inputValue)
  }

  private def initBucketDict(dictParams: String, bucketSize: String): NBucketDictionary = {
    val partitionID = TaskContext.get.partitionId
    val encodeBucketId = partitionID % bucketSize.toInt
    val globalDict = new NGlobalDictionaryV2(dictParams)

    val cachedBucketDict = globalDict.loadBucketDictionary(encodeBucketId)
    DictEncodeImpl.cacheBucketDict.get.put(dictParams, cachedBucketDict)
    TaskContext.get().addTaskCompletionListener(new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = {
        DictEncodeImpl.cacheBucketDict.get().clear()
      }
    })
    DictEncodeImpl.cacheBucketDict.get().get(dictParams)
  }
}
