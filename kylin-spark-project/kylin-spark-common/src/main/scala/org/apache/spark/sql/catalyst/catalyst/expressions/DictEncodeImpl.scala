/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */
package org.apache.spark.sql.catalyst.expressions

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
