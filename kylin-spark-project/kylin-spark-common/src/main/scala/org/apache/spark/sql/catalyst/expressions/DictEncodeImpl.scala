package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.TaskContext
import java.util

import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.dict.{NBucketDictionary, NGlobalDictionaryV2}

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
