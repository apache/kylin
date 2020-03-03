package org.apache.kylin.engine.spark.builder

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.dict.NGlobalDictionaryV2
import org.apache.spark.sql.Row
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging

import scala.collection.mutable.ListBuffer

object DictHelper extends Logging{
  
  def genDict(columnName: String, broadcastDict: Broadcast[NGlobalDictionaryV2], iter: Iterator[Row]) = {
    val partitionID = TaskContext.get().partitionId()
    logInfo(s"Build partition dict col: ${columnName}, partitionId: $partitionID")
    val broadcastGlobalDict = broadcastDict.value
    val bucketDict = broadcastGlobalDict.loadBucketDictionary(partitionID)
    iter.foreach(dic => bucketDict.addRelativeValue(dic.getString(0)))
    bucketDict.saveBucketDict(partitionID)
    ListBuffer.empty.iterator
  }
}
