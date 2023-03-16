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
package org.apache.spark.sql.execution.datasource

import org.apache.kylin.guava30.shaded.common.cache.{Cache, CacheBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.FileStatusCache

import java.util.concurrent.atomic.AtomicReference

object ShardFileStatusCache {
   var fsc: AtomicReference[FileStatusCache] = new AtomicReference[FileStatusCache]
   val segmentBuildTimeCache: Cache[String, java.lang.Long] = CacheBuilder.newBuilder().build()

   def getFileStatusCache(session: SparkSession): FileStatusCache = {
      if (fsc.get() == null) {
         fsc.set(FileStatusCache.getOrCreate(session))
      }
      fsc.get()
   }

   def getSegmentBuildTime(segmentId: String): Long = {
      val cacheTime = segmentBuildTimeCache.getIfPresent(segmentId)
      if (cacheTime == null) -1 else cacheTime
   }

   def refreshSegmentBuildTimeCache(segmentId: String, newBuildTime: Long): Unit = {
      segmentBuildTimeCache.put(segmentId, newBuildTime)
   }
}
