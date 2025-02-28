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
package org.apache.spark.sql.delta


import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KylinConfig

import com.google.common.cache.{Cache, CacheBuilder}

import io.delta.standalone.actions.{AddFile => JAddFile}
import io.delta.standalone.{DeltaLog => JDeltaLog}

object KylinDeltaCache {

  private type DeltaLogCacheKey = (Path, Long)

  private type DeltaLogCacheValue = (JDeltaLog, Seq[JAddFile])

  val DeltaLogVersionCache: Cache[DeltaLogCacheKey, DeltaLogCacheValue] = {
    val config = KylinConfig.getInstanceFromEnv
    val expireThreshold = config.getV3DeltaLogCacheExpireThreshold
    val builder = CacheBuilder.newBuilder()
      .expireAfterAccess(expireThreshold, TimeUnit.SECONDS)
    builder.build[DeltaLogCacheKey, DeltaLogCacheValue]()
  }
}