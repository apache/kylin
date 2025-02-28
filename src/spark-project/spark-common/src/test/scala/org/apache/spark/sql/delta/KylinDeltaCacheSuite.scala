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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KylinConfig
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

import io.delta.standalone.actions.AddFile
import io.delta.standalone.{DeltaLog => JDeltaLog}

class KylinDeltaCacheSuite extends SparderBaseFunSuite
  with LocalMetadata with SharedSparkSession with Matchers {

  def getTestConfig: KylinConfig = {
    KylinConfig.createKylinConfig(new String())
  }

  test("cache DeltaLog and AddFile objects") {
    val path = new Path("/test/path")
    val version = 1L
    val deltaLog = JDeltaLog.forTable(new Configuration(), path)
    val addFile = AddFile.builder("/path", Map.empty[String, String].asJava, 100L,
      System.currentTimeMillis(), true).build()
    val cacheKey = (path, version)
    val cacheValue = (deltaLog, Seq(addFile))

    KylinDeltaCache.DeltaLogVersionCache.put(cacheKey, cacheValue)

    val cachedValue = KylinDeltaCache.DeltaLogVersionCache.getIfPresent(cacheKey)

    cachedValue should not be null
    cachedValue._1 shouldEqual deltaLog
    cachedValue._2 should contain theSameElementsAs Seq(addFile)
  }

  test("return null if no value is cached for a key") {
    val path = new Path("/non/existent/path")
    val version = 1L
    val cacheKey = (path, version)

    val cachedValue = KylinDeltaCache.DeltaLogVersionCache.getIfPresent(cacheKey)

    cachedValue should be(null)
  }
}
