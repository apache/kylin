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

package org.apache.kylin.query.asyncprofiler

import io.kyligence.kap.plugin.asyncprofiler.BuildAsyncProfilerSparkPlugin
import org.apache.kylin.common.KylinConfig
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.{SparkConf, SparkContext}
import org.mockito.Mockito.mock

import java.io.OutputStream

class AsyncProfilingTest extends AsyncPluginWithMeta {

  val sparkPluginName: String = classOf[BuildAsyncProfilerSparkPlugin].getName
  val flagFileDir: String = System.getProperty("java.io.tmpdir") + "default/jobStepId/"
  val actionFilePath: String = flagFileDir + "/action"
  val statusFileName: String = flagFileDir + "/status"
  val dumpFileName: String = flagFileDir + "/dump.tar.gz"

  test("init AsyncProfiling") {
    AsyncProfiling.asyncProfilerUtils
  }

  test("start and dump AsyncProfiling") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)

    sc = new SparkContext(conf)
    AsyncProfiling.start("")
    AsyncProfiling.dump("")

    sc.stop()
    sc = null
  }

  test("waitForResult AsyncProfiling") {
    KylinConfig.getInstanceFromEnv.setProperty("kylin.query.async-profiler-result-timeout", "1ms")

    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)


    sc = new SparkContext(conf)
    AsyncProfiling.start("")
    AsyncProfiling.dump("")
    AsyncProfiling.waitForResult(mock(classOf[OutputStream]))

    sc.stop()
    sc = null
  }

  test("cacheExecutorResult AsyncProfiling") {
    KylinConfig.getInstanceFromEnv.setProperty("kylin.query.async-profiler-result-timeout", "1ms")

    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)

    sc = new SparkContext(conf)
    AsyncProfiling.start("")
    AsyncProfiling.cacheExecutorResult("content", "1")

    sc.stop()
    sc = null
  }
}