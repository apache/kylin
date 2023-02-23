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

package org.apache.kylin.query.plugin.diagnose

import com.codahale.metrics.MetricRegistry
import org.apache.kylin.query.plugin.SparkPluginWithMeta
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert

import java.nio.file.Files
import java.util

class DiagnoseExecutorPluginTest extends SparkPluginWithMeta {

  val sparkPluginName: String = classOf[DiagnoseSparkPlugin].getName
  val executorPluginTest = new DiagnoseExecutorPlugin()
  val mockPluginCtx = new MockPluginCtx
  val tempContainerDir = Files.createTempDirectory("PluginContainerTest")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)
    sc = new SparkContext(conf)

    mockPluginCtx.sparkConf = sc.getConf
    mockPluginCtx.hdfsDir = tempContainerDir.toString
    mockPluginCtx.mockId = "mockId"

    executorPluginTest.setCtx(mockPluginCtx)
    executorPluginTest.setContainerDir(tempContainerDir.toFile)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    mockPluginCtx.clear()
  }


  test("Test executor plugin") {
    val filePath = Files.createTempFile(tempContainerDir, "gc", "log")
    Assert.assertTrue(filePath.toFile.exists())

    mockPluginCtx.message = DiagnoseConstant.COLLECT
    executorPluginTest.checkAndDiagnose()

    mockPluginCtx.message = DiagnoseConstant.NOP
    executorPluginTest.checkAndDiagnose()

    mockPluginCtx.message = DiagnoseConstant.EMPTY
    executorPluginTest.checkAndDiagnose()
  }
}

class MockPluginCtx() extends PluginContext {
  var message: String = _
  var mockId: String = _
  var hdfsDir: String = _
  var sparkConf: SparkConf = _

  override def ask(input: Any): String = {
    if (DiagnoseConstant.HDFSDIR.equals(input)) {
      hdfsDir
    } else {
      message
    }
  }

  override def executorID(): String = mockId

  override def conf(): SparkConf = sparkConf

  override def metricRegistry(): MetricRegistry = null

  override def hostname(): String = "MockHostname"

  override def resources(): util.Map[String, ResourceInformation] = null

  override def send(message: Any): Unit = {}

  def clear(): Unit = {
    message = null
    mockId = null
    hdfsDir = null
    sparkConf = null
  }

}
