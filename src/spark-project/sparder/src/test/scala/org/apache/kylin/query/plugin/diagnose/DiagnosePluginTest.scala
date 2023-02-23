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

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.query.plugin.SparkPluginWithMeta
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert

class DiagnosePluginTest extends SparkPluginWithMeta {

  val sparkPluginName: String = classOf[DiagnoseSparkPlugin].getName
  val diagPlugin = new DiagnoseSparkPlugin
  val driverPluginTest = new DiagnoseDriverPlugin()
  val executorPluginTest = new DiagnoseExecutorPlugin()

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)
    sc = new SparkContext(conf)
  }


  test("Test trigger gc collection") {
    // get gc success
    DiagnoseHelper.collectSparderExecutorGc()
    Assert.assertEquals(DiagnoseConstant.STATE_WAIT, DiagnoseHelper.state)

    // get gc failed
    DiagnoseHelper.resultCollectionTimeout = 100L
    DiagnoseHelper.activeExecutorCount = 1
    DiagnoseHelper.collectSparderExecutorGc()
    Assert.assertEquals(DiagnoseConstant.STATE_WAIT, DiagnoseHelper.state)
  }


  test("Test driver plugin") {
    // NEXTCMD
    DiagnoseHelper.setState(DiagnoseConstant.STATE_WAIT)
    var reply = driverPluginTest.receive(DiagnoseConstant.NEXTCMD)
    Assert.assertEquals(DiagnoseConstant.NOP, reply)

    DiagnoseHelper.setState(DiagnoseConstant.STATE_COLLECT)
    reply = driverPluginTest.receive(DiagnoseConstant.NEXTCMD)
    Assert.assertEquals(DiagnoseConstant.COLLECT, reply)

    // SENDRESULT
    DiagnoseHelper.initGcCount(2)
    reply = driverPluginTest.receive(DiagnoseConstant.SENDRESULT)
    Assert.assertEquals(DiagnoseConstant.EMPTY, reply)
    Assert.assertEquals(1, DiagnoseHelper.gcResult.getCount)

    // HDFSDIR
    reply = driverPluginTest.receive(DiagnoseConstant.HDFSDIR)
    Assert.assertEquals(KylinConfig.getInstanceFromEnv.getHdfsWorkingDirectory, reply)

    // Other
    reply = driverPluginTest.receive("Other")
    Assert.assertEquals(DiagnoseConstant.EMPTY, reply)
  }

}
