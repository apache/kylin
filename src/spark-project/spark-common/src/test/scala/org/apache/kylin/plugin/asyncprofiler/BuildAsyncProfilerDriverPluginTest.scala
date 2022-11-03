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

package org.apache.kylin.plugin.asyncprofiler

import io.kyligence.kap.plugin.asyncprofiler.BuildAsyncProfilerSparkPlugin
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.asyncprofiler.{AsyncProfilerToolTest, Message}
import org.apache.kylin.common.util.{HadoopUtil, NLocalFileMetadataTestCase}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.junit.Assert
import org.mockito.ArgumentMatchers.any
import org.scalatest.BeforeAndAfterAll

import java.io.File

class BuildAsyncProfilerDriverPluginTest extends SparkFunSuite with BeforeAndAfterAll {

  @transient var sc: SparkContext = _
  protected val ut_meta = "../examples/test_case_data/localmeta"
  lazy val metaStore: NLocalFileMetadataTestCase = new NLocalFileMetadataTestCase

  val sparkPluginName: String = classOf[BuildAsyncProfilerSparkPlugin].getName
  val flagFileDir: String = System.getProperty("java.io.tmpdir") + "default/jobStepId/"
  val actionFilePath: String = flagFileDir + "/action"
  val statusFileName: String = flagFileDir + "/status"
  val dumpFileName: String = flagFileDir + "/dump.tar.gz"

  protected def metadata: Seq[String] = {
    Seq(fitPathForUT(ut_meta))
  }

  private def fitPathForUT(path: String): String = {
    if (new File(path).exists()) {
      path
    } else {
      s"../$path"
    }
  }

  override def beforeAll(): Unit = {
    metaStore.createTestMetadata(metadata: _*)
    super.beforeAll()
  }

  protected def overwriteSystemProp(key: String, value: String): Unit = {
    metaStore.overwriteSystemProp(key, value)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    metaStore.restoreSystemProps()
    metaStore.cleanupTestMetadata()
  }

  test("plugin initialization missing flagFileDir") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)

    sc = new SparkContext(conf)
    Assert.assertEquals(sparkPluginName, sc.getConf.get("spark.plugins"))

    sc.stop()
    sc = null
  }

  test("plugin initialization") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)
    System.setProperty("spark.profiler.flagsDir", flagFileDir)

    sc = new SparkContext(conf)
    Assert.assertEquals(sparkPluginName, sc.getConf.get("spark.plugins"))

    sc.stop()
    sc = null
  }

  test("plugin checkAction Null") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)
    System.setProperty("spark.profiler.flagsDir", flagFileDir)

    sc = new SparkContext(conf)

    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.actionFilePath = new Path(actionFilePath)
    driverPlugin.init(sc, any())
    HadoopUtil.writeStringToHdfs("null", driverPlugin.actionFilePath)
    driverPlugin.checkAction()

    sc.stop()
    sc = null
  }

  test("plugin checkAction NOP") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)
    System.setProperty("spark.profiler.flagsDir", flagFileDir)

    sc = new SparkContext(conf)

    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.actionFilePath = new Path(actionFilePath)
    driverPlugin.init(sc, any())
    driverPlugin.checkAction()

    sc.stop()
    sc = null
  }

  test("plugin checkAction STA") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)
    System.setProperty("spark.profiler.flagsDir", flagFileDir)

    sc = new SparkContext(conf)

    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.actionFilePath = new Path(actionFilePath)
    driverPlugin.statusFileName = new Path(statusFileName)
    driverPlugin.dumpFileName = new Path(dumpFileName)
    driverPlugin.init(sc, any())
    val driverMessage = Message.createDriverMessage(Message.START, AsyncProfilerToolTest.START_PARAMS)
    HadoopUtil.writeStringToHdfs(driverMessage, driverPlugin.actionFilePath)
    driverPlugin.checkAction()
    HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration, driverPlugin.actionFilePath)
    sc.stop()
    sc = null
  }

  test("plugin checkAction STA error") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)
    System.setProperty("spark.profiler.flagsDir", flagFileDir)

    sc = new SparkContext(conf)

    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.actionFilePath = new Path(actionFilePath)
    driverPlugin.statusFileName = new Path(statusFileName)
    driverPlugin.dumpFileName = new Path(dumpFileName)
    driverPlugin.init(sc, any())
    driverPlugin.start("")
    val driverMessage = Message.createDriverMessage(Message.START, AsyncProfilerToolTest.START_PARAMS)
    HadoopUtil.writeStringToHdfs(driverMessage, driverPlugin.actionFilePath)
    driverPlugin.checkAction()
    HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration, driverPlugin.actionFilePath)
    sc.stop()
    sc = null
  }

  test("plugin checkAction STA twice") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)
    System.setProperty("spark.profiler.flagsDir", flagFileDir)

    sc = new SparkContext(conf)

    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.actionFilePath = new Path(actionFilePath)
    driverPlugin.statusFileName = new Path(statusFileName)
    driverPlugin.dumpFileName = new Path(dumpFileName)
    driverPlugin.init(sc, any())
    val driverMessage = Message.createDriverMessage(Message.START, AsyncProfilerToolTest.START_PARAMS)
    HadoopUtil.writeStringToHdfs(driverMessage, driverPlugin.actionFilePath)
    driverPlugin.start("")
    driverPlugin.checkAction()
    HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration, driverPlugin.actionFilePath)
    sc.stop()
    sc = null
  }

  test("plugin checkAction STOP") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)
    System.setProperty("spark.profiler.flagsDir", flagFileDir)

    sc = new SparkContext(conf)

    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.actionFilePath = new Path(actionFilePath)
    driverPlugin.statusFileName = new Path(statusFileName)
    driverPlugin.dumpFileName = new Path(dumpFileName)
    driverPlugin.init(sc, any())
    driverPlugin.start("")
    val driverMessage = Message.createDriverMessage(Message.STOP, AsyncProfilerToolTest.START_PARAMS)
    HadoopUtil.writeStringToHdfs(driverMessage, driverPlugin.actionFilePath)
    driverPlugin.checkAction()
    HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration, driverPlugin.actionFilePath)
    sc.stop()
    sc = null
  }

  test("plugin checkAction STOP when not running") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)
    System.setProperty("spark.profiler.flagsDir", flagFileDir)

    sc = new SparkContext(conf)

    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.actionFilePath = new Path(actionFilePath)
    driverPlugin.statusFileName = new Path(statusFileName)
    driverPlugin.dumpFileName = new Path(dumpFileName)
    driverPlugin.init(sc, any())
    val driverMessage = Message.createDriverMessage(Message.STOP, AsyncProfilerToolTest.START_PARAMS)
    HadoopUtil.writeStringToHdfs(driverMessage, driverPlugin.actionFilePath)
    driverPlugin.checkAction()
    HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration, driverPlugin.actionFilePath)
    sc.stop()
    sc = null
  }

  test("plugin checkAction DUMP") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)
    System.setProperty("spark.profiler.flagsDir", flagFileDir)

    sc = new SparkContext(conf)

    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.actionFilePath = new Path(actionFilePath)
    driverPlugin.statusFileName = new Path(statusFileName)
    driverPlugin.dumpFileName = new Path(dumpFileName)
    driverPlugin.init(sc, any())
    val driverMessage = Message.createDriverMessage(Message.DUMP, AsyncProfilerToolTest.START_PARAMS)
    HadoopUtil.writeStringToHdfs(driverMessage, driverPlugin.actionFilePath)
    driverPlugin.checkAction()
    HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration, driverPlugin.actionFilePath)
    sc.stop()
    sc = null
  }

  test("plugin checkAction DUMP error") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)
    System.setProperty("spark.profiler.flagsDir", flagFileDir)

    sc = new SparkContext(conf)

    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.actionFilePath = new Path(actionFilePath)
    driverPlugin.statusFileName = new Path(statusFileName)
    driverPlugin.dumpFileName = new Path(dumpFileName)
    driverPlugin.init(sc, any())
    val driverMessage = Message.createDriverMessage(Message.DUMP, AsyncProfilerToolTest.START_PARAMS)
    HadoopUtil.writeStringToHdfs(driverMessage, driverPlugin.actionFilePath)
    driverPlugin.dump("")
    driverPlugin.checkAction()
    HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration, driverPlugin.actionFilePath)
    sc.stop()
    sc = null
  }

  test("plugin checkAction DUMP twice") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)
    System.setProperty("spark.profiler.flagsDir", flagFileDir)

    sc = new SparkContext(conf)

    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.actionFilePath = new Path(actionFilePath)
    driverPlugin.statusFileName = new Path(statusFileName)
    driverPlugin.dumpFileName = new Path(dumpFileName)
    driverPlugin.init(sc, any())
    val driverMessage = Message.createDriverMessage(Message.DUMP, AsyncProfilerToolTest.START_PARAMS)
    HadoopUtil.writeStringToHdfs(driverMessage, driverPlugin.actionFilePath)
    driverPlugin.dump("")
    driverPlugin.dump("")
    HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration, driverPlugin.actionFilePath)
    sc.stop()
    sc = null
  }

  test("plugin checkAction DUMP when running") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)
    System.setProperty("spark.profiler.flagsDir", flagFileDir)

    sc = new SparkContext(conf)

    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.actionFilePath = new Path(actionFilePath)
    driverPlugin.statusFileName = new Path(statusFileName)
    driverPlugin.dumpFileName = new Path(dumpFileName)
    driverPlugin.init(sc, any())
    driverPlugin.start("")
    driverPlugin.dump("")
    HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration, driverPlugin.actionFilePath)
    sc.stop()
    sc = null
  }

  test("plugin receive next") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)

    sc = new SparkContext(conf)

    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.receive("NEX-1:start,event=cpu")

    sc.stop()
    sc = null
  }

  test("plugin receive result") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)

    sc = new SparkContext(conf)

    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.receive("RES-1:flamegraph")

    sc.stop()
    sc = null
  }

  test("plugin receive others") {
    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.receive("OTH-1:start,event=cpu")
  }

  test("plugin start") {
    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.actionFilePath = new Path(actionFilePath)
    driverPlugin.statusFileName = new Path(statusFileName)
    driverPlugin.dumpFileName = new Path(dumpFileName)
    driverPlugin.start("")
  }

  test("plugin start timeout") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)
    System.setProperty("spark.profiler.flagsDir", flagFileDir)

    sc = new SparkContext(conf)

    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.init(sc, any())
    driverPlugin.actionFilePath = new Path(actionFilePath)
    driverPlugin.statusFileName = new Path(statusFileName)
    driverPlugin.dumpFileName = new Path(dumpFileName)
    driverPlugin.start("")
    driverPlugin.dump("")

    sc.stop()
    sc = null
  }

  test("plugin dump") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)
    System.setProperty("spark.profiler.flagsDir", flagFileDir)

    sc = new SparkContext(conf)

    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.init(sc, any())
    driverPlugin.actionFilePath = new Path(actionFilePath)
    driverPlugin.statusFileName = new Path(statusFileName)
    driverPlugin.dumpFileName = new Path(dumpFileName)
    driverPlugin.dump("")

    sc.stop()
    sc = null
  }

  test("plugin dump with null") {
    val conf = new SparkConf()
      .setAppName(getClass.getName)
      .set(SparkLauncher.SPARK_MASTER, "local[1]")
      .set("spark.plugins", sparkPluginName)

    sc = new SparkContext(conf)

    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.init(sc, any())
    driverPlugin.statusFileName = new Path(statusFileName)
    driverPlugin.actionFilePath = null
    driverPlugin.dumpFileName = null
    Assert.assertThrows(classOf[NullPointerException], () => driverPlugin.dump(""))

    sc.stop()
    sc = null
  }

  test("plugin initDirectory") {
    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.actionFilePath = new Path(actionFilePath)
    driverPlugin.statusFileName = new Path(statusFileName)
    driverPlugin.initDirectory(flagFileDir)
  }

  test("plugin shutdown") {
    val driverPlugin = new BuildAsyncProfilerDriverPlugin()
    driverPlugin.statusFileName = new Path(statusFileName)
    driverPlugin.shutdown()
  }
}