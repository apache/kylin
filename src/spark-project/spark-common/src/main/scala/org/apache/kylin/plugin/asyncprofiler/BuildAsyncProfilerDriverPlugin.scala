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

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.kylin.common.asyncprofiler.Message._
import org.apache.kylin.common.asyncprofiler.{AsyncProfilerTool, AsyncProfilerUtils}
import org.apache.kylin.common.util.HadoopUtil
import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext}
import org.apache.spark.internal.Logging

import java.nio.file.Files
import java.util
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

class BuildAsyncProfilerDriverPlugin extends DriverPlugin with Logging {
  private val checkingInterval: Long = 1000
  private val localCacheDir = Files.createTempDirectory("ke-build-async-profiler-result-").toFile
  localCacheDir.deleteOnExit()
  private val resultCollectionTimeout = sys.props.get("spark.profiler.collection.timeout").getOrElse("60000").toLong
  private val profilingTimeout = sys.props.get("spark.profiler.profiling.timeout").getOrElse("300000").toLong
  private var timeoutExecutionThread: Thread = _

  private var nextCommand: String = createDriverMessage(NOP)
  private var running = false
  private var dumped = false
  private var cachedResult: CountDownLatch = _
  private var sparkContext: SparkContext = _
  private val FLAG_FILE_DIR = "spark.profiler.flagsDir"
  private val flagFileDir = sys.props.get(FLAG_FILE_DIR)
  var actionFilePath: Path = _
  var statusFileName: Path = _
  var dumpFileName: Path = _
  var working = true

  private val asyncProfilerUtils: AsyncProfilerUtils = AsyncProfilerUtils.getInstance()
  asyncProfilerUtils.build(resultCollectionTimeout, localCacheDir)

  private val scheduledExecutorService = Executors.newScheduledThreadPool(1,
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("profiler-action-check-%d").build())

  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    sparkContext = sc
    if (flagFileDir.isEmpty) {
      working = false
      log.error("Missing system parameter spark.profiler.flags.dir")
    } else {
      actionFilePath = new Path(flagFileDir.get + "/action")
      statusFileName = new Path(flagFileDir.get + "/status")
      dumpFileName = new Path(flagFileDir.get + "/dump.tar.gz")
      initDirectory(flagFileDir.get)

      val profile = new Runnable {
        override def run(): Unit = checkAction()
      }
      log.debug(s"AsyncProfiler status: ${AsyncProfilerTool.status()}")
      scheduledExecutorService.scheduleWithFixedDelay(
        profile, 0, checkingInterval, TimeUnit.MILLISECONDS)

    }

    super.init(sc, pluginContext)
  }

  def initDirectory(flagFileDir: String): Unit = {
    val fs: FileSystem = HadoopUtil.getFileSystem(flagFileDir)
    val flagFileDirPath = new Path(flagFileDir)
    if (!fs.exists(flagFileDirPath)) {
      fs.mkdirs(flagFileDirPath)
    }
    fs.listStatus(flagFileDirPath).map(file => fs.delete(file.getPath, true))

    HadoopUtil.writeStringToHdfs(fs, createDriverMessage(NOP), actionFilePath)
    HadoopUtil.writeStringToHdfs(fs, ProfilerStatus.IDLE, statusFileName)
  }

  def checkAction(): Unit = {
    try {
      val fs: FileSystem = HadoopUtil.getFileSystem(flagFileDir.get)
      val reply = HadoopUtil.readStringFromHdfs(fs, actionFilePath)
      val (command, _, param) = processMessage(reply)
      command match {
        case START if !running =>
          start(param)
        case STOP if running =>
          dump(param)
        case DUMP if !dumped => // doesn't allow a second dump for simplicity
          dump(param)
        case _ =>
      }
    } catch {
      case e: Exception =>
        logInfo("error while communication/profiling", e)
    }
  }


  override def receive(message: Any): AnyRef = {

    val (command, executorId, param) = processMessage(message.toString)
    command match {
      case NEXT_COMMAND =>
        nextCommand
      case RESULT =>
        asyncProfilerUtils.cacheExecutorResult(param, executorId)
        ""
      case _ => ""
    }
  }

  override def shutdown(): Unit = {
    val fs: FileSystem = HadoopUtil.getFileSystem(statusFileName)
    HadoopUtil.writeStringToHdfs(fs, ProfilerStatus.CLOSED, statusFileName)
  }

  def start(params: String): Unit = {
    val startParam = Option(params).getOrElse("start,event=cpu")
    AsyncProfilerTool.synchronized {
      logInfo("profiler start")
      running = true
      dumped = false
      nextCommand = createDriverMessage(START, startParam) // inform executors
      AsyncProfilerTool.start(startParam) // start driver prof
      logInfo("profiler started")

      // update status file
      val fs: FileSystem = HadoopUtil.getFileSystem(statusFileName)
      fs.delete(dumpFileName, true)
      HadoopUtil.writeStringToHdfs(fs, ProfilerStatus.RUNNING, statusFileName)

      timeoutExecutionThread = new Thread(() => {
        try {
          Thread.sleep(profilingTimeout)
        } catch {
          case _: InterruptedException =>
            logTrace("profiler stopped normally, timeout thread interrupted and exit")
            Thread.currentThread().interrupt()
        }

        if (!Thread.interrupted()) {
          logInfo(s"profiling timeout after ${profilingTimeout}ms, stopping profiling")
          AsyncProfilerTool.synchronized {
            if (running) {
              running = false
              nextCommand = createDriverMessage(STOP)
              AsyncProfilerTool.stop()
              HadoopUtil.writeStringToHdfs(fs, ProfilerStatus.STOPPED, statusFileName)
              // reset action avoid start auto
              HadoopUtil.writeStringToHdfs(fs, createDriverMessage(NOP), actionFilePath)
            }
          }
        }
      })
      timeoutExecutionThread.start()
    }
  }

  def dump(params: String): Unit = {
    val dumpParam = Option(params).getOrElse("flamegraph")
    AsyncProfilerTool.synchronized {
      if (dumped) {
        return
      }

      if (running) {
        logDebug("profiler stop")
        running = false
        timeoutExecutionThread.interrupt()
      } else {
        logWarning("profiling is not started")
      }

      dumped = true

      asyncProfilerUtils.cleanLocalCache()

      // expecting driver + count(executor) amount of results
      cachedResult = new CountDownLatch(

        sparkContext.getExecutorMemoryStatus.size
      )
      asyncProfilerUtils.build(cachedResult)
      logInfo(s"expecting ${cachedResult.getCount} to be collected")

      nextCommand = createDriverMessage(DUMP, dumpParam) // inform executors
      asyncProfilerUtils.cacheDriverResult(AsyncProfilerTool.dump(dumpParam)) // dump driver prof

      val fs: FileSystem = HadoopUtil.getFileSystem(statusFileName)
      var outStream: FSDataOutputStream = null
      try {
        outStream = fs.create(dumpFileName)
        asyncProfilerUtils.waitForResult(outStream)
      } finally {
        if (outStream != null) {
          outStream.close()
        }
      }
      HadoopUtil.writeStringToHdfs(fs, ProfilerStatus.DUMPED, statusFileName)
    }
  }
}