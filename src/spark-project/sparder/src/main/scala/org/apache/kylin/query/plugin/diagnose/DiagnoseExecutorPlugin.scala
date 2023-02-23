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

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kylin.common.util.ExecutorServiceUtil
import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.internal.Logging
import org.joda.time.DateTime

import java.io.File
import java.util
import java.util.concurrent.{Executors, TimeUnit}

class DiagnoseExecutorPlugin extends ExecutorPlugin with Logging {

  private val SPARDER_LOG: String = "_sparder_logs"
  private val LOCAL_GC_FILE_PREFIX: String = "gc"
  private val DATE_PATTERN = "yyyy-MM-dd"
  private val checkingInterval: Long = 10000L
  private val configuration: Configuration = new Configuration()
  private val fileSystem: FileSystem = FileSystem.get(configuration)

  private val scheduledExecutorService = Executors.newScheduledThreadPool(1,
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Diagnose-%d").build())

  private var state = DiagnoseConstant.STATE_WAIT
  private var curContainerDir = new File(".")
  private var sparderLogDir: String = ""
  private var ctx: PluginContext = _

  override def init(_ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    ctx = _ctx
    val diagnose = new Runnable {
      override def run(): Unit = checkAndDiagnose()
    }
    logInfo("Diagnose executor plugin is initializing ...")
    scheduledExecutorService.scheduleWithFixedDelay(
      diagnose, 0, checkingInterval, TimeUnit.MILLISECONDS)
  }

  def checkAndDiagnose(): Unit = {
    try {
      val replay: AnyRef = ctx.ask(DiagnoseConstant.NEXTCMD)
      logDebug(s"Executor ${ctx.executorID()} get replay $replay from driver ...")
      replay match {
        case DiagnoseConstant.COLLECT =>
          if (DiagnoseConstant.STATE_WAIT.equals(state)) {
            state = DiagnoseConstant.STATE_COLLECT
            logDebug(s"Set executor state to $state")
            collectGcLog()
            ctx.send(DiagnoseConstant.SENDRESULT)
          }
        case DiagnoseConstant.NOP =>
          if (!DiagnoseConstant.STATE_WAIT.equals(state)) {
            state = DiagnoseConstant.STATE_WAIT
            logDebug(s"Set executor state to $state")
          }
        case _ => ""
      }
    } catch {
      case e: Exception =>
        logInfo("Error while communication/Diagnose", e)
    }
  }

  def collectGcLog(): Unit = {
    logDebug(s"Collectting sparder gc log file ...")
    if (sparderLogDir.isEmpty) {
      val reply = ctx.ask(DiagnoseConstant.HDFSDIR).toString
      if (reply.isEmpty) {
        logWarning(s"Can not get kylin working dir, will not collect sparder executor gc log.")
        return
      } else {
        sparderLogDir = reply + SPARDER_LOG
        logInfo(s"HDFS sparder log dir is setting to ${sparderLogDir}")
      }
    }
    val filePath = sparderLogDir + File.separator + new DateTime().toString(DATE_PATTERN) +
      File.separator + ctx.conf().getAppId + File.separator
    val fileNamePrefix = "executor-%s-".format(ctx.executorID())

    curContainerDir.listFiles().filter(file => file.getName.startsWith(LOCAL_GC_FILE_PREFIX))
      .map(file => copyLocalFileToHdfs(new Path(file.getAbsolutePath), new Path(filePath, fileNamePrefix + file.getName)))
  }

  def copyLocalFileToHdfs(local: Path, hdfs: Path): Unit = {
    logInfo(s"Local gc file path is: ${local}, target hdfs file is: ${hdfs}")
    fileSystem.copyFromLocalFile(local, hdfs)
  }

  override def shutdown(): Unit = {
    ExecutorServiceUtil.shutdownGracefully(scheduledExecutorService, 3)
    super.shutdown()
  }

  // for test only
  def setCtx(_ctx: PluginContext): Unit = {
    ctx = _ctx
  }

  // for test only
  def setContainerDir(_curContainerDir: File): Unit = {
    curContainerDir = _curContainerDir
  }
}
