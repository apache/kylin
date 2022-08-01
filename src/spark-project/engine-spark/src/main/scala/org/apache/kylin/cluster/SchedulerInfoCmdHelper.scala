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

package org.apache.kylin.cluster

import org.apache.kylin.engine.spark.utils.StorageUtils
import io.netty.util.internal.ThrowableUtil
import org.apache.hadoop.yarn.conf.{HAUtil, YarnConfiguration}
import org.apache.kylin.common.util.{JsonUtil, ShellException}
import org.apache.spark.internal.Logging

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets

object SchedulerInfoCmdHelper extends Logging {
  private val useHttps: Boolean = YarnConfiguration.useHttps(StorageUtils.getCurrentYarnConfiguration)

  def schedulerInfo: String = {
    val cmds = getSocketAddress.map(address => genCmd(address._1, address._2))
    getInfoByCmds(cmds)
  }

  def metricsInfo: String = {
    val cmds = getSocketAddress.map(address => genMetricsCmd(address._1, address._2))
    getInfoByCmds(cmds)
  }

  private[cluster] def getInfoByCmds(cmds: Iterable[String]): String = {
    val results = cmds.map { cmd =>
      try {
        logInfo(s"Executing the command: ${cmd}")
        execute(cmd)
      } catch {
        case throwable: Throwable => (1, ThrowableUtil.stackTraceToString(throwable))
      }
    }
    val tuples = results.filter(result => result._1 == 0 && JsonUtil.isJson(result._2))
    if (tuples.isEmpty) {
      val errors = tuples.map(_._2).mkString("\n")
      logWarning(s"Error occurred when get scheduler info from cmd $cmds")
      throw new RuntimeException(errors)
    } else {
      require(tuples.size == 1)
      tuples.head._2
    }
  }

  private[cluster] def getSocketAddress: Map[String, Int] = {
    val conf = StorageUtils.getCurrentYarnConfiguration
    val addresses = if (HAUtil.isHAEnabled(conf)) {
      val haIds = HAUtil.getRMHAIds(conf).toArray
      require(haIds.nonEmpty, "Ha ids is empty, please check your yarn-site.xml.")
      if (useHttps) {
        haIds.map(id => conf.getSocketAddr(s"${YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS}.$id",
          YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS, YarnConfiguration.DEFAULT_NM_WEBAPP_HTTPS_PORT))
      } else {
        haIds.map(id => conf.getSocketAddr(s"${YarnConfiguration.RM_WEBAPP_ADDRESS}.$id",
          YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS, YarnConfiguration.DEFAULT_NM_WEBAPP_PORT))
      }
    } else {
      if (useHttps) {
        Array(conf.getSocketAddr(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS,
          YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS, YarnConfiguration.DEFAULT_NM_WEBAPP_HTTPS_PORT))
      } else {
        Array(conf.getSocketAddr(YarnConfiguration.RM_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS, YarnConfiguration.DEFAULT_NM_WEBAPP_PORT))
      }
    }
    addresses.map(address => address.getHostName -> address.getPort).toMap
  }

  private[cluster] def genCmd(hostName: String, port: Int): String = {
    val uri = if (useHttps) {
      s"https://$hostName:$port/ws/v1/cluster/scheduler"
    } else {
      s"http://$hostName:$port/ws/v1/cluster/scheduler"
    }
    s"""curl -k --negotiate -u : "$uri""""
  }

  private[cluster] def genMetricsCmd(hostName: String, port: Int): String = {
    val uri = if (useHttps) {
      s"https://$hostName:$port/ws/v1/cluster/metrics"
    } else {
      s"http://$hostName:$port/ws/v1/cluster/metrics"
    }
    s"""curl -k --negotiate -u : "$uri""""
  }

  /**
   * only return std out after execute command
   *
   * @param command
   * @return
   */
  private[cluster] def execute(command: String): (Int, String) = {
    try {
      val cmd = new Array[String](3)
      val osName = System.getProperty("os.name")
      if (osName.startsWith("Windows")) {
        cmd(0) = "cmd.exe"
        cmd(1) = "/C"
      }
      else {
        cmd(0) = "/bin/bash"
        cmd(1) = "-c"
      }
      cmd(2) = command
      val builder = new ProcessBuilder(cmd: _*)
      builder.environment().putAll(System.getenv())
      val proc = builder.start
      val resultStdout = new StringBuilder
      val inReader = new BufferedReader(new InputStreamReader(proc.getInputStream, StandardCharsets.UTF_8.name()))
      val newLine = System.getProperty("line.separator")
      var line: String = inReader.readLine()
      while (line != null) {
        resultStdout.append(line).append(newLine)
        line = inReader.readLine()
      }

      val stderr = new StringBuilder
      val errorReader = new BufferedReader(new InputStreamReader(proc.getErrorStream, StandardCharsets.UTF_8.name()))
      line = errorReader.readLine()
      while (line != null) {
        stderr.append(line).append(newLine)
        line = errorReader.readLine()
      }

      logInfo(s"The corresponding http response for the above command: \n ${stderr}")
      try {
        val exitCode = proc.waitFor
        if (exitCode != 0) {
          logError(s"executing command $command; exit code: $exitCode")
          logError(s"==========================[stderr]===============================")
          logError(stderr.toString)
          logError(s"==========================[stderr]===============================")

          logError(s"==========================[stdout]===============================")
          logError(resultStdout.toString)
          logError(s"==========================[stdout]===============================")
        }
        (exitCode, resultStdout.toString)
      } catch {
        case e: InterruptedException =>
          Thread.currentThread.interrupt()
          throw e
      }
    } catch {
      case e: Exception => throw new ShellException(e)
    }
  }
}
