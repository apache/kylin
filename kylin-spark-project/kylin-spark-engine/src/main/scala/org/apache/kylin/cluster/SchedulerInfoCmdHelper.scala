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

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.Charset

import org.apache.kylin.engine.spark.utils.BuildUtils
import io.netty.util.internal.ThrowableUtil
import org.apache.hadoop.yarn.conf.{HAUtil, YarnConfiguration}
import org.apache.kylin.common.util.JsonUtil
import org.apache.kylin.job.exception.ShellException
import org.apache.spark.internal.Logging

object SchedulerInfoCmdHelper extends Logging {
  private val useHttps: Boolean = YarnConfiguration.useHttps(BuildUtils.getCurrentYarnConfiguration)

  def schedulerInfo: String = {
    val cmds = getSocketAddress.map(address => genCmd(address._1, address._2))
    val results = cmds.map { cmd =>
      try {
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
    val conf = BuildUtils.getCurrentYarnConfiguration
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
      val result = new StringBuilder
      val inReader = new BufferedReader(new InputStreamReader(proc.getInputStream, Charset.defaultCharset()))
      var line: String = inReader.readLine()
      while (line != null) {
        result.append(line).append('\n')
        logInfo(s"stdout\t$line")
        line = inReader.readLine()
      }

      val errorReader = new BufferedReader(new InputStreamReader(proc.getErrorStream, Charset.defaultCharset()))
      line = errorReader.readLine()
      while (line != null) {
        logInfo(s"stderr\t$line")
        line = errorReader.readLine()
      }

      try {
        logInfo(s"Thread wait for executing command $command")
        val exitCode = proc.waitFor
        (exitCode, result.toString)
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
