/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.kyligence.kap.cluster

import java.io.{BufferedReader, InputStreamReader}

import io.kyligence.kap.engine.spark.utils.BuildUtils
import io.netty.util.internal.ThrowableUtil
import org.apache.hadoop.yarn.conf.{HAUtil, YarnConfiguration}
import org.apache.kylin.common.util.{JsonUtil, ShellException}
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
      val inReader = new BufferedReader(new InputStreamReader(proc.getInputStream))
      var line: String = inReader.readLine()
      while (line != null) {
        result.append(line).append('\n')
        logInfo(s"stdout\t$line")
        line = inReader.readLine()
      }

      val errorReader = new BufferedReader(new InputStreamReader(proc.getErrorStream))
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
