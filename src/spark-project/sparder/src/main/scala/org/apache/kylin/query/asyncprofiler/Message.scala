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

/** simple command in string
 *  messages are in form of {command}{executor-id}:{param}
 *  commands are limited to fixed 3 characters
 */
object Message {
  /** issue from driver */
  // profiling command
  val START = "STA"
  val DUMP = "DMP"
  val STOP = "STP"
  val NOP = "NOP"

  /** issue from executors */
  // ask for next command
  val NEXT_COMMAND = "NEX"
  // send back profiling result
  val RESULT = "RES"

  private val COMMAND_LEN = 3
  private val SEPARATOR = ":"

  def getCommand(msg: String): String = {
    msg.substring(0, COMMAND_LEN)
  }

  def getId(msg: String): String = {
    msg.substring(3, msg.indexOf(SEPARATOR))
  }

  def getParam(msg: String): String = {
    msg.substring(msg.indexOf(SEPARATOR) + 1)
  }

  def createDriverMessage(cmd: String, param: String = ""): String = {
    s"$cmd-1$SEPARATOR$param"
  }

  def createExecutorMessage(cmd: String, id: String, param: String = ""): String = {
    s"$cmd$id$SEPARATOR$param"
  }

  /**
    *
    * @param msg
    * @return (cmd, executor-id, param)
    */
  def processMessage(msg: String): (String, String, String) = {
    (getCommand(msg), getId(msg), getParam(msg))
  }
}

