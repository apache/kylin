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

package org.apache.spark.utils

import org.apache.spark.internal.Logging
import scala.reflect.ClassTag


object LogUtils {
  def jsonArray[T, U: ClassTag](seq: Seq[T])(f: T => U): String = {
    implicitly[ClassTag[U]].runtimeClass match {
      case _: Class[String] =>
        seq.map(f).map("\"" + _ + "\"").mkString("[", ",", "]")
      case _ =>
        seq.map(f).mkString("[", ",", "]")
    }
  }
}

trait LogEx extends Logging {

  protected def logTime[U](action: String, info: Boolean = false)(body: => U): U = {
    val start = System.currentTimeMillis()
    val result = body
    val end = System.currentTimeMillis()

    // If action is quite fast, don't logging
    if (end - start > 2) {
      if (info) {
        logDebug(s"Run $action take ${end - start} ms")
      } else {
        logTrace(s"Run $action take ${end - start} ms")
      }
    }
    result
  }

  protected def logInfoIf(filter: => Boolean)(msg: => String): Unit = {
    if (filter) logInfo(msg)
  }

  protected def logWarningIf(filter: => Boolean)(msg: => String): Unit = {
    if (filter) logWarning(msg)
  }
}