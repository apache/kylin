/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
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

package org.apache.kylin.engine.spark.utils

import org.apache.spark.internal.Logging
import scala.reflect.ClassTag


object LogUtils {
  def jsonArray[T, U: ClassTag](seq: Seq[T])(f: T => U ) : String = {
    implicitly[ClassTag[U]].runtimeClass match {
      case _ : Class[String] =>
        seq.map(f).map("\"" + _ + "\"").mkString("[", ",", "]")
      case _ =>
        seq.map(f).mkString("[", ",", "]")
    }
  }
  def jsonMap[A, B](map: Map[A, B]) : String = {
    map.map { kv =>
      s""""${kv._1}":"${kv._2}""""
    }.mkString("{", ",", "}")
  }
}

trait LogEx extends  Logging {

  protected def logTime[U](action: String, debug: Boolean = false)(body: => U): U = {
    val start = System.currentTimeMillis()
    val result = body
    val end = System.currentTimeMillis()

    // If action is quite fast, don't logging
    if  (end - start > 2 ) {
      if (debug) {
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
