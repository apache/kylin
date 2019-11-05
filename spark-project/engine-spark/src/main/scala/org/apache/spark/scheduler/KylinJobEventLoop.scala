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

package org.apache.spark.scheduler

import java.util
import java.util.{List => JList}

import io.kyligence.kap.engine.spark.scheduler.{KylinJobEvent, KylinJobListener}
import org.apache.spark.util.EventLoop

import scala.collection.JavaConverters._

class KylinJobEventLoop extends EventLoop[KylinJobEvent]("spark-entry-event-loop") {
  // register listener in single thread, so LinkedList is enough
  private var listeners: JList[KylinJobListener] = new util.LinkedList[KylinJobListener]()

  def registerListener(listener: KylinJobListener): Boolean = {
    listeners.add(listener)
  }

  def unregisterListener(listener: KylinJobListener): Boolean = {
    listeners.remove(listener)
  }

  override protected def onReceive(event: KylinJobEvent): Unit = {
    listeners.asScala.foreach(_.onReceive(event))
  }

  override protected def onError(e: Throwable): Unit = {

  }
}
