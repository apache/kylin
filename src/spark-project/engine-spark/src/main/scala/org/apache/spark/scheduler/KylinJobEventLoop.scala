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

package org.apache.spark.scheduler

import java.util
import java.util.{List => JList}

import org.apache.kylin.engine.spark.scheduler.{KylinJobEvent, KylinJobListener}
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
