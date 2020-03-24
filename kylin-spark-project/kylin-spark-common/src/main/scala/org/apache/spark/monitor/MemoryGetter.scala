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

package org.apache.spark.memory

import java.lang.management._

trait MemoryGetter {
  def names: Seq[String]

  def values(dest: Array[Long], offset: Int): Unit
}

class MemoryMxBeanGetter(bean: MemoryMXBean) extends MemoryGetter {
  val names: Seq[String] = for {
    source <- Seq("heap", "offheap")
    usage <- Seq("used", "committed")
  } yield {
    source + ":" + usage
  }

  def values(dest: Array[Long], offset: Int): Unit = {
    val heap = bean.getHeapMemoryUsage
    dest(offset) = heap.getUsed
    dest(offset + 1) = heap.getCommitted
    val offheap = bean.getNonHeapMemoryUsage
    dest(offset + 2) = offheap.getUsed
    dest(offset + 3) = offheap.getCommitted
  }
}

class PoolGetter(bean: MemoryPoolMXBean) extends MemoryGetter {
  val names: Seq[String] =
    Seq("used", "committed").map {
      case n =>
        bean.getName + ":" + n
    }

  def values(dest: Array[Long], offset: Int): Unit = {
    val usage = bean.getUsage
    dest(offset) = usage.getUsed
    dest(offset + 1) = usage.getCommitted
  }
}

class GcInfoGetter(bean: GarbageCollectorMXBean) extends MemoryGetter {
  val names: Seq[String] =
    Seq("time", "count").map {
      case n =>
        bean.getName + ":" + n
    }

  def values(dest: Array[Long], offset: Int): Unit = {
    dest(offset) = bean.getCollectionTime
    dest(offset + 1) = bean.getCollectionCount
  }
}

class BufferPoolGetter(bean: BufferPoolMXBean) extends MemoryGetter {
  val names: Seq[String] =
    Seq("capacity", "used").map {
      case n =>
        bean.getName + ":" + n
    }

  def values(dest: Array[Long], offset: Int): Unit = {
    dest(offset) = bean.getTotalCapacity
    dest(offset + 1) = bean.getMemoryUsed
  }
}

class SystemInfoGetter(bean: OperatingSystemMXBean) extends MemoryGetter {
  val names: Seq[String] =
    Seq("processs", "System Load Average").map {
      case n =>
        bean.getName + ":" + n
    }

  def values(dest: Array[Long], offset: Int): Unit = {
    dest(offset) = bean.getAvailableProcessors
    dest(offset + 1) = bean.getSystemLoadAverage.toLong
  }

}
