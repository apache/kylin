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
