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
import java.text.SimpleDateFormat

import com.google.common.collect.Lists
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._
import scala.collection.mutable

class MemoryMonitor() extends Logging {
  val sparkMemManagerHandle: Option[SparkMemoryManagerHandle] = SparkMemoryManagerHandle.get()
  val memoryBean: MemoryMXBean = ManagementFactory.getMemoryMXBean
  val poolBeans: mutable.Buffer[MemoryPoolMXBean] = ManagementFactory.getMemoryPoolMXBeans.asScala
  val offHeapPoolBeans: mutable.Buffer[MemoryPoolMXBean] = poolBeans.filter { pool =>
    !pool.isCollectionUsageThresholdSupported && pool.isUsageThresholdSupported
  }
  val bufferPoolsBeans: mutable.Buffer[BufferPoolMXBean] =
    ManagementFactory.getPlatformMXBeans(classOf[BufferPoolMXBean]).asScala
  val gcBeans: mutable.Buffer[GarbageCollectorMXBean] = ManagementFactory.getGarbageCollectorMXBeans.asScala
  val systemInfoBeans: OperatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean

  val getters: Seq[MemoryGetter] =
    Seq(new MemoryMxBeanGetter(memoryBean)) ++
      offHeapPoolBeans.map(new PoolGetter(_)) ++
      bufferPoolsBeans.map(new BufferPoolGetter(_)) ++
      gcBeans.map(new GcInfoGetter(_)) ++
      Seq(new SystemInfoGetter(systemInfoBeans)) ++
      sparkMemManagerHandle.toSeq

  val nameInfos: Seq[String] = getters.flatMap(_.names)
  val nMetricsLength: Int = nameInfos.length

  private val getterAndOffset = {
    var offset = 0
    getters.map {
      g =>
        val thisOffset = offset
        offset += g.names.length
        (g, thisOffset)
    }
  }

  def collectSnapshot: MemorySnapshot = {
    val now = System.currentTimeMillis()
    val values = new Array[Long](nMetricsLength)
    getterAndOffset.foreach { case (g, offset) =>
      g.values(values, offset)
    }
    new MemorySnapshot(now, values)
  }

  def collectThreadInfo: ThreadInfoSnapshot = {
    val now = System.currentTimeMillis()
    val infos = MemoryMonitor.getThreadInfo
    val metricsThreadInfos = new mutable.ListBuffer[MetricsThreadInfo]
    infos.foreach {
      case info =>
        metricsThreadInfos.+=(MetricsThreadInfo.wrap(info))
    }
    new ThreadInfoSnapshot(now, metricsThreadInfos.toArray)
  }

  def formatSnapshot(mem: MemorySnapshot): java.util.List[String] = {
    val memList = Lists.newArrayList[String]()
    logInfo(s"Mem usage at ${MemoryMonitor.dateFormat.format(mem.time)}")
    logInfo("===============")
    (0 until nMetricsLength).foreach { idx =>
      val v = mem.values(idx)
      val formattedV = nameInfos(idx) + ":" + Utils.bytesToString(v) + "(" + v + ")"
      logInfo(formattedV)
      memList.add(formattedV)
    }
    memList
  }

  def formatThreadDump(threadInfoSnapshot: ThreadInfoSnapshot): java.util.List[String] = {
    val threadInfoList = Lists.newArrayList[String]()
    threadInfoSnapshot.values.foreach { t =>
      if (t == null) {
        logInfo("<null thread>")
      } else {
        val headInfo = t.threadId + " " + t.threadName + " " + t.threadState.name()
        threadInfoList.add(headInfo)
        logInfo(headInfo)
        t.stackTrace.foreach { elem => threadInfoList.add("    " + elem); logInfo("\t" + elem) }
      }
    }
    threadInfoList
  }
}

object MemoryMonitor extends Logging {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")

  private var monitor: MemoryMonitor = _

  def install(): MemoryMonitor = synchronized {
    if (monitor == null) {
      monitor = new MemoryMonitor()
    }
    monitor
  }

  def getThreadInfo: Array[ThreadInfo] = {
    ManagementFactory.getThreadMXBean.dumpAllThreads(false, false)
  }
}