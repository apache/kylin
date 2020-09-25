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
import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.kylin.shaded.com.google.common.collect.Lists
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

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.ROOT)

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