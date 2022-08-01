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
package org.apache.kylin.common

import org.apache.spark.utils.KafkaTestUtils

import java.util.{Calendar, Date, Locale, TimeZone}
import scala.io.Source

trait StreamingDataGenerate extends KafkaTestUtils {

  var start = System.currentTimeMillis()
  var totalMsgCount = 0

  def generate(path: String, topic: String): Unit = {
    sendOneRound(path, topic, 10)
  }

  def generate(path: String, topic: String, qps: Int, loop: Boolean): Unit = {
    if (loop) {
      while (true) {
        sendOneRound(path, topic, qps)
      }
    } else {
      sendOneRound(path, topic, qps)
    }
  }

  def generate(path: String, topic: String, count: Int): Unit = {
    val lines = Source.fromFile(path).getLines()
//    val lines = Source.fromFile("/Users/binbin.zheng/select.txt").getLines().mkString

    var cnt = 0;
    val iter = lines.toIterator
    while (iter.hasNext && cnt < count) {
      val line = iter.next()
      val currentTimeStamp = System.currentTimeMillis()
      val cal = Calendar.getInstance(TimeZone.getDefault, Locale.getDefault(Locale.Category.FORMAT))
      cal.setTime(new Date(currentTimeStamp))
      cal.set(Calendar.SECOND, 0)
      cal.set(Calendar.MILLISECOND, 0)
      val msg = cal.getTime.getTime + "," + line
      totalMsgCount += 1
      sendMessages(topic, Map(msg -> 1))
      cnt = cnt + 1
    }
    flush()

  }

  def sendOneRound(path: String, topic: String, qps: Int): Unit = {
    val content = Source.fromFile(path).getLines().foreach { line =>
      val currentTimeStamp = System.currentTimeMillis()
      val cal = Calendar.getInstance(TimeZone.getDefault, Locale.getDefault(Locale.Category.FORMAT))
      cal.setTime(new Date(currentTimeStamp))
      cal.set(Calendar.SECOND, 0)
      cal.set(Calendar.MILLISECOND, 0)
      val msg = cal.getTime.getTime + "," + line
      totalMsgCount += 1
      sendMessages(topic, Map(msg -> 1))
      if (totalMsgCount % qps == 0) {
        val interval = System.currentTimeMillis() - start
        logInfo(s"send ${StreamingTestConstant.KAFKA_QPS} massege cost ${interval}")
        logInfo(s"total send msg count is ${totalMsgCount}")
        if (interval < 1000) {
          Thread.sleep(1000 - interval)
        }
        start = System.currentTimeMillis()
      }
    }
    flush()
  }
}
