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
package org.apache.spark.sql

import java.nio.ByteBuffer

import org.apache.kylin.measure.bitmap.RoaringBitmapCounter
import org.apache.kylin.measure.hllc.HLLCounter
import org.apache.spark.sql.catalyst.util.stackTraceToString
import org.apache.spark.sql.common.SparderQueryTest
import org.scalatest.Assertions

trait KapQueryUtils {
  this : Assertions =>

  protected def getBitmapArray(values: Long*): Array[Byte] = {
    val buffer = ByteBuffer.allocate(1024)
    val count = new RoaringBitmapCounter
    values.foreach(count.add)
    count.write(buffer)
    buffer.array()
  }

  protected def getHllcArray(values: Int*): Array[Byte] = {
    val buffer = ByteBuffer.allocate(1024 * 1024)
    val hllc = new HLLCounter(14)
    values.foreach(hllc.add)
    hllc.writeRegisters(buffer)
    buffer.array()
  }

  protected def checkAnswer(
      df: => DataFrame,
      expectedAnswer: Seq[Row]): Unit = {
    val analyzedDF = try df
    catch {
      case ae: AnalysisException =>
        if (ae.plan.isDefined) {
          fail(s"""
                  |Failed to analyze query: $ae
                  |${ae.plan.get}
                  |
                  |${stackTraceToString(ae)}
                  |""".stripMargin)
        } else {
          throw ae
        }
    }
    SparderQueryTest.checkAnswerBySeq(analyzedDF, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }
}
