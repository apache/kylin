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
package org.apache.spark.sql.catalyst.expressions

import org.apache.kylin.engine.spark.common.util.KylinDateTimeUtils._
import org.apache.kylin.engine.spark.cross.CrossDateTimeUtils

import java.util.{Calendar, Locale, TimeZone}

object TimestampAddImpl {
  private val localCalendar = new ThreadLocal[Calendar] {
    override def initialValue(): Calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT)
  }

  private def calendar: Calendar = localCalendar.get()

  // add int on DateType
  def evaluate(unit: String, increment: Int, time: Int): Int = {
    calendar.clear()
    addTime("DAY", time, calendar)
    addTime(unit, increment, calendar)
    CrossDateTimeUtils.millisToDays(calendar.getTimeInMillis)
  }

  // add long on DateType
  def evaluate(unit: String, increment: Long, time: Int): Int = {
    if (increment > Int.MaxValue) throw new IllegalArgumentException(s"Increment($increment) is greater than Int.MaxValue")
    else evaluate(unit, increment.intValue(), time)
  }

  // add int on TimestampType (NanoSecond)
  def evaluate(unit: String, increment: Int, time: Long): Long = {
    calendar.clear()
    calendar.setTimeInMillis(time / MICROS_PER_MILLIS)
    addTime(unit, increment, calendar)
    calendar.getTimeInMillis * MICROS_PER_MILLIS
  }

  // add long on TimestampType (NanoSecond)
  def evaluate(unit: String, increment: Long, time: Long): Long = {
    if (increment > Int.MaxValue) throw new IllegalArgumentException(s"Increment($increment) is greater than Int.MaxValue")
    else evaluate(unit, increment.intValue(), time)
  }


  private def addTime(unit: String, increment: Int, cal: Calendar): Unit = {
    unit.toUpperCase(Locale.ROOT) match {
      case "FRAC_SECOND" | "SQL_TSI_FRAC_SECOND" =>
        cal.add(Calendar.MILLISECOND, increment)
      case "SECOND" | "SQL_TSI_SECOND" =>
        cal.add(Calendar.SECOND, increment)
      case "MINUTE" | "SQL_TSI_MINUTE" =>
        cal.add(Calendar.MINUTE, increment)
      case "HOUR" | "SQL_TSI_HOUR" =>
        cal.add(Calendar.HOUR, increment)
      case "DAY" | "SQL_TSI_DAY" =>
        cal.add(Calendar.DATE, increment)
      case "WEEK" | "SQL_TSI_WEEK" =>
        cal.add(Calendar.WEEK_OF_YEAR, increment)
      case "MONTH" | "SQL_TSI_MONTH" =>
        cal.add(Calendar.MONTH, increment)
      case "QUARTER" | "SQL_TSI_QUARTER" =>
        cal.add(Calendar.MONTH, increment * MONTHS_PER_QUARTER.intValue())
      case "YEAR" | "SQL_TSI_YEAR" =>
        cal.add(Calendar.YEAR, increment)
      case _ =>
        throw new IllegalArgumentException(s"Illegal unit: $unit," +
          s" only support [YEAR, SQL_TSI_YEAR, QUARTER, SQL_TSI_QUARTER, MONTH, SQL_TSI_MONTH, WEEK, SQL_TSI_WEEK, DAY, SQL_TSI_DAY," +
          s" HOUR, SQL_TSI_HOUR, MINUTE, SQL_TSI_MINUTE, SECOND, SQL_TSI_SECOND, FRAC_SECOND, SQL_TSI_FRAC_SECOND] for now.")
    }
  }
}
