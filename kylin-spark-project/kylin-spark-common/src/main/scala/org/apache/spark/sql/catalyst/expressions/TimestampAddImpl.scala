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
package org.apache.spark.sql.catalyst.expressions

import org.apache.kylin.engine.spark.common.util.KapDateTimeUtils._
import java.util.{Calendar, Locale, TimeZone}

import org.apache.spark.sql.catalyst.util.DateTimeUtils

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
    DateTimeUtils.millisToDays(calendar.getTimeInMillis)
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
