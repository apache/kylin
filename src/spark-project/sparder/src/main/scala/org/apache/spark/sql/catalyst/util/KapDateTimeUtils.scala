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

package org.apache.spark.sql.catalyst.util

import java.sql.Date
import java.time.ZoneId
import java.util.{Calendar, Locale, TimeZone}

import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{getDayOfMonth, getMonth, getYear}

object KapDateTimeUtils {

  // we use Int and Long internally to represent [[DateType]] and [[TimestampType]]
  type SQLDate = Int
  type SQLTimestamp = Long

  final val MONTHS_PER_QUARTER: Long = 3L
  final val QUARTERS_PER_YEAR: Long = 4L

  def defaultTimeZone(): TimeZone = TimeZone.getDefault()

  def addMonths(timestamp: Long, m: Int): Long = {
    // spark ts unit is microsecond
    val ms = timestamp / 1000
    val day0 = millisToDaysLegacy(ms, TimeZone.getTimeZone(ZoneId.systemDefault()))
    val millis = ms - day0 * MILLIS_PER_DAY
    val x = dateAddMonths(day0, m)
    (x * MILLIS_PER_DAY + millis) * 1000
  }

  /** Finds the number of months between two dates, each represented as the
   * number of days since the epoch.
   */
  def dateSubtractMonths(date0: Int, date1: Int): Int = {
    if (date0 < date1) return -dateSubtractMonths(date1, date0)
    // Start with an estimate.
    // Since no month has more than 31 days, the estimate is <= the true value.
    var m = (date0 - date1) / 31

    while (true) {
      val date2 = dateAddMonths(date1, m)
      if (date2 >= date0) return m
      val date3 = dateAddMonths(date1, m + 1)
      if (date3 > date0) return m

      m += 1
    }

    // will never reach here
    -1
  }

  def subtractMonths(t0: Long, t1: Long): Int = {
    val millis0 = floorMod(t0, MILLIS_PER_DAY)
    val d0 = millisToDaysLegacy(t0, TimeZone.getTimeZone(ZoneId.systemDefault()))
    val millis1 = floorMod(t1, MILLIS_PER_DAY)
    val d1 = millisToDaysLegacy(t1, TimeZone.getTimeZone(ZoneId.systemDefault()))
    var x = dateSubtractMonths(d0, d1)
    val d2 = dateAddMonths(d1, x)
    if (x > 0 && d2 == d0 && millis0 < millis1) x -= 1
    if (x < 0 && d2 == d0 && millis0 > millis1) x += 1
    x
  }

  // TODO fixme spark3 millisToDaysLegacy
  // millisToDays() and fromJavaDate() are taken from Spark 2.4
  def millisToDaysLegacy(millisUtc: Long, timeZone: TimeZone): Int = {
    val millisLocal = millisUtc + timeZone.getOffset(millisUtc)
    Math.floor(millisLocal.toDouble / MILLIS_PER_DAY).toInt
  }

  def fromJavaDateLegacy(date: Date): Int = {
    millisToDaysLegacy(date.getTime, TimeZone.getTimeZone(ZoneId.systemDefault()))
  }

  // reverse of millisToDays
  def daysToMillis(days: SQLDate): Long = {
    daysToMillis(days, defaultTimeZone())
  }

  def daysToMillis(days: SQLDate, timeZone: TimeZone): Long = {
    val millisLocal = days.toLong * MILLIS_PER_DAY
    millisLocal - getOffsetFromLocalMillis(millisLocal, timeZone)
  }

  private[sql] def getOffsetFromLocalMillis(millisLocal: Long, tz: TimeZone): Long = {
    var guess = tz.getRawOffset
    // the actual offset should be calculated based on milliseconds in UTC
    val offset = tz.getOffset(millisLocal - guess)
    if (offset != guess) {
      guess = tz.getOffset(millisLocal - offset)
      if (guess != offset) {
        // fallback to do the reverse lookup using java.sql.Timestamp
        // this should only happen near the start or end of DST
        val days = Math.floor(millisLocal.toDouble / MILLIS_PER_DAY).toInt
        val year = getYear(days)
        val month = getMonth(days)
        val day = getDayOfMonth(days)

        var millisOfDay = (millisLocal % MILLIS_PER_DAY).toInt
        if (millisOfDay < 0) {
          millisOfDay += MILLIS_PER_DAY.toInt
        }
        val seconds = (millisOfDay / 1000L).toInt
        val hh = seconds / 3600
        val mm = seconds / 60 % 60
        val ss = seconds % 60
        val ms = millisOfDay % 1000
        val calendar = Calendar.getInstance(tz, Locale.getDefault(Locale.Category.FORMAT))
        calendar.set(year, month - 1, day, hh, mm, ss)
        calendar.set(Calendar.MILLISECOND, ms)
        guess = (millisLocal - calendar.getTimeInMillis()).toInt
      }
    }
    guess
  }

  def dayOfWeek(date: Int): Int = {
    (date + 4) % 7 + 1
  }

  /**
   * Add date and year-month interval.
   * Returns a date value, expressed in days since 1.1.1970.
   */
  def dateAddMonths(date: Int, m: Int): Int = {
    var y0 = org.apache.calcite.avatica.util.DateTimeUtils
      .unixDateExtract(TimeUnitRange.YEAR, date)
      .toInt
    var m0 = org.apache.calcite.avatica.util.DateTimeUtils
      .unixDateExtract(TimeUnitRange.MONTH, date)
      .toInt
    var d0 = org.apache.calcite.avatica.util.DateTimeUtils
      .unixDateExtract(TimeUnitRange.DAY, date)
      .toInt

    val endOfMonth = lastDay(y0, m0)
    val y = (m + m0) / 12
    y0 += y
    m0 = m + m0 - y * 12
    if (m0 <= 0) {
      m0 += 12
      assert(m0 > 0)
      y0 -= 1
    }
    val last = lastDay(y0, m0)
    if (d0 == endOfMonth || d0 > last) d0 = last

    org.apache.calcite.avatica.util.DateTimeUtils.ymdToUnixDate(y0, m0, d0)
  }

  def floorDiv(x: Long, y: Long): Long = {
    var r = x / y
    if ((x ^ y) < 0L && r * y != x) r -= 1
    r
  }

  def floorMod(x: Long, y: Long): Long = x - floorDiv(x, y) * y

  private def lastDay(y: Int, m: Int) = m match {
    case 2 =>
      if (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) 29
      else 28
    case 4 => 30
    case 6 => 30
    case 9 => 30
    case 11 => 30
    case _ => 31
  }
}
