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

package org.apache.spark.sql.udf

import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.KapDateTimeUtils
import org.apache.spark.sql.catalyst.util.KapDateTimeUtils._

import java.util.Locale

object TimestampDiffImpl {

  // TimestampType -> DateType
  def evaluate(unit: String, timestamp: Long, date: Int): Long = {
    val before = timestamp / MICROS_PER_MILLIS
    val after = KapDateTimeUtils.daysToMillis(date)
    convertDuration(unit, before, after)
  }

  // TimestampType -> TimestampType
  def evaluate(unit: String, timestamp1: Long, timestamp2: Long): Long = {
    val before = timestamp1 / MICROS_PER_MILLIS
    val after = timestamp2 / MICROS_PER_MILLIS
    convertDuration(unit, before, after)
  }

  // DateType -> DateType
  def evaluate(unit: String, date1: Int, date2: Int): Long = {
    val before = KapDateTimeUtils.daysToMillis(date1)
    val after = KapDateTimeUtils.daysToMillis(date2)
    convertDuration(unit, before, after)
  }

  // DateType -> TimestampType
  def evaluate(unit: String, date: Int, timestamp: Long): Long = {
    val before = KapDateTimeUtils.daysToMillis(date)
    val after = timestamp / MICROS_PER_MILLIS
    convertDuration(unit, before, after)
  }

  private def convertDuration(unit: String, bMillis: Long, aMillis: Long): Long = {
    unit.toUpperCase(Locale.ROOT) match {
      case "FRAC_SECOND" | "SQL_TSI_FRAC_SECOND" =>
        aMillis - bMillis
      case "SECOND" | "SQL_TSI_SECOND" =>
        (aMillis - bMillis) / MILLIS_PER_SECOND
      case "MINUTE" | "SQL_TSI_MINUTE" =>
        (aMillis - bMillis) / MILLIS_PER_MINUTE
      case "HOUR" | "SQL_TSI_HOUR" =>
        (aMillis - bMillis) / MILLIS_PER_HOUR
      case "DAY" | "SQL_TSI_DAY" =>
        (aMillis - bMillis) / MILLIS_PER_DAY
      case "WEEK" | "SQL_TSI_WEEK" =>
        (aMillis - bMillis) / MILLIS_PER_DAY / DAYS_PER_WEEK
      case "MONTH" | "SQL_TSI_MONTH" =>
        KapDateTimeUtils.subtractMonths(aMillis, bMillis)
      case "QUARTER" | "SQL_TSI_QUARTER" =>
        KapDateTimeUtils.subtractMonths(aMillis, bMillis) / MONTHS_PER_QUARTER
      case "YEAR" | "SQL_TSI_YEAR" =>
        KapDateTimeUtils.subtractMonths(aMillis, bMillis) / MONTHS_PER_QUARTER / QUARTERS_PER_YEAR
      case _ =>
        throw new IllegalArgumentException(s"Illegal unit: $unit," +
          s" only support [YEAR, SQL_TSI_YEAR, QUARTER, SQL_TSI_QUARTER, MONTH, SQL_TSI_MONTH, WEEK, SQL_TSI_WEEK, DAY, SQL_TSI_DAY," +
          s" HOUR, SQL_TSI_HOUR, MINUTE, SQL_TSI_MINUTE, SECOND, SQL_TSI_SECOND, FRAC_SECOND, SQL_TSI_FRAC_SECOND] for now.")
    }
  }
}
