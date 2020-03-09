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
import org.apache.kylin.engine.spark.common.util.KapDateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils

object TimestampDiffImpl {

  // TimestampType -> DateType
  def evaluate(unit: String, timestamp: Long, date: Int): Long = {
    val before = timestamp / MICROS_PER_MILLIS
    val after = DateTimeUtils.daysToMillis(date)
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
    val before = DateTimeUtils.daysToMillis(date1)
    val after = DateTimeUtils.daysToMillis(date2)
    convertDuration(unit, before, after)
  }

  // DateType -> TimestampType
  def evaluate(unit: String, date: Int, timestamp: Long): Long = {
    val before = DateTimeUtils.daysToMillis(date)
    val after = timestamp / MICROS_PER_MILLIS
    convertDuration(unit, before, after)
  }

  private def convertDuration(unit: String, bMillis: Long, aMillis: Long): Long = {
    unit.toUpperCase match {
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
