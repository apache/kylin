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

package org.apache.kylin.engine.spark.cross

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{SQLDate, SQLTimestamp}
import org.apache.spark.unsafe.types.UTF8String

import java.time.ZoneId
import java.util.TimeZone

object CrossDateTimeUtils {
  def stringToTimestamp(value: Any): Option[SQLTimestamp] = {
    DateTimeUtils.stringToTimestamp(UTF8String.fromString(value.toString), TimeZone.getDefault)
  }

  def stringToTimestamp(value: Any, zoneId: ZoneId): Option[SQLTimestamp] = {
    DateTimeUtils.stringToTimestamp(UTF8String.fromString(value.toString), TimeZone.getTimeZone(zoneId))
  }

  def stringToDate(value: Any): Option[SQLDate] = {
    DateTimeUtils.stringToDate(UTF8String.fromString(value.toString))
  }

  def millisToDays(millis: Long): Int = {
    DateTimeUtils.millisToDays(millis)
  }

  def daysToMillis(days: Int): Long = {
    DateTimeUtils.daysToMillis(days)
  }

  def dateToString(): String = {
    DateTimeUtils.dateToString(DateTimeUtils.millisToDays(System.currentTimeMillis()))
  }
}
