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
package org.apache.kylin.streaming

import java.sql.{Date, Timestamp}
import java.util.Locale

import com.google.common.base.Preconditions
import com.google.gson.JsonParser
import org.apache.commons.lang.time.DateUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.util.DateFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

class PartitionRowIterator(iter: Iterator[Row], parsedSchema: StructType, partitionColumn: String) extends Iterator[Row] {
  val logger = LoggerFactory.getLogger(classOf[PartitionRowIterator])

  val EMPTY_ROW = Row()

  val DATE_PATTERN = Array[String](DateFormat.COMPACT_DATE_PATTERN,
    DateFormat.DEFAULT_DATE_PATTERN,
    DateFormat.DEFAULT_DATE_PATTERN_WITH_SLASH,
    DateFormat.DEFAULT_DATETIME_PATTERN_WITH_TIMEZONE,
    DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS,
    DateFormat.DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS)

  def hasNext: Boolean = {
    iter.hasNext
  }

  def next: Row = {
    val csvString = iter.next.get(0)
    if (csvString == null || StringUtils.isEmpty(csvString.toString)) {
      return EMPTY_ROW
    }
    try {
      convertJson2Row(csvString.toString())
    } catch {
      case e: Exception =>
        logger.error(s"parse json text fail ${e.toString}  stackTrace is: " +
          s"${e.getStackTrace.toString} line is: ${csvString}")
        EMPTY_ROW
    }
  }

  def convertJson2Row(jsonStr: String): Row = {
    val jsonObj = JsonParser.parseString(jsonStr).getAsJsonObject
    val jsonMap = new mutable.HashMap[String, String]()
    val entries = jsonObj.entrySet().asScala
    entries.foreach { entry =>
      jsonMap.put(entry.getKey.toLowerCase(Locale.ROOT), entry.getValue.getAsString)
    }
    Row((0 to parsedSchema.fields.length - 1).map { index =>
      val colName = parsedSchema.fields(index).name.toLowerCase(Locale.ROOT)
      parseValue(jsonMap, colName, index)
    }: _*)
  }

  def parseValue(jsonMap: mutable.HashMap[String, String], colName: String, index: Int): Any = {
    if (!jsonMap.contains(colName)) { // key not exist
      return null
    }
    val value = jsonMap.get(colName).getOrElse(null) // value not exist
    val dataType = parsedSchema.fields(index).dataType
    if (StringUtils.isEmpty(value) && dataType != StringType) {
      return null
    }
    dataType match {
      case ShortType => value.toShort
      case IntegerType => value.toInt
      case LongType => value.toLong
      case DoubleType => value.toDouble
      case FloatType => value.toFloat
      case BooleanType => value.toBoolean
      case TimestampType => processTimestamp(colName, value)
      case DateType => new Date(DateUtils.parseDate(value, DATE_PATTERN).getTime)
      case DecimalType() => BigDecimal(value)
      case _ => value
    }
  }

  def processTimestamp(colName: String, value: String): Timestamp = {
    val timestamp = DateUtils.parseDate(value, DATE_PATTERN).getTime
    if (colName.equalsIgnoreCase(partitionColumn)) {
      Preconditions.checkArgument(timestamp >= 0, "invalid value %s", value)
    }
    new Timestamp(timestamp)
  }
}
