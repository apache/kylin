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

import org.apache.kylin.guava30.shaded.common.base.{Preconditions, Throwables}
import org.apache.commons.lang.time.DateUtils
import org.apache.commons.lang3.{ObjectUtils, StringUtils}
import org.apache.kylin.common.util.DateFormat
import org.apache.kylin.parser.AbstractDataParser
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.lang
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.util.{Locale, Objects}
import scala.collection.JavaConverters._
import scala.collection.mutable

class PartitionRowIterator(iter: Iterator[Row],
                           parsedSchema: StructType,
                           partitionColumn: String,
                           dateParser: AbstractDataParser[ByteBuffer]) extends Iterator[Row] {
  private val logger = LoggerFactory.getLogger(classOf[PartitionRowIterator])

  private val EMPTY_ROW = Row()

  private val DATE_PATTERN = Array[String](DateFormat.COMPACT_DATE_PATTERN,
    DateFormat.DEFAULT_DATE_PATTERN,
    DateFormat.DEFAULT_DATE_PATTERN_WITH_SLASH,
    DateFormat.DEFAULT_DATETIME_PATTERN_WITH_TIMEZONE,
    DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS,
    DateFormat.DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS)

  def hasNext: Boolean = {
    iter.hasNext
  }

  def next: Row = {
    val input = iter.next.get(0)
    if (Objects.isNull(input) || StringUtils.isEmpty(input.toString)) {
      logger.error(s"input data is null or length is 0, returning empty row. line is '$input'")
      return EMPTY_ROW
    }
    try {
      parseToRow(input.toString)
    } catch {
      case e: Exception =>
        logger.error(s"parse data failed, line is: '$input'", Throwables.getRootCause(e))
        EMPTY_ROW
    }
  }

  def parseToRow(input: String): Row = {
    val jsonMap: mutable.Map[String, AnyRef] = dateParser.process(StandardCharsets.UTF_8.encode(input)).asScala
      .map(pair => (pair._1.toLowerCase(Locale.ROOT), pair._2))

    Row(parsedSchema.fields.indices.map { index =>
      val colName = parsedSchema.fields(index).name.toLowerCase(Locale.ROOT)
      parseValue(jsonMap, colName, index)
    }: _*)
  }

  def parseValue(jsonMap: mutable.Map[String, AnyRef], colName: String, index: Int): Any = {
    if (!jsonMap.contains(colName)) { // key not exist
      return null
    }
    val value: AnyRef = jsonMap.getOrElse(colName, null) // value not exist
    val dataType = parsedSchema.fields(index).dataType
    if (dataType == StringType) {
      value
    } else if (ObjectUtils.isEmpty(value)) {
      // key not exist -> null
      // value not exist ("", null, new int[]{}) -> null
      null
    } else {
      val strValue = value.toString
      dataType match {
        case ShortType => lang.Short.parseShort(strValue)
        case IntegerType => Integer.parseInt(strValue)
        case LongType => lang.Long.parseLong(strValue)
        case DoubleType => lang.Double.parseDouble(strValue)
        case FloatType => lang.Float.parseFloat(strValue)
        case BooleanType => lang.Boolean.parseBoolean(strValue)
        case TimestampType => processTimestamp(colName, strValue)
        case DateType => new Date(DateUtils.parseDate(strValue, DATE_PATTERN).getTime)
        case DecimalType() => BigDecimal(strValue)
        case _ => value
      }
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
