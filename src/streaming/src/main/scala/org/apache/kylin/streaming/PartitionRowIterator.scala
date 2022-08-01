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

import com.google.gson.JsonParser
import org.apache.commons.lang.time.DateUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.util.DateFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.sql.{Date, Timestamp}
import java.util.Locale
import scala.collection.JavaConverters._
import scala.collection.mutable

class PartitionRowIterator(iter: Iterator[Row], parsedSchema: StructType) extends Iterator[Row] {
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
    val parser = new JsonParser()
    if (csvString == null || StringUtils.isEmpty(csvString.toString)) {
      EMPTY_ROW
    } else {
      try {
        convertJson2Row(csvString.toString(), parser)
      } catch {
        case e: Exception =>
          logger.error(s"parse json text fail ${e.toString}  stackTrace is: " +
            s"${e.getStackTrace.toString} line is: ${csvString}")
          EMPTY_ROW
      }
    }
  }

  def convertJson2Row(jsonStr: String, parser: JsonParser): Row = {
    val jsonMap = new mutable.HashMap[String, String]()
    val jsonObj = parser.parse(jsonStr).getAsJsonObject
    val entries = jsonObj.entrySet().asScala
    entries.foreach { entry =>
      jsonMap.put(entry.getKey.toLowerCase(Locale.ROOT), entry.getValue.getAsString)
    }
    Row((0 to parsedSchema.fields.length - 1).map { index =>
      val colName = parsedSchema.fields(index).name.toLowerCase(Locale.ROOT)
      if (!jsonMap.contains(colName)) { // key not exist
        null
      } else {
        val value = jsonMap.get(colName).getOrElse(null) // value not exist
        parsedSchema.fields(index).dataType match {
          case ShortType => if (value == null || value.equals("")) null else value.toShort
          case IntegerType => if (value == null || value.equals("")) null else value.toInt
          case LongType => if (value == null || value.equals("")) null else value.toLong
          case DoubleType => if (value == null || value.equals("")) null else value.toDouble
          case FloatType => if (value == null || value.equals("")) null else value.toFloat
          case BooleanType => if (value == null || value.equals("")) null else value.toBoolean
          case TimestampType => if (value == null || value.equals("")) null
          else new Timestamp(DateUtils.parseDate(value, DATE_PATTERN).getTime)
          case DateType => if (value == null || value.equals("")) null
          else new Date(DateUtils.parseDate(value, DATE_PATTERN).getTime)
          case DecimalType() => if (StringUtils.isEmpty(value)) null else BigDecimal(value)
          case _ => value
        }
      }
    }: _*)
  }

}
