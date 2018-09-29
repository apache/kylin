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

package org.apache.spark.sql.util

import java.math.BigDecimal
import java.sql.Timestamp
import java.util.Locale

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.kylin.common.util.DateFormat
import org.apache.kylin.metadata.datatype.DataType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object SparderTypeUtil extends Logging {
  val DATETIME_FAMILY = List("time", "date", "timestamp", "datetime")

  def isDateTimeFamilyType(dataType: String): Boolean = {
    DATETIME_FAMILY.contains(dataType.toLowerCase(Locale.ROOT))
  }

  def isDateType(dataType: String): Boolean = {
    "date".equalsIgnoreCase(dataType)
  }

  def isDateTime(sqlTypeName: SqlTypeName): Boolean = {
    SqlTypeName.DATETIME_TYPES.contains(sqlTypeName)
  }

  // scalastyle:off
  def kylinTypeToSparkResultType(dataTp: DataType): org.apache.spark.sql.types.DataType = {
    dataTp.getName match {
      case tp if tp.startsWith("hllc") => BinaryType
      case tp if tp.startsWith("top") => BinaryType
      case tp if tp.startsWith("percentile") => BinaryType
      case tp if tp.startsWith("bitmap") => BinaryType
      case "decimal" => DecimalType(dataTp.getPrecision, dataTp.getScale)
      case "date" => IntegerType
      case "time" => LongType
      case "timestamp" => LongType
      case "datetime" => LongType
      case "tinyint" => ByteType
      case "smallint" => ShortType
      case "integer" => IntegerType
      case "int4" => IntegerType
      case "bigint" => LongType
      case "long8" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case "real" => DoubleType
      case tp if tp.startsWith("varchar") => StringType
      case tp if tp.startsWith("char") => StringType
      case "bitmap" => LongType
      case "dim_dc" => BinaryType
      case "boolean" => BooleanType
      case "extendedcolumn" => BinaryType
      case "raw" => BinaryType
      case noSupport => throw new IllegalArgumentException(s"No supported data type: $noSupport")
    }
  }

  // scalastyle:off
  def kylinSQLTypeToSparkType(dataTp: DataType): org.apache.spark.sql.types.DataType = {
    dataTp.getName match {
      case "decimal" => DecimalType(dataTp.getPrecision, dataTp.getScale)
      case "date" => DateType
      case "time" => DateType
      case "timestamp" => TimestampType
      case "datetime" => DateType
      case "tinyint" => ByteType
      case "smallint" => ShortType
      case "integer" => IntegerType
      case "int4" => IntegerType
      case "bigint" => LongType
      case "long8" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case "real" => DoubleType
      case tp if tp.startsWith("varchar") => StringType
      case tp if tp.startsWith("char") => StringType
      case "bitmap" => LongType
      case "dim_dc" => LongType
      case "boolean" => BooleanType
      case noSupport => throw new IllegalArgumentException(s"No supported data type: $noSupport")
    }
  }

  // scalastyle:off
  def convertSqlTypeNameToSparkType(sqlTypeName: SqlTypeName): String = {
    sqlTypeName match {
      case SqlTypeName.DECIMAL => "decimal"
      case SqlTypeName.CHAR => "string"
      case SqlTypeName.VARCHAR => "string"
      case SqlTypeName.INTEGER => "int"
      case SqlTypeName.TINYINT => "byte"
      case SqlTypeName.SMALLINT => "short"
      case SqlTypeName.BIGINT => "long"
      case SqlTypeName.FLOAT => "float"
      case SqlTypeName.DOUBLE => "double"
      case SqlTypeName.DATE => "date"
      case SqlTypeName.TIMESTAMP => "timestamp"
      case SqlTypeName.BOOLEAN => "boolean"
      case noSupport => throw new IllegalArgumentException(s"No supported data type: $noSupport")
    }
  }

  // scalastyle:off
  def kylinCubeDataTypeToSparkType(dataTp: DataType): org.apache.spark.sql.types.DataType = {
    dataTp.getName match {
      case "decimal" => DecimalType(dataTp.getPrecision, dataTp.getScale)
      case "date" => DateType
      case "time" => DateType
      case "timestamp" => TimestampType
      case "datetime" => DateType
      case "tinyint" => ByteType
      case "smallint" => ShortType
      case "integer" => IntegerType
      case "int4" => IntegerType
      case "bigint" => LongType
      case "long8" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case "real" => DoubleType
      case tp if tp.startsWith("varchar") => StringType
      case tp if tp.startsWith("char") => StringType
      case "bitmap" => LongType
      case "dim_dc" => LongType
      case "boolean" => BooleanType
      case noSupport => throw new IllegalArgumentException(s"No supported data type: $noSupport")
    }
  }

  // for reader
  // scalastyle:off
  def kylinDimensionDataTypeToSparkType(dataTp: String): org.apache.spark.sql.types.DataType = {
    dataTp match {
      case "string" => StringType
      case "date" => LongType
      case "time" => LongType
      case "timestamp" => LongType
      case "datetime" => LongType
      case "tinyint" => ByteType
      case "smallint" => ShortType
      case "integer" => IntegerType
      case "int4" => IntegerType
      case "bigint" => LongType
      case "long8" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case "real" => DoubleType
      case tp if tp.startsWith("varchar") => StringType
      case tp if tp.startsWith("char") => StringType
      case "bitmap" => LongType
      case "dim_dc" => LongType
      case "decimal" => DecimalType(19, 4)
      case tp if tp.startsWith("decimal") && tp.contains("(") => {
        try {
          val precisionAndScale = tp.replace("decimal", "").replace("(", "").replace(")", "").split(",")
          DataTypes.createDecimalType(precisionAndScale(0).toInt, precisionAndScale(1).toInt)
        } catch {
          case e: Exception =>
            throw new IllegalArgumentException(s"Unsupported data type : $tp", e)
        }
      }
      case "boolean" => BooleanType
      case noSupport => throw new IllegalArgumentException(s"No supported data type: $noSupport")
    }
  }

  // scalastyle:off
  def kylinRawTableSQLTypeToSparkType(dataTp: DataType): org.apache.spark.sql.types.DataType = {
    dataTp.getName match {
      case "decimal" => DecimalType(dataTp.getPrecision, dataTp.getScale)
      case "date" => DateType
      case "time" => DateType
      case "timestamp" => TimestampType
      case "datetime" => DateType
      case "tinyint" => ByteType
      case "smallint" => ShortType
      case "integer" => IntegerType
      case "int4" => IntegerType
      case "bigint" => LongType
      case "long8" => LongType
      case "float" => FloatType
      case "double" => DoubleType
      case "real" => DoubleType
      case tp if tp.startsWith("char") => StringType
      case tp if tp.startsWith("varchar") => StringType
      case "bitmap" => LongType
      case "dim_dc" => LongType
      case "boolean" => BooleanType
      case noSupport => throw new IllegalArgumentException(s"No supported data type: $noSupport")
    }
  }


  def convertSqlTypeToSparkType(dt: RelDataType): org.apache.spark.sql.types.DataType = {
    dt.getSqlTypeName match {
      case SqlTypeName.DECIMAL => DecimalType(dt.getPrecision, dt.getScale)
      case SqlTypeName.CHAR => StringType
      case SqlTypeName.VARCHAR => StringType
      case SqlTypeName.INTEGER => IntegerType
      case SqlTypeName.TINYINT => ByteType
      case SqlTypeName.SMALLINT => ShortType
      case SqlTypeName.BIGINT => LongType
      case SqlTypeName.FLOAT => FloatType
      case SqlTypeName.DOUBLE => DoubleType
      case SqlTypeName.DATE => DateType
      case SqlTypeName.TIMESTAMP => TimestampType
      case SqlTypeName.BOOLEAN => BooleanType
      case noSupport => throw new IllegalArgumentException(s"No supported data type: $noSupport")
    }
  }

  // scalastyle:off
  def convertStringToValue(s: Any, rowType: RelDataType, toCalcite: Boolean): Any = {
    val sqlTypeName = rowType.getSqlTypeName
    if (s == null) {
      null
    } else if (s.toString.isEmpty) {
      val a: Any = sqlTypeName match {
        case SqlTypeName.DECIMAL => new java.math.BigDecimal(0)
        case SqlTypeName.CHAR => s.toString
        case SqlTypeName.VARCHAR => s.toString
        case SqlTypeName.INTEGER => 0
        case SqlTypeName.TINYINT => 0.toByte
        case SqlTypeName.SMALLINT => 0.toShort
        case SqlTypeName.BIGINT => 0L
        case SqlTypeName.FLOAT => 0f
        case SqlTypeName.DOUBLE => 0d
        case SqlTypeName.DATE => 0
        case SqlTypeName.TIMESTAMP => 0L
        case SqlTypeName.TIME => 0L
        case SqlTypeName.BOOLEAN => null;
        case null => null
        case _ => null
      }
    } else {
      try {
        val a: Any = sqlTypeName match {
          case SqlTypeName.DECIMAL =>
            if (s.isInstanceOf[java.lang.Double] || s
              .isInstanceOf[java.lang.Float] || s.toString.contains(".")) {
              new java.math.BigDecimal(s.toString)
                .setScale(rowType.getScale, BigDecimal.ROUND_HALF_EVEN)
            } else {
              new java.math.BigDecimal(s.toString)
            }
          case SqlTypeName.CHAR => s.toString
          case SqlTypeName.VARCHAR => s.toString
          case SqlTypeName.INTEGER => s.toString.toInt
          case SqlTypeName.TINYINT => s.toString.toByte
          case SqlTypeName.SMALLINT => s.toString.toShort
          case SqlTypeName.BIGINT => s.toString.toLong
          case SqlTypeName.FLOAT => java.lang.Float.parseFloat(s.toString)
          case SqlTypeName.DOUBLE => java.lang.Double.parseDouble(s.toString)
          case SqlTypeName.DATE => {
            // time over here is with timezone.
            val string = s.toString
            if (string.contains("-")) {
              val time = DateFormat.stringToDate(string).getTime
              if (toCalcite) {
                (time / (3600 * 24 * 1000)).toInt
              } else {
                // ms to s
                time / 1000
              }
            } else {
              // should not come to here?
              if (toCalcite) {
                (toCalciteTimestamp(DateFormat.stringToMillis(string)) / (3600 * 24 * 1000)).toInt
              } else {
                DateFormat.stringToMillis(string)
              }
            }
          }
          case SqlTypeName.TIMESTAMP | SqlTypeName.TIME => {
            var ts = s.asInstanceOf[Timestamp].getTime
            if (toCalcite) {
              ts
            } else {
              // ms to s
              ts / 1000
            }
          }
          case SqlTypeName.BOOLEAN => s;
          case _ => s.toString
        }
        a
      } catch {
        case th: Throwable =>
          logError(s"Error for convert value : $s , class: ${s.getClass}", th)
          safetyConvertStringToValue(s, rowType, toCalcite)
      }
    }
  }

  def safetyConvertStringToValue(s: Any, rowType: RelDataType, toCalcite: Boolean): Any = {
    try {
      rowType.getSqlTypeName match {
        case SqlTypeName.DECIMAL =>
          if (s.isInstanceOf[java.lang.Double] || s
            .isInstanceOf[java.lang.Float] || s.toString.contains(".")) {
            new java.math.BigDecimal(s.toString)
              .setScale(rowType.getScale, BigDecimal.ROUND_HALF_EVEN)
          } else {
            new java.math.BigDecimal(s.toString)
          }
        case SqlTypeName.CHAR => s.toString
        case SqlTypeName.VARCHAR => s.toString
        case SqlTypeName.INTEGER => s.toString.toDouble.toInt
        case SqlTypeName.TINYINT => s.toString.toDouble.toByte
        case SqlTypeName.SMALLINT => s.toString.toDouble.toShort
        case SqlTypeName.BIGINT => s.toString.toDouble.toLong
        case SqlTypeName.FLOAT => java.lang.Float.parseFloat(s.toString)
        case SqlTypeName.DOUBLE => java.lang.Double.parseDouble(s.toString)
        case SqlTypeName.DATE => {
          // time over here is with timezone.
          val string = s.toString
          if (string.contains("-")) {
            val time = DateFormat.stringToDate(string).getTime
            if (toCalcite) {
              (time / (3600 * 24 * 1000)).toInt
            } else {
              // ms to s
              time / 1000
            }
          } else {
            // should not come to here?
            if (toCalcite) {
              (toCalciteTimestamp(DateFormat.stringToMillis(string)) / (3600 * 24 * 1000)).toInt
            } else {
              DateFormat.stringToMillis(string)
            }
          }
        }
        case SqlTypeName.TIMESTAMP | SqlTypeName.TIME => {
          var ts = s.asInstanceOf[Timestamp].getTime
          if (toCalcite) {
            ts
          } else {
            // ms to s
            ts / 1000
          }
        }
        case SqlTypeName.BOOLEAN => s;
        case _ => s.toString
      }
    } catch {
      case th: Throwable =>
        throw new RuntimeException(s"Error for convert value : $s , class: ${s.getClass}", th)
    }
  }

  // scalastyle:off
  def convertStringToResultValue(s: Any, rowType: String, toCalcite: Boolean): Any = {
    if (s == null) {
      val a: Any = rowType match {
        case "DECIMAL" => new java.math.BigDecimal(0)
        case "CHAR" => null
        case "VARCHAR" => null
        case "INTEGER" => 0
        case "TINYINT" => 0.toByte
        case "SMALLINT" => 0.toShort
        case "BIGINT" => 0L
        case "FLOAT" => 0f
        case "DOUBLE" => 0d
        case "DATE" => 0
        case "TIMESTAMP" => 0L
        case "TIME" => 0L
        case "BOOLEAN" => null
        case null => null
        case _ => null
      }
      a
    } else {
      try {
        val a: Any = rowType match {
          case "DECIMAL" => new java.math.BigDecimal(s.toString)
          case "CHAR" => s.toString
          case "VARCHAR" => s.toString
          case "INTEGER" => s.toString.toInt
          case "TINYINT" => s.toString.toByte
          case "SMALLINT" => s.toString.toShort
          case "BIGINT" => s.toString.toLong
          case "FLOAT" => java.lang.Float.parseFloat(s.toString)
          case "DOUBLE" => java.lang.Double.parseDouble(s.toString)
          case "DATE" => {
            if (toCalcite)
              DateFormat.formatToDateStr(DateFormat.stringToMillis(s.toString))
            else
              DateFormat.stringToMillis(s.toString)
          }
          case "TIMESTAMP" | "TIME" =>
            DateFormat.formatToTimeStr(s.asInstanceOf[Timestamp].getTime)
          case "BOOLEAN" => s
          case _ => s.toString
        }
        a
      } catch {
        case th: Throwable =>
          logError(s"Error for convert value : $s , class: ${s.getClass}", th)
          throw th
      }
    }
  }

  def convertRowToRow(
                       rows: Iterator[Row],
                       typeMap: Map[Int, String],
                       separator: String): Iterator[String] = {
    rows.map {
      row =>
        var rowIndex = 0
        row.toSeq
          .map {
            cell => {
              val rType = typeMap.apply(rowIndex)
              val value =
                SparderTypeUtil
                  .convertStringToResultValue(cell, rType, toCalcite = true)

              rowIndex = rowIndex + 1
              if (value == null) {
                ""
              } else {
                value
              }
            }
          }
          .mkString(separator)
    }

  }

  // ms to second
  def toSparkTimestamp(calciteTimestamp: Long): java.lang.Long = {
    calciteTimestamp / 1000
  }

  // ms to microsecond, spark need micro sec.
  def toSparkMicrosecond(calciteTimestamp: Long): java.lang.Long = {
    calciteTimestamp * 1000
  }

  // ms to day
  def toSparkDate(calciteTimestamp: Long): java.lang.Integer = {
    (calciteTimestamp / 1000 / 3600 / 24).toInt
  }

  def toCalciteTimestamp(sparkTimestamp: Long): Long = {
    sparkTimestamp * 1000
  }

}
