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
package org.apache.spark.sql.utils

import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.kylin.common.util.DateFormat
import org.apache.spark.sql.Column
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Cast
import java.math.BigDecimal

import org.apache.calcite.util.NlsString
import org.apache.calcite.rel.`type`.RelDataType
import java.sql.{Date, Timestamp, Types}
import java.time.ZoneId
import java.util.regex.Pattern

import org.apache.spark.sql.functions.col
import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.rex.RexLiteral
import java.util.{GregorianCalendar, Locale}

import org.apache.kylin.engine.spark.metadata.FunctionDesc
import org.apache.kylin.metadata.datatype.DataType
import org.apache.spark.sql.types._
import org.apache.kylin.engine.spark.cross.CrossDateTimeUtils

object SparkTypeUtil extends Logging {
  private def defaultZoneId = ZoneId.systemDefault()
  private def UTC = ZoneId.of("UTC")

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
      case tp if tp.startsWith("hllc") => LongType
      case tp if tp.startsWith("percentile") => DoubleType
      case tp if tp.startsWith("bitmap") => LongType
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
      case tp if tp.startsWith("varchar") => StringType
      case tp if tp.startsWith("char") => StringType
      case "bitmap" => LongType
      case "dim_dc" => LongType
      case "boolean" => BooleanType
      case _ => throw new IllegalArgumentException
    }
  }

  // scalastyle:off
  def toSparkType(dataTp: DataType, isSum: Boolean = false): org.apache.spark.sql.types.DataType = {
    dataTp.getName match {
      // org.apache.spark.sql.catalyst.expressions.aggregate.Sum#resultType
      case "decimal" =>
        if (isSum) {
          val i = dataTp.getPrecision + 10
          DecimalType(Math.min(DecimalType.MAX_PRECISION, i), dataTp.getScale)
        }
        else DecimalType(dataTp.getPrecision, dataTp.getScale)
      case "date" => DateType
      case "time" => DateType
      case "timestamp" => TimestampType
      case "datetime" => DateType
      case "tinyint" => if (isSum) LongType else ByteType
      case "smallint" => if (isSum) LongType else ShortType
      case "integer" => if (isSum) LongType else IntegerType
      case "int4" => if (isSum) LongType else IntegerType
      case "bigint" => LongType
      case "long8" => LongType
      case "float" => if (isSum) DoubleType else FloatType
      case "double" => DoubleType
      case tp if tp.startsWith("varchar") => StringType
      case tp if tp.startsWith("char") => StringType
      case "dim_dc" => LongType
      case "boolean" => BooleanType
      case tp if tp.startsWith("hllc") => BinaryType
      case tp if tp.startsWith("bitmap") => BinaryType
      case tp if tp.startsWith("extendedcolumn") => BinaryType
      case tp if tp.startsWith("percentile") => BinaryType
      case tp if tp.startsWith("raw") => BinaryType
      case _ => throw new IllegalArgumentException(dataTp.toString)
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
      case SqlTypeName.ANY => StringType
      case _ =>
        throw new IllegalArgumentException(s"unsupported SqlTypeName $dt")
    }
  }

  def convertSparkTypeToSqlType(dt: org.apache.spark.sql.types.DataType): String = {
    dt match {
      case StringType => SqlTypeName.VARCHAR.getName
      case IntegerType => SqlTypeName.INTEGER.getName
      case ByteType => SqlTypeName.TINYINT.getName
      case ShortType => SqlTypeName.SMALLINT.getName
      case LongType => SqlTypeName.BIGINT.getName
      case FloatType => SqlTypeName.FLOAT.getName
      case DoubleType => SqlTypeName.DOUBLE.getName
      case DateType => SqlTypeName.DATE.getName
      case TimestampType => SqlTypeName.TIMESTAMP.getName
      case BooleanType => SqlTypeName.BOOLEAN.getName
      case _ => {
        if (dt.isInstanceOf[DecimalType]) {
          val decimalType = dt.asInstanceOf[DecimalType]
          SqlTypeName.DECIMAL.getName + "(" + decimalType.precision + "," + decimalType.scale + ")"
        } else {
          throw new IllegalArgumentException(s"unsupported SqlTypeName $dt")
        }
      }
    }
  }

  def getValueFromRexLit(literal: RexLiteral) = {
    val ret = literal.getValue match {
      case s: NlsString =>
        s.getValue
      case g: GregorianCalendar =>
        if (literal.getTypeName.getName.equals("DATE")) {
          new Date(CrossDateTimeUtils.stringToTimestamp(literal).get / 1000)
        } else {
          new Timestamp(CrossDateTimeUtils.stringToTimestamp(literal).get / 1000)
        }
      case range: TimeUnitRange =>
        // Extract(x from y) in where clause
        range.name
      case b: java.lang.Boolean =>
        b
      case b: BigDecimal =>
        literal.getType.getSqlTypeName match {
          case SqlTypeName.BIGINT =>
            b.longValue()
          case SqlTypeName.INTEGER =>
            b.intValue()
          case SqlTypeName.DOUBLE =>
            b.doubleValue()
          case SqlTypeName.FLOAT =>
            b.floatValue()
          case SqlTypeName.SMALLINT =>
            b.shortValue()
          case _ =>
            b
        }
      case b: java.lang.Float =>
        b
      case b: java.lang.Double =>
        b
      case b: java.lang.Integer =>
        b
      case b: java.lang.Byte =>
        b
      case b: java.lang.Short =>
        b
      case b: java.lang.Long =>
        b
      case _ =>
        literal.getValue.toString
    }
    ret
  }

  // scalastyle:off
  def convertStringToValue(s: Any, rowType: RelDataType, toCalcite: Boolean): Any = {
    val sqlTypeName = rowType.getSqlTypeName
    if (s == null) {
      null
    } else if (s.toString.isEmpty) {
      sqlTypeName match {
        case SqlTypeName.DECIMAL => new java.math.BigDecimal(0)
        case SqlTypeName.CHAR => s.toString
        case SqlTypeName.VARCHAR => s.toString
        case SqlTypeName.INTEGER => 0
        case SqlTypeName.TINYINT => 0.toByte
        case SqlTypeName.SMALLINT => 0.toShort
        case SqlTypeName.BIGINT => 0L
        case SqlTypeName.FLOAT => 0f
        case SqlTypeName.REAL => 0f
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
          case SqlTypeName.FLOAT => java.lang.Double.parseDouble(s.toString)
          case SqlTypeName.DOUBLE => java.lang.Double.parseDouble(s.toString)
          case SqlTypeName.DATE => {
            // time over here is with timezone.
            val string = s.toString
            if (string.contains("-")) {
              val time = DateFormat.stringToDate(string).getTime
              if (toCalcite) {
                //current date is local timezone, org.apache.calcite.avatica.util.AbstractCursor.DateFromNumberAccessor need to utc
                CrossDateTimeUtils.stringToDate(string).get
              } else {
                // ms to s
                time / 1000
              }
            } else {
              // should not come to here?
              if (toCalcite) {
                (toCalciteTimestamp(DateFormat.stringToMillis(string)) / (3600 * 24 * 1000)).toInt
              } else {
                DateFormat.stringToMillis(string) / 1000
              }
            }
          }
          case SqlTypeName.TIMESTAMP | SqlTypeName.TIME => {
            var ts = s.asInstanceOf[Timestamp].toString
            if (toCalcite) {
              // current ts is local timezone ,org.apache.calcite.avatica.util.AbstractCursor.TimeFromNumberAccessor need to utc
              CrossDateTimeUtils.stringToTimestamp(ts, UTC).get / 1000
            } else {
              // ms to s
              s.asInstanceOf[Timestamp].getTime / 1000
            }
          }
          case SqlTypeName.BOOLEAN => s;
          case _ => s.toString
        }
        a
      } catch {
        case th: Throwable =>
          logError(s"Error for convert value : $s , class: ${s.getClass}", th)
          // fixme aron never come to here, for coverage ignore.
          safetyConvertStringToValue(s, rowType, toCalcite)
      }
    }
  }

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


  // ms to second
  def toSparkTimestamp(calciteTimestamp: Long): java.lang.Long = {
    calciteTimestamp / 1000
  }

  // ms to microsecond, spark need micro sec.
  //  def toSparkMicrosecond(calciteTimestamp: Long): java.lang.Long = {
  //    calciteTimestamp * 1000
  //  }

  // ms to day
  //  def toSparkDate(calciteTimestamp: Long): java.lang.Integer = {
  //    (calciteTimestamp / 1000 / 3600 / 24).toInt
  //  }

  def toCalciteTimestamp(sparkTimestamp: Long): Long = {
    sparkTimestamp * 1000
  }

  def alignDataType(origin: StructType, goal: StructType): Array[Column] = {
    val columns = origin.zip(goal).map {
      case (sparkField, goalField) =>
        val sparkDataType = sparkField.dataType
        val goalDataType = goalField.dataType
        if (!sparkDataType.sameType(goalDataType)) {
          if (Cast.canCast(sparkDataType, goalDataType)) {
            col(sparkField.name).cast(goalDataType)
          } else {
            logError(s"Error for cast datatype from  $sparkDataType to $goalDataType with column name is : ${sparkField.name}")
            col(sparkField.name)
          }
        } else {
          col(sparkField.name)
        }
    }.toArray
    logInfo(s"Align data type is ${columns.mkString(",")}")
    columns
  }

  def generateFunctionReturnDataType(function: FunctionDesc): org.apache.spark.sql.types.DataType = {
    function.expression.toUpperCase(Locale.ROOT) match {
      case "SUM" =>
        toSparkType(function.returnType.toKylinDataType, true)
      case "COUNT" => LongType
      case x if x.startsWith("TOP_N") =>
        val fields = function.pra.drop(1).map(p =>
          StructField(s"DIMENSION_${p.columnName}", p.dataType))
        DataTypes.createArrayType(StructType(Seq(
          StructField("measure", DoubleType),
          StructField("dim", StructType(fields))
        )))
      case "MAX" | "MIN" =>
        function.pra.head.dataType
      case _ => toSparkType(function.returnType.toKylinDataType)
    }
  }

  def convertSparkFieldToJavaField(field: StructField): org.apache.kylin.engine.spark.metadata.cube.StructField = {
    val builder = new org.apache.kylin.engine.spark.metadata.cube.StructField.StructFieldBuilder
    builder.setName(field.name)
    val typeName = if (field.dataType.sql.startsWith("DECIMAL")) {
      "DECIMAL"
    } else {
      field.dataType.sql
    }
    val javaType = typeName match {
      case "BINARY" => Types.BINARY
      case "BOOLEAN" => Types.BOOLEAN
      case "DATE" => Types.DATE
      case "DOUBLE" => Types.DOUBLE
      case "FLOAT" => Types.FLOAT
      case "INT" => Types.INTEGER
      case "BIGINT" => Types.BIGINT
      case "NUMERIC" => Types.NUMERIC
      case "SMALLINT" => Types.SMALLINT
      case "TIMESTAMP" => Types.TIMESTAMP
      case "STRING" => Types.VARCHAR
      case "DECIMAL" =>
        val precisionAndScalePair = getDecimalPrecisionAndScale(typeName)
        if (precisionAndScalePair != null) {
          builder.setPrecision(precisionAndScalePair._1)
          builder.setScale(precisionAndScalePair._2)
        }
        Types.DECIMAL
      case _ => Types.OTHER
    }

    builder.setDataType(javaType)
    builder.setDataTypeName(typeName)
    builder.setNullable(field.nullable)
    builder.createStructField()
  }

  private def getDecimalPrecisionAndScale(javaType: String): (Int, Int) = {
    val DECIMAL_PATTERN = Pattern.compile("DECIMAL\\(([0-9]+),([0-9]+)\\)", Pattern.CASE_INSENSITIVE)
    val decimalMatcher = DECIMAL_PATTERN.matcher(javaType)
    if (decimalMatcher.find) {
      (Integer.valueOf(decimalMatcher.group(1)), Integer.valueOf(decimalMatcher.group(2)))
    }
    else null
  }

}
