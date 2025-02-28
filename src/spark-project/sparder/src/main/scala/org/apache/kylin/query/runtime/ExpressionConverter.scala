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

package org.apache.kylin.query.runtime

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.query.util.UnsupportedSparkFunctionException
import org.apache.spark.sql.Column
import org.apache.spark.sql.KapFunctions._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.SparderTypeUtil

import scala.collection.convert.ImplicitConversions.`seq AsJavaList`
import scala.collection.mutable

object ExpressionConverter {

  private val sparkUdfSet = mutable.HashSet(
    // string functions
    "ascii", "base64", "btrim", "chr", "char", "char_length", "character_length", "concat_ws",
    "decode", "encode", "find_in_set", "initcap", "initcapb", "instr",
    "lcase", "length", "left", "levenshtein", "locate", "lower", "lpad", "ltrim", "overlay",
    "replace", "regexp_extract", "regexp_like", "right", "rlike", "rpad", "repeat", "rtrim",
    "sentences", "space", "split", "split_part", "strpos", "substring_index", "substring", "substr",
    "ucase", "unbase64", "upper",

    // datetime functions
    "add_months", "current_date", "current_timestamp",
    "date_add", "datediff", "date_format", "date_part", "date_sub", "date_trunc",
    "from_utc_timestamp", "from_unixtime", "months_between",
    "to_date", "to_timestamp", "to_utc_timestamp", "trunc", "unix_timestamp", "weekofyear",
    "_ymdint_between",

    // math functions
    "acos", "asin", "atan", "atan2", "bround", "cbrt", "cos", "cosh", "cot", "conv",
    "degrees", "exp", "expm1", "factorial", "hypot",
    "ln", "log", "log1p", "log10", "log2", "pi", "power", "pow",
    "radians", "rint", "round", "sign", "sin", "sinh", "tan", "tanh",

    // leaf functions
    "current_database", "input_file_block_length", "input_file_block_start", "input_file_name",
    "monotonically_increasing_id", "now", "spark_partition_id", "uuid",

    // misc functions
    "crc32", "explode", "if", "ifnull", "isnull", "md5", "nvl", "sha", "sha1", "sha2",

    // collection functions
    "concat", "size"
  )

  private val bitmapUDF = mutable.HashSet("intersect_count_by_col",
    "subtract_bitmap_value", "subtract_bitmap_uuid", "bitmap_uuid_to_array",
    "subtract_bitmap_uuid_count", "subtract_bitmap_uuid_distinct", "subtract_bitmap_uuid_value_all", "subtract_bitmap_uuid_value"
  )

  // scalastyle:off
  def convert(sqlTypeName: SqlTypeName, relDataType: RelDataType, op: SqlKind, opName: String, children: Seq[Any]): Any = {
    op match {
      case IS_NULL =>
        assert(children.size == 1)
        k_lit(children.head).isNull
      case IS_NOT_NULL =>
        assert(children.size == 1)
        k_lit(children.head).isNotNull
      case LIKE =>
        if (children.length == 3) {
          if (!children.last.isInstanceOf[java.lang.String] || children.last.asInstanceOf[java.lang.String].length != 1) {
            throw new UnsupportedOperationException(
              s"Invalid paramters for LIKE ESCAPE, expecting a single char for ESCAPE")
          }
          val escapeChar = children.last.asInstanceOf[java.lang.String].charAt(0)
          k_like(k_lit(children.head), k_lit(children(1)), escapeChar)
        } else if (children.length == 2) {
          k_like(k_lit(children.head), k_lit(children.last))
        } else {
          throw new UnsupportedOperationException(
            s"Invalid paramters for LIKE, expecting LIKE ... , LIKE ... ESCAPE ... ")
        }
      case SIMILAR =>
        if (children.size == 2) {
          k_similar(k_lit(children.head), k_lit(children.last))
        } else if (children.size == 3) {
          if (!children.last.isInstanceOf[java.lang.String] || children.last.asInstanceOf[java.lang.String].length != 1) {
            throw new UnsupportedOperationException(
              s"Invalid paramters for SIMILAR TO ESCAPE, expecting a single char for ESCAPE")
          }
          val escapeChar = children.last.asInstanceOf[java.lang.String].charAt(0)
          val stringReplacedWithEscapeChar = if (!children(1).asInstanceOf[java.lang.String].contains(escapeChar)) {
            children(1)
          } else {
            val charArray = children(1).asInstanceOf[java.lang.String].toCharArray
            var escapeCharReplaced = false
            val stringDeletedEscape = new StringBuilder
            for (i <- 0 until charArray.length) {
              if (charArray(i) != escapeChar) {
                stringDeletedEscape.append(charArray(i))
                escapeCharReplaced = false
              } else {
                if (!escapeCharReplaced) {
                  stringDeletedEscape.append("\\")
                  escapeCharReplaced = true
                } else {
                  stringDeletedEscape.append(escapeChar)
                  escapeCharReplaced = false
                }
              }
            }
            stringDeletedEscape.toString()
          }
          k_similar(k_lit(children.head), k_lit(stringReplacedWithEscapeChar))
        } else {
          throw new UnsupportedOperationException(
            s"Invalid paramters for SIMILAR TO, expecting SIMILAR TO ... , SIMILAR TO ... ESCAPE ... ")
        }
      case MINUS_PREFIX =>
        assert(children.size == 1)
        negate(k_lit(children.head))
      case IN =>
        val (columns, values) = children.map(c => k_lit(c).expr).partition {
          case _: AttributeReference | _: UnresolvedAttribute => true
          case _ => false
        }
        if (columns.size > 1) {
          in(CreateStruct(columns), values)
        } else {
          val values = children.drop(1).map(c => k_lit(c).expr)
          in(k_lit(children.head).expr, values)
        }
      case NOT_IN =>
        val (columns, values) = children.map(c => k_lit(c).expr).partition {
          case _: AttributeReference | _: UnresolvedAttribute => true
          case _ => false
        }
        if (columns.size > 1) {
          not(in(CreateStruct(columns), values))
        } else {
          val values = children.drop(1).map(c => k_lit(c).expr)
          not(in(k_lit(children.head).expr, values))
        }
      case DIVIDE =>
        assert(children.size == 2)
        k_lit(children.head).divide(k_lit(children.last))
      case CASE =>
        val evens =
          children.zipWithIndex.filter(p => p._2 % 2 == 0).map(p => k_lit(p._1))
        val odds =
          children.zipWithIndex.filter(p => p._2 % 2 == 1).map(p => k_lit(p._1))
        assert(evens.length == odds.length + 1)
        val zip = evens zip odds
        var column: Column = null
        if (zip.nonEmpty) {
          column = when(zip.head._1, zip.head._2)
          zip
            .drop(1)
            .foreach(p => {
              column = column.when(p._1, p._2)
            })
        }
        column.otherwise(evens.last)
      case EXTRACT =>
        val timeUnit = children.head.asInstanceOf[String]
        val inputAsTS = children.apply(1)

        timeUnit match {
          case "YEAR" => year(k_lit(inputAsTS))
          case "QUARTER" => quarter(k_lit(inputAsTS))
          case "MONTH" => month(k_lit(inputAsTS))
          case "WEEK" => weekofyear(k_lit(inputAsTS))
          case "DOY" => dayofyear(k_lit(inputAsTS))
          case "DAY" => dayofmonth(k_lit(inputAsTS))
          case "DOW" => dayofweek(k_lit(inputAsTS))
          case "HOUR" => hour(k_lit(inputAsTS))
          case "MINUTE" => minute(k_lit(inputAsTS))
          case "SECOND" => second(k_lit(inputAsTS))
          case _ =>
            throw new UnsupportedSparkFunctionException(
              s"Unsupported function $timeUnit")
        }
      case REINTERPRET =>
        k_lit(children.head)
      case CAST =>
        // all date type is long,skip is
        val goalType = SparderTypeUtil.convertSqlTypeToSparkType(
          relDataType)
        k_lit(children.head).cast(goalType)

      case TRIM =>
        if (children.length == 3) {
          children.head match {
            case "TRAILING" =>
              rtrim(k_lit(children.apply(2)), children.apply(1).asInstanceOf[String])
            case "LEADING" =>
              ltrim(k_lit(children.apply(2)), children.apply(1).asInstanceOf[String])
            case "BOTH" =>
              trim(k_lit(children.apply(2)), children.apply(1).asInstanceOf[String])
          }
        } else {
          trim(k_lit(children.head))
        }

      case OTHER =>
        val funcName = opName.toLowerCase
        funcName match {
          case "||" => concat(k_lit(children.head), k_lit(children.apply(1)))
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported function $funcName")
        }
      case OTHER_FUNCTION =>
        val funcName = opName.toLowerCase
        funcName match {
          // math functions
          case "abs" =>
            abs(
              k_lit(children.head).cast(SparderTypeUtil
                .convertSqlTypeToSparkType(relDataType)))
          case "truncate" =>
            if (children.size == 1) {
              k_truncate(k_lit(children.head), k_lit(0))
            } else {
              k_truncate(k_lit(children.head), k_lit(children.apply(1)))
            }
          // datetime functions
          case "to_char" | "date_format" =>
            val part = k_lit(children.apply(1)).toString().toUpperCase match {
              case "YEAR" => "y"
              case "MONTH" => "M"
              case "DAY" => "d"
              case "HOUR" => "h"
              case "MINUTE" => "m"
              case "MINUTES" => "m"
              case "SECOND" => "s"
              case "SECONDS" => "s"
              case _ => k_lit(children.apply(1)).toString()
            }
            date_format(k_lit(children.head), part)
          case func if sparkUdfSet.contains(func) =>
            call_udf(func, children.map(k_lit): _*)
          case func if bitmapUDF.contains(func) =>
            func match {
              case "intersect_count_by_col" =>
                new Column(IntersectCountByCol(children.head.asInstanceOf[Column].expr.children))
              case "subtract_bitmap_value" | "subtract_bitmap_uuid_value_all" =>
                new Column(SubtractBitmapAllValues(children.head.asInstanceOf[Column].expr,
                  children.last.asInstanceOf[Column].expr,
                  KylinConfig.getInstanceFromEnv.getBitmapValuesUpperBound))
              case "subtract_bitmap_uuid" | "subtract_bitmap_uuid_distinct" =>
                new Column(SubtractBitmapUUID(children.head.asInstanceOf[Column].expr,
                  children.last.asInstanceOf[Column].expr))
              case "subtract_bitmap_uuid_count" =>
                new Column(SubtractBitmapCount(children.head.asInstanceOf[Column].expr,
                  children.last.asInstanceOf[Column].expr))
              case "subtract_bitmap_uuid_value" =>
                val left = children.head.asInstanceOf[Column].expr
                val right = children.get(1).asInstanceOf[Column].expr
                val limitArg = children.get(2)
                val offsetArg = children.get(3)
                val limit = limitArg.toString.toInt
                val offset = offsetArg.toString.toInt
                if (limit < 0 || offset < 0) {
                  throw new UnsupportedOperationException(s"both limit and offset must be >= 0")
                }
                new Column(SubtractBitmapValue(left, right, limit, offset,
                  KylinConfig.getInstanceFromEnv.getBitmapValuesUpperBound))
              case "bitmap_uuid_to_array" =>
                new Column(BitmapUuidToArray(children.head.asInstanceOf[Column].expr))
            }
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported function $funcName")
        }
      case CEIL =>
        if (children.length == 1) {
          ceil(k_lit(children.head))
        } else if (children.length == 2) {
          call_udf("ceil_datetime", children.map(k_lit): _*)
        } else {
          throw new UnsupportedOperationException(
            s"ceil must provide one or two parameters under sparder")
        }
      case FLOOR =>
        if (children.length == 1) {
          floor(k_lit(children.head))
        } else if (children.length == 2) {
          date_trunc(children.apply(1).toString, k_lit(children.head))
        } else {
          throw new UnsupportedOperationException(
            s"floor must provide one or two parameters under sparder")
        }
      case ARRAY_VALUE_CONSTRUCTOR =>
        array(children.map(child => k_lit(child)): _*)
      case ROW =>
        call_udf("named_struct", children.zipWithIndex.flatMap { case (child, i) =>
          Seq(lit(s"col${i + 1}"), k_lit(child))
        }: _*)
      case IS_NOT_DISTINCT_FROM =>
        k_lit(children.head).eqNullSafe(k_lit(children.apply(1)))
      // TDVT SQL626 - null compare with true/false is special
      case IS_TRUE =>
        // false and null => false
        val col = k_lit(children.head)
        col.isNotNull && col
      case IS_NOT_TRUE =>
        // true or not null => true or null => true
        val col = k_lit(children.head)
        col.isNull || !col
      case IS_FALSE =>
        // false and not null => false and null => false
        val col = k_lit(children.head)
        col.isNotNull && !col
      case IS_NOT_FALSE =>
        // true or null => true
        val col = k_lit(children.head)
        col.isNull || col
      // see https://olapio.atlassian.net/browse/KE-42036
      // Calcite 1.30 changed SqlKind.OTHER_FUNCTION with SqlKind.POSITION in SqlPositionFunction
      case POSITION => //position(substr,str,start)
        call_udf("position", children.map(k_lit): _*)
      // see https://olapio.atlassian.net/browse/KE-42354
      // Calcite 1.30 changed SqlKind.OTHER_FUNCTION with SqlKind.ITEM in SqlItemOperator
      case ITEM =>
        element_at(k_lit(children.head), k_lit(children.apply(1).asInstanceOf[Int] + 1))
      // Calcite 1.30 changed the if operator to case, eliminate this change
      case IF =>
        call_udf("if", children.map(k_lit): _*)
      case NVL =>
        call_udf("ifnull", children.map(k_lit): _*)
      case unsupportedFunc =>
        throw new UnsupportedOperationException(unsupportedFunc.toString)
    }
  }
}
