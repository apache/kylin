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
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Cast, If, IfNull, IntersectCountByCol, Literal, StringLocate, StringRepeat, StringReplace, SubtractBitmapUUID, SubtractBitmapValue}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.SparderTypeUtil

import scala.collection.mutable

object ExpressionConverter {

  val unaryParameterFunc = mutable.HashSet("ucase", "lcase", "base64",
    "sentences", "unbase64", "crc32", "md5", "sha", "sha1",
    // time
    "weekofyear",
    // math
    "cbrt", "cosh", "expm1", "factorial", "log1p", "log2", "rint", "sinh", "tanh",
    // other
    "explode"
  )

  val ternaryParameterFunc = mutable.HashSet("replace", "substring_index", "lpad", "rpad", "conv")
  val binaryParameterFunc =
    mutable.HashSet("decode", "encode", "find_in_set", "levenshtein", "sha2",
      "trunc", "add_months", "date_add", "date_sub", "from_utc_timestamp", "to_utc_timestamp",
      // math function
      "bround", "hypot", "log"
    )

  val noneParameterfunc = mutable.HashSet("current_database", "input_file_block_length", "input_file_block_start",
    "input_file_name", "monotonically_increasing_id", "now", "spark_partition_id", "uuid"
  )

  val varArgsFunc = mutable.HashSet("months_between", "locate", "rtrim", "from_unixtime")

  val bitmapUDF = mutable.HashSet("intersect_count_by_col", "subtract_bitmap_value", "subtract_bitmap_uuid");

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
      case IN => val values = children.drop(1).map(c => k_lit(c).expr)
        in(k_lit(children.head).expr, values)
      case NOT_IN =>
        val values = children.drop(1).map(c => k_lit(c).expr)
        not(in(k_lit(children.head).expr, values))
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
          case "DOW" => k_day_of_week(k_lit(inputAsTS))
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
          //math_funcs
          case "abs" =>
            abs(
              k_lit(children.head).cast(SparderTypeUtil
                .convertSqlTypeToSparkType(relDataType)))
          case "round" =>
            var scale = children.apply(1)
            if (scale.isInstanceOf[Column]) {
              val extractConst = (scale: Column) => {
                val extractCastValue = (child: Literal) => child.value
                val reduceCast = (expr: Cast) => extractCastValue(expr.child.asInstanceOf[Literal])
                scale.expr.getClass.getSimpleName match {
                  case "Cast" =>
                    reduceCast(scale.expr.asInstanceOf[Cast])
                  case _ =>
                    throw new UnsupportedOperationException(s"Scale parameter of round function doesn't support this expression " + scale.expr.toString())
                }
              }
              scale = extractConst(scale.asInstanceOf[Column])
            }
            round(
              k_lit(children.head),
              scale.asInstanceOf[Int])
          case "truncate" =>
            if (children.size == 1) {
              k_truncate(k_lit(children.head), 0)
            } else {
              k_truncate(k_lit(children.head), children.apply(1).asInstanceOf[Int])
            }
          case "cot" =>
            k_lit(1).divide(tan(k_lit(children.head)))
          // null handling funcs
          case "isnull" =>
            isnull(k_lit(children.head))
          case "ifnull" =>
            new Column(new IfNull(k_lit(children.head).expr, k_lit(children.apply(1)).expr))
          case "nvl" =>
            new Column(new IfNull(k_lit(children.head).expr, k_lit(children.apply(1)).expr))
          // string_funcs
          case "lower" => lower(k_lit(children.head))
          case "upper" => upper(k_lit(children.head))
          case "char_length" => length(k_lit(children.head))
          case "character_length" => length(k_lit(children.head))
          case "replace" =>
            new Column(StringReplace(k_lit(children.head).expr,
              k_lit(children.apply(1)).expr,
              k_lit(children.apply(2)).expr))
          case "substring" | "substr" =>
            if (children.length == 3) { //substr(str1,startPos,length)
              k_lit(children.head)
                .substr(k_lit(children.apply(1)), k_lit(children.apply(2)))
            } else if (children.length == 2) { //substr(str1,startPos)
              k_lit(children.head).
                substr(k_lit(children.apply(1)), k_lit(Int.MaxValue))
            } else {
              throw new UnsupportedOperationException(
                s"substring must provide three or two parameters under sparder")
            }
          case "initcapb" =>
            initcap(k_lit(children.head))
          case "instr" =>
            val instr =
              if (children.length == 2) 1
              else children.apply(2).asInstanceOf[Int]
            new Column(StringLocate(k_lit(children.apply(1)).expr, k_lit(children.head).expr, lit(instr).expr)) //instr(str,substr,start)
          case "length" =>
            length(k_lit(children.head))
          case "left" =>
            substring(k_lit(children.head), 1, children.apply(1).asInstanceOf[Int])
          case "repeat" =>
            repeat(k_lit(children.head), children.apply(1).asInstanceOf[Int])
          case "size" =>
            size(k_lit(children.head))
          case "strpos" =>
            val pos =
              if (children.length == 2) 1
              else children.apply(2).asInstanceOf[Int]
            new Column(StringLocate(k_lit(children.apply(1)).expr, k_lit(children.head).expr, lit(pos).expr)) //strpos(str,substr,start)
          case "position" =>
            val pos =
              if (children.length == 2) 1
              else children.apply(2).asInstanceOf[Int]
            new Column(StringLocate(k_lit(children.head).expr, k_lit(children.apply(1)).expr, lit(pos).expr)) //position(substr,str,start)
          case "concat" =>
            concat(k_lit(children.head), k_lit(children.apply(1)))
          case "concat_ws" =>
            concat_ws(children.head.toString, k_lit(children.apply(1)))
          case "split_part" =>
            val args = Seq(k_lit(children.head), lit(children.apply(1)), lit(children.apply(2).asInstanceOf[Int])).toArray
            callUDF("split_part", args: _*)
          // time_funcs
          case "current_date" =>
            current_date()
          case "current_timestamp" =>
            current_timestamp()
          case "to_timestamp" =>
            if (children.length == 1) {
              to_timestamp(k_lit(children.head))
            } else if (children.length == 2) {
              to_timestamp(k_lit(children.head), k_lit(children.apply(1)).toString())
            } else {
              throw new UnsupportedOperationException(
                s"to_timestamp must provide one or two parameters under sparder")
            }
          case "unix_timestamp" =>
            if (children.isEmpty) {
              unix_timestamp
            } else if (children.length == 1) {
              unix_timestamp(k_lit(children.head))
            } else if (children.length == 2) {
              unix_timestamp(k_lit(children.head), k_lit(children.apply(1)).toString())
            } else {
              throw new UnsupportedOperationException(
                s"unix_timestamp only supports two or fewer parameters")
            }
          case "to_date" =>
            if (children.length == 1) {
              to_date(k_lit(children.head))
            } else if (children.length == 2) {
              to_date(k_lit(children.head), k_lit(children.apply(1)).toString())
            } else {
              throw new UnsupportedOperationException(
                s"to_date must provide one or two parameters under sparder")
            }
          case "to_char" | "date_format" =>
            var part = k_lit(children.apply(1)).toString().toUpperCase match {
              case "YEAR" =>
                "y"
              case "MONTH" =>
                "M"
              case "DAY" =>
                "d"
              case "HOUR" =>
                "h"
              case "MINUTE" =>
                "m"
              case "MINUTES" =>
                "m"
              case "SECOND" =>
                "s"
              case "SECONDS" =>
                "s"
              case _ =>
                k_lit(children.apply(1)).toString()
            }
            date_format(k_lit(children.head), part)
          case "power" =>
            pow(k_lit(children.head), k_lit(children.apply(1)))
          case "log10" =>
            log10(k_lit(children.head))
          case "ln" =>
            log(k_lit(children.head))
          case "exp" =>
            exp(k_lit(children.head))
          case "acos" =>
            acos(k_lit(children.head))
          case "asin" =>
            asin(k_lit(children.head))
          case "atan" =>
            atan(k_lit(children.head))
          case "atan2" =>
            assert(children.size == 2)
            atan2(k_lit(children.head), k_lit(children.last))
          case "cos" =>
            cos(k_lit(children.head))
          case "degrees" =>
            degrees(k_lit(children.head))
          case "radians" =>
            radians(k_lit(children.head))
          case "sign" =>
            signum(k_lit(children.head))
          case "tan" =>
            tan(k_lit(children.head))
          case "sin" =>
            sin(k_lit(children.head))
          case func if (noneParameterfunc.contains(func)) =>
            callUDF(func)
          case func if (unaryParameterFunc.contains(func)) =>
            callUDF(func, k_lit(children.head))
          case func if (binaryParameterFunc.contains(func)) =>
            callUDF(func, k_lit(children.head), k_lit(children.apply(1)))
          case func if (ternaryParameterFunc.contains(func)) =>
            callUDF(func, k_lit(children.head), k_lit(children.apply(1)), k_lit(children.apply(2)))
          case func if (varArgsFunc.contains(func)) => {
            callUDF(func, children.map(k_lit(_)): _*)
          }
          case "date_part" =>
            var part = k_lit(children.head).toString().toUpperCase match {
              case "YEAR" =>
                "y"
              case "MONTH" =>
                "M"
              case "DAY" =>
                "d"
              case "HOUR" =>
                "h"
              case "MINUTE" =>
                "m"
              case "MINUTES" =>
                "m"
              case "SECOND" =>
                "s"
              case "SECONDS" =>
                "s"
              case _ =>
                k_lit(children.head).toString()
            }
            date_format(k_lit(children.apply(1)), part)
          case "date_trunc" =>
            date_trunc(children.head.toString, k_lit(children.apply(1)))
          case "datediff" =>
            datediff(k_lit(children.head), k_lit(children.apply(1)))
          case "initcap" =>
            initcap(k_lit(children.head))
          case "pi" =>
            k_lit(Math.PI)
          case "regexp_like" | "rlike" =>
            k_lit(children.head).rlike(children.apply(1).toString)
          case "if" =>
            new Column(new If(k_lit(children.head).expr, k_lit(children.apply(1)).expr, k_lit(children.apply(2)).expr))
          case "overlay" =>
            if (children.length == 3) {
              concat(substring(k_lit(children.head), 0, children.apply(2).asInstanceOf[Int] - 1), k_lit(children.apply(1)),
                substring(k_lit(children.head), children.apply(2).asInstanceOf[Int] + children.apply(1).toString.length, Integer.MAX_VALUE))
            } else if (children.length == 4) {
              concat(substring(k_lit(children.head), 0, children.apply(2).asInstanceOf[Int] - 1), k_lit(children.apply(1)),
                substring(k_lit(children.head), (children.apply(2).asInstanceOf[Int] + children.apply(3).asInstanceOf[Int]), Integer.MAX_VALUE))
            } else {
              throw new UnsupportedOperationException(
                s"overlay must provide three or four parameters under sparder")
            }
          case func if bitmapUDF.contains(func) =>
            func match {
              case "intersect_count_by_col" =>
                new Column(IntersectCountByCol(children.head.asInstanceOf[Column].expr.children.toSeq))
              case "subtract_bitmap_value" =>
                new Column(SubtractBitmapValue(children.head.asInstanceOf[Column].expr, children.last.asInstanceOf[Column].expr, KylinConfig.getInstanceFromEnv.getBitmapValuesUpperBound))
              case "subtract_bitmap_uuid" =>
                new Column(SubtractBitmapUUID(children.head.asInstanceOf[Column].expr, children.last.asInstanceOf[Column].expr))
            }
          case "ascii" =>
            ascii(k_lit(children.head))
          case "chr" =>
            new Column(expressions.Chr(k_lit(children.head).expr))
          case "space" =>
            new Column(StringRepeat(k_lit(" ").expr, k_lit(children.head).expr))
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported function $funcName")
        }
      case CEIL =>
        if (children.length == 1) {
          ceil(k_lit(children.head))
        } else if (children.length == 2) {
          callUDF("ceil_datetime", children.map(k_lit): _*)
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
      case (IS_FALSE | IS_NOT_TRUE) =>
        not(k_lit(children.head))
      case (IS_TRUE | IS_NOT_FALSE) =>
        k_lit(children.head)
      case unsupportedFunc =>
        throw new UnsupportedOperationException(unsupportedFunc.toString)
    }
  }
}
