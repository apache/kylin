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

import java.util.Locale

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.kylin.query.exception.UnsupportedSparkFunctionException
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{If, IfNull, StringLocate}
import org.apache.kylin.engine.spark.cross.CrossDateTimeUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.KylinFunctions._
import org.apache.spark.sql.utils.SparkTypeUtil

import scala.collection.mutable

object ExpressionConverter {

  val unaryParameterFunc = mutable.HashSet("ucase", "lcase", "base64",
    "sentences", "unbase64", "crc32", "md5", "sha", "sha1",
    //time
    "weekofyear",
    // math
    "cbrt", "cosh", "expm1", "factorial", "log1p", "log2", "rint", "sinh", "tanh"
  )

  val ternaryParameterFunc = mutable.HashSet("replace", "substring_index", "lpad", "rpad", "conv")
  val binaryParameterFunc =
    mutable.HashSet("decode", "encode", "find_in_set", "levenshtein", "sha2",
      "trunc", "add_months", "date_add", "date_sub", "from_unixtime", "from_utc_timestamp", "to_utc_timestamp",
      // math function
      "bround", "hypot", "log"
    )

  val noneParameterfunc = mutable.HashSet("current_database", "input_file_block_length", "input_file_block_start",
    "input_file_name", "monotonically_increasing_id", "now", "spark_partition_id", "uuid"
  )

  val varArgsFunc = mutable.HashSet("months_between", "locate", "rtrim")

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
        assert(children.size == 2)
        k_like(k_lit(children.head), k_lit(children.last))
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
          case "DOW" => kylin_day_of_week(k_lit(inputAsTS))
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
        val goalType = SparkTypeUtil.convertSqlTypeToSparkType(
          relDataType)
        k_lit(children.head).cast(goalType)

      case TRIM =>
        if (children.length == 3) {
          children.head match {
            case "TRAILING" =>
              rtrim(k_lit(children.last))
            case "LEADING" =>
              ltrim(k_lit(children.last))
            case "BOTH" =>
              trim(k_lit(children.last))
          }
        } else {
          trim(k_lit(children.head))
        }

      case OTHER =>
        val funcName = opName.toLowerCase(Locale.ROOT)
        funcName match {
          case "||" => concat(k_lit(children.head), k_lit(children.apply(1)))
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported function $funcName")
        }
      case OTHER_FUNCTION =>
        val funcName = opName.toLowerCase(Locale.ROOT)
        funcName match {
          //math_funcs
          case "abs" =>
            abs(
              k_lit(children.head).cast(SparkTypeUtil
                .convertSqlTypeToSparkType(relDataType)))
          case "round" =>
            round(
              k_lit(children.head),
              children.apply(1).asInstanceOf[Int])
          case "truncate" =>
            kylin_truncate(k_lit(children.head), children.apply(1).asInstanceOf[Int])
          case "cot" =>
            k_lit(1).divide(tan(k_lit(children.head)))
          // null handling funcs
          case "isnull" =>
            isnull(k_lit(children.head))
          case "ifnull" =>
            new Column(new IfNull(k_lit(children.head).expr, k_lit(children.apply(1)).expr))
          // string_funcs
          case "lower" => lower(k_lit(children.head))
          case "upper" => upper(k_lit(children.head))
          case "char_length" => length(k_lit(children.head))
          case "character_length" => length(k_lit(children.head))
          case "replace " =>
            regexp_replace(k_lit(children.head),
              children.apply(1).asInstanceOf[String],
              children.apply(2).asInstanceOf[String])
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
            k_lit(CrossDateTimeUtils.dateToString())
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
            if (children.length == 0) {
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
            var part = k_lit(children.apply(1)).toString().toUpperCase(Locale.ROOT) match {
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
            log(Math.E, k_lit(children.head))
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
          case "date_part" | "date_trunc" =>
            var part = k_lit(children.head).toString().toUpperCase(Locale.ROOT) match {
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
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported function $funcName")
        }
      case CEIL =>
        ceil(k_lit(children.head))
      case FLOOR =>
        floor(k_lit(children.head))
      case ARRAY_VALUE_CONSTRUCTOR =>
        array(children.map(child => k_lit(child.toString)): _*)
      case unsupportedFunc =>
        throw new UnsupportedOperationException(unsupportedFunc.toString)
    }
  }
}
