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

import org.apache.calcite.DataContext
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.sql.`type`.{BasicSqlType, IntervalSqlType, SqlTypeFamily, SqlTypeName}
import org.apache.calcite.sql.fun.{SqlDateTimeDivisionOperator, SqlDatetimeSubtractionOperator}
import org.apache.kylin.common.util.DateFormat
import org.apache.spark.sql.KapFunctions._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, DateType, LongType, StringType, TimestampType}
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.unsafe.types.UTF8String

import java.sql.Timestamp
import java.time.ZoneId
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Convert RexNode to a nested Column
 *
 * @param inputFieldNames fieldNames
 * @param rowType         rowtyple
 * @param dataContext     context
 */
class SparderRexVisitor(val inputFieldNames: Seq[String],
                        val rowType: RelDataType,
                        val dataContext: DataContext)
  extends RexVisitorImpl[Any](true) {

  def this(dfs: Array[DataFrame],
           rowType: RelDataType,
           dataContext: DataContext) = this(dfs.flatMap(df => df.schema.fieldNames), rowType, dataContext)

  def this(df: DataFrame,
           rowType: RelDataType,
           dataContext: DataContext) = this(Array(df), rowType, dataContext)

  def this(plan: LogicalPlan,
           rowType: RelDataType,
           dataContext: DataContext) = this(plan.output.map(c => c.name), rowType, dataContext)

  // scalastyle:off
  override def visitCall(call: RexCall): Any = {

    val children = new ListBuffer[Any]()
    var isDateType = false
    var isTimeType = false

    for (operand <- call.operands.asScala) {
      if (operand.getType.getSqlTypeName.name().equals("DATE")) {
        isDateType = true
      }
      if (operand.getType.getSqlTypeName.name().equals("TIMESTAMP")) {
        isTimeType = true
      }

      val childFilter = operand.accept(this)
      children += childFilter
    }

    def getColumns: ListBuffer[Column] = {
      children.map {
        case null => new Column(Literal(null, DataTypes.BooleanType))
        // see https://olapio.atlassian.net/browse/KE-42088
        // Calcite 1.30 change child type
        case boolCol: Boolean => new Column(Literal(boolCol.booleanValue(), DataTypes.BooleanType))
        case child =>
          assert(child.isInstanceOf[Column])
          child.asInstanceOf[Column]
      }
    }

    def getOperands: (Column, Column) = {
      var left = k_lit(children.head)
      var right = k_lit(children.last)

      // get the lit pos.
      // ($1, "2010-01-01 15:43:38") pos:1
      // ("2010-01-01 15:43:38", $1) pos:0
      val litPos = call.getOperands.asScala.zipWithIndex
        .filter(!_._1.isInstanceOf[RexInputRef])
        .map(_._2)

      if (isDateType) {
        litPos
          .foreach {
            case 0 => left = left.cast(TimestampType).cast(DateType)
            case 1 => right = right.cast(TimestampType).cast(DateType)
          }
      }
      if (isTimeType) {
        litPos
          .foreach {
            case 0 => left = left.cast(TimestampType)
            case 1 => right = right.cast(TimestampType)
          }
      }

      (left, right)
    }

    val op = call.getOperator
    op.getKind match {
      case AND =>
        getColumns.reduce {
          _.and(_)
        }
      case OR =>
        getColumns.reduce {
          _.or(_)
        }

      case NOT =>
        assert(children.size == 1)
        children.foreach(filter => assert(filter.isInstanceOf[Column]))
        not(children.head.asInstanceOf[Column])
      case EQUALS =>
        assert(children.size == 2)
        val (left: Column, right: Column) = getOperands
        left === right
      case GREATER_THAN =>
        assert(children.size == 2)
        val (left: Column, right: Column) = getOperands
        left > right
      case LESS_THAN =>
        assert(children.size == 2)
        val (left: Column, right: Column) = getOperands
        left < right
      case GREATER_THAN_OR_EQUAL =>
        assert(children.size == 2)
        val (left: Column, right: Column) = getOperands
        left >= right
      case LESS_THAN_OR_EQUAL =>
        assert(children.size == 2)
        val (left: Column, right: Column) = getOperands
        left <= right
      case NOT_EQUALS =>
        assert(children.size == 2)
        val (left: Column, right: Column) = getOperands
        left =!= right
      case PLUS =>
        assert(children.size == 2)
        // see https://olapio.atlassian.net/browse/KE-42035
        // Calcite 1.30 changed the name of operand in the SqlDatetimePlusOperator class,
        // instead of using "DATETIME_PLUS", use "+"
        if (op.getName.equals("+")) {
          // scalastyle:off
          children.last match {
            case num: MonthNum => {
              // both add_month and add_year case
              val ts = k_lit(children.head).cast(TimestampType)
              call.getType.getSqlTypeName match {
                case SqlTypeName.DATE =>
                  return k_lit(k_add_months(k_lit(ts), num.num)).cast(DateType)
                case SqlTypeName.TIMESTAMP =>
                  return k_lit(k_add_months(k_lit(ts), num.num)).cast(TimestampType)
                case _ =>
                  return k_lit(k_add_months(k_lit(ts), num.num))
              }
            }
            case _ =>
          }
          call.getType.getSqlTypeName match {
            case SqlTypeName.CHAR | SqlTypeName.VARCHAR | SqlTypeName.BINARY | SqlTypeName.VARBINARY =>
              return k_lit(children.head)
                .cast(TimestampType)
                .cast(LongType)
                .plus(k_lit(children.last))
                .cast(TimestampType)
                .cast(StringType)
            case _ =>
          }
        }

        call.getType.getSqlTypeName match {
          case SqlTypeName.DATE =>
            k_lit(children.head)
              .cast(TimestampType)
              .cast(LongType)
              .plus(k_lit(children.last))
              .cast(TimestampType)
              .cast(DateType)
          case SqlTypeName.TIMESTAMP =>
            k_lit(children.head)
              .cast(TimestampType)
              .cast(LongType)
              .plus(k_lit(children.last))
              .cast(TimestampType)
          case _ =>
            k_lit(children.head)
              .plus(k_lit(children.last))
        }
      case MINUS =>
        assert(children.size == 2)
        if (op.isInstanceOf[SqlDatetimeSubtractionOperator]) {
          call.getType.getSqlTypeName match {
            case SqlTypeName.DATE =>
              return k_lit(children.head).cast(TimestampType).cast(LongType).minus(lit(children.last)).cast(TimestampType).cast(DateType)
            case SqlTypeName.TIMESTAMP =>
              return k_lit(children.head)
                .cast(LongType)
                .minus(k_lit(children.last))
                .cast(TimestampType)
            case _ =>
          }
          val timeUnitName = call.`type`
            .asInstanceOf[IntervalSqlType]
            .getIntervalQualifier
            .timeUnitRange
            .name
          if ("DAY".equalsIgnoreCase(timeUnitName)
            || "SECOND".equalsIgnoreCase(timeUnitName)
            || "HOUR".equalsIgnoreCase(timeUnitName)
            || "MINUTE".equalsIgnoreCase(timeUnitName)) {
            // for ADD_DAY case
            // the calcite plan looks like: /INT(Reinterpret(-($0, 2012-01-01)), 86400000)
            // and the timeUnitName is DAY

            // for ADD_WEEK case
            // the calcite plan looks like: /INT(CAST(/INT(Reinterpret(-($0, 2000-01-01)), 1000)):INTEGER, 604800)
            // and the timeUnitName is SECOND

            // for MINUTE case
            // the Calcite plan looks like: CAST(/INT(Reinterpret(-($1, CAST($0):TIMESTAMP(0))), 60000)):INTEGER

            // for HOUR case
            // the Calcite plan looks like: CAST(/INT(Reinterpret(-($1, CAST($0):TIMESTAMP(0))), 3600000)):INTEGER

            // expecting ts instead of seconds
            // so we need to multiply 1000 here

            val ts1 = k_lit(children.head).cast(TimestampType).cast(LongType) //col
            val ts2 = k_lit(children.last).cast(TimestampType).cast(LongType) //lit
            ts1.minus(ts2).multiply(1000)

          } else if ("MONTH".equalsIgnoreCase(timeUnitName) || "YEAR"
            .equalsIgnoreCase(timeUnitName)) {

            // for ADD_YEAR case,
            // the calcite plan looks like: CAST(/INT(Reinterpret(-($0, 2000-03-01)), 12)):INTEGER
            // and the timeUnitName is YEAR

            // for ADD_QUARTER case
            // the calcite plan looks like: /INT(CAST(Reinterpret(-($0, 2000-01-01))):INTEGER, 3)
            // and the timeUnitName is MONTH

            // for ADD_MONTH case

            val ts1 = k_lit(children.head).cast(TimestampType)
            val ts2 = k_lit(children.last).cast(TimestampType)
            k_subtract_months(ts1, ts2)

          } else {
            throw new IllegalStateException(
              "Unsupported SqlInterval: " + timeUnitName)
          }
        } else {
          k_lit(children.head).minus(k_lit(children.last))
        }
      case TIMES =>
        assert(children.size == 2)
        children.head match {
          case num: MonthNum => {
            val ts = k_lit(children.apply(1)).cast(TimestampType).cast(LongType)
            MonthNum(k_lit(ts).multiply(k_lit(num.num)))
          }
          case _ =>
            k_lit(children.head).multiply(k_lit(children.last))
        }
      case MOD =>
        assert(children.size == 2)
        val (left: Column, right: Any) = getOperands
        left mod right
      case DIVIDE =>
        assert(children.size == 2)
        var divisionResult = k_lit(children.head).divide(k_lit(children.last))
        if (op.isInstanceOf[SqlDateTimeDivisionOperator]) {
          divisionResult = divisionResult.cast(LongType)
        }
        divisionResult
      case COALESCE =>
        assert(children.size == 2)
        coalesce(children.apply(0).asInstanceOf[Column], k_lit(children.apply(1)))
      case _ =>
        ExpressionConverter.convert(call.getType.getSqlTypeName, call.`type`, op.getKind, op.getName, children)
    }
  }

  override def visitLocalRef(localRef: RexLocalRef) =
    throw new UnsupportedOperationException("local ref:" + localRef)

  override def visitInputRef(inputRef: RexInputRef): Column =
    col(inputFieldNames(inputRef.getIndex))

  override def visitLiteral(literal: RexLiteral): Any = {
    val v = convertFilterValueAfterAggr(literal)
    v match {
      case Some(toReturn) => toReturn
      case None => null
    }
  }

  case class MonthNum(num: Column)

  // as underlying schema types for cuboid table are all "string",
  // we rely spark to convert the cuboid col data from string to real type to finish comparing
  private def convertFilterValueAfterAggr(literal: RexLiteral): Any = {
    if (literal == null || literal.getValue == null) {
      return None
    }

    literal.getType match {
      case t: IntervalSqlType => {
        if (Seq("MONTH", "YEAR", "QUARTER").contains(
          t.getIntervalQualifier.timeUnitRange.name)) {
          return Some(
            MonthNum(k_lit(RexLiteral.intValue(literal))))
        }
        if (literal.getType.getFamily
          .asInstanceOf[SqlTypeFamily] == SqlTypeFamily.INTERVAL_DAY_TIME) {
          return Some(
            SparderTypeUtil.toSparkTimestamp(
              new java.math.BigDecimal(RexLiteral.value(literal).toString).longValue()))
        }
      }

      case literalSql: BasicSqlType => {
        val literalStr = RexLiteral.value(literal).toString
        literalSql.getSqlTypeName match {
          case SqlTypeName.DATE =>
            return Some(stringToTime(literalStr))
          case SqlTypeName.TIMESTAMP =>
            val string = UTF8String.fromString(literalStr)
            val maybeLong = stringToTimestamp(string, ZoneId.systemDefault())
            return Some(toJavaTimestamp(maybeLong.head))
          case _ =>
        }
      }

      case _ =>
    }
    val ret = SparderTypeUtil.getValueFromRexLit(literal)
    Some(ret)
  }

  override def visitDynamicParam(dynamicParam: RexDynamicParam): Any = {
    val name = dynamicParam.getName
    var s = dataContext.get(name)
    val rType = dynamicParam.getType

    if (rType.getSqlTypeName.getName.equals("TIMESTAMP")) {
      if (s.isInstanceOf[Timestamp]) {
        return s
      }
      return new Timestamp(DateFormat.stringToMillis(s.toString))
    }
    SparderTypeUtil.convertStringToValue(s, rType, toCalcite = false)
  }
}
