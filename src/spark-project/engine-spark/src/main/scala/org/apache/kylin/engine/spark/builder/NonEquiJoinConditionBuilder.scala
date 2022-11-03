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

package org.apache.kylin.engine.spark.builder;

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`.{BasicSqlType, IntervalSqlType, SqlTypeName}
import org.apache.kylin.engine.spark.job.NSparkCubingUtil
import org.apache.kylin.metadata.model.NonEquiJoinCondition
import org.apache.kylin.query.calcite.KylinRelDataTypeSystem
import org.apache.kylin.query.runtime.ExpressionConverter
import org.apache.spark.sql.Column
import org.apache.spark.sql.KapFunctions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DateType, LongType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import java.time.ZoneId

object NonEquiJoinConditionBuilder {

  import org.apache.calcite.sql.SqlKind._
  import org.apache.spark.sql.functions._

  val typeSystem = new KylinRelDataTypeSystem()

  private def getLiteral(literal: Any, dataType: RelDataType): Any = {
    if (literal == null) {
      return null
    }

    dataType.getSqlTypeName match {
      case SqlTypeName.DATE =>
        return DateTimeUtils.stringToTime(literal.toString)
      case SqlTypeName.TIMESTAMP =>
        return DateTimeUtils.toJavaTimestamp(DateTimeUtils.stringToTimestamp(UTF8String.fromString(literal.toString),
          ZoneId.systemDefault()).head)
      case _ =>
    }
    literal
  }

  private def k_col(value: Any): Column = {
    value match {
      case col: Column => col
      case other => lit(other)
    }
  }

  def convert(condDesc: NonEquiJoinCondition): Column = {
    k_col(doConvert(condDesc))
  }

  private def doConvert(condDesc: NonEquiJoinCondition): Any = {
    val children = condDesc.getOperands.map(in => doConvert(in))

    def convertBinary: (Column, Column) = {
      assert(children.length == 2)
      (k_col(children.head), k_col(children.last))
    }

    // TODO: get rid of RelDataType
    val dataType = condDesc.getDataType
    val relDataType =
      if (dataType.getTypeName.allowsPrecScale(true, true)) {
        new BasicSqlType(typeSystem, dataType.getTypeName, dataType.getPrecision, dataType.getScale)
      } else if (dataType.getTypeName.allowsPrec()) {
        new BasicSqlType(typeSystem, dataType.getTypeName, dataType.getPrecision)
      } else {
        new BasicSqlType(typeSystem, dataType.getTypeName)
      }
    val sqlTypeName = condDesc.getDataType.getTypeName
    val op = condDesc.getOp
    val opName = condDesc.getOpName
    op match {
      case INPUT_REF =>
        col(NSparkCubingUtil.convertFromDot(condDesc.getColRef.getBackTickIdentity))
      case LITERAL =>
        getLiteral(condDesc.getTypedValue, relDataType)
      case AND =>
        condDesc.getOperands.map(childCond => convert(childCond)).reduce(_ && _)
      case OR =>
        condDesc.getOperands.map(childCond => convert(childCond)).reduce(_ || _)
      case NOT =>
        assert(condDesc.getOperands.length == 1)
        not(condDesc.getOperands.map(childCond => convert(childCond)).head)
      case EQUALS =>
        val (left: Column, right: Column) = convertBinary
        left === right
      case GREATER_THAN =>
        val (left: Column, right: Column) = convertBinary
        left > right
      case LESS_THAN =>
        val (left: Column, right: Column) = convertBinary
        left < right
      case GREATER_THAN_OR_EQUAL =>
        val (left: Column, right: Column) = convertBinary
        left >= right
      case LESS_THAN_OR_EQUAL =>
        val (left: Column, right: Column) = convertBinary
        left <= right
      case NOT_EQUALS =>
        val (left: Column, right: Column) = convertBinary
        left =!= right
      case PLUS =>
        assert(children.length == 2)

        sqlTypeName match {
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
        assert(children.length == 2)
        if (sqlTypeName == SqlTypeName.DATE || sqlTypeName == SqlTypeName.TIMESTAMP) {
          sqlTypeName match {
            case SqlTypeName.DATE =>
              return k_lit(children.head).cast(TimestampType).cast(LongType).minus(lit(children.last)).cast(TimestampType).cast(DateType)
            case SqlTypeName.TIMESTAMP =>
              return k_lit(children.head)
                .cast(LongType)
                .minus(k_lit(children.last))
                .cast(TimestampType)
            case _ =>
          }
          val timeUnitName = relDataType
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

            val ts1 = k_lit(children.head).cast(TimestampType).cast(LongType) // col
            val ts2 = k_lit(children.last).cast(TimestampType).cast(LongType) // lit
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
        assert(children.length == 2)
        k_lit(children.head).multiply(k_lit(children.last))
      case MOD =>
        assert(children.size == 2)
        val (left: Column, right: Any) = convertBinary
        left mod right
      case _ =>
        ExpressionConverter.convert(sqlTypeName, relDataType, op, opName, children)
    }
  }
}
