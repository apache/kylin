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
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionUtils.expression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ApproxCountDistinctDecode, CeilDateTime, DictEncode, EmptyRow, Expression, ExpressionInfo, FloorDateTime, ImplicitCastInputTypes, In, KapAddMonths, KapDayOfWeek, KapSubtractMonths, Like, Literal, PercentileDecode, PreciseCountDistinctDecode, RLike, RoundBase, SplitPart, Sum0, TimestampAdd, TimestampDiff, Truncate}
import org.apache.spark.sql.types.{ArrayType, BinaryType, ByteType, DataType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType}
import org.apache.spark.sql.udaf.{ApproxCountDistinct, IntersectCount, Percentile, PreciseBitmapBuildBase64Decode, PreciseBitmapBuildBase64WithIndex, PreciseBitmapBuildPushDown, PreciseCardinality, PreciseCountDistinct, PreciseCountDistinctAndArray, PreciseCountDistinctAndValue, ReusePreciseCountDistinct}

object KapFunctions {


  private def withAggregateFunction(func: AggregateFunction,
                                    isDistinct: Boolean = false): Column = {
    Column(func.toAggregateExpression(isDistinct))
  }

  def k_add_months(startDate: Column, numMonths: Column): Column = {
    Column(KapAddMonths(startDate.expr, numMonths.expr))
  }

  def k_subtract_months(date0: Column, date1: Column): Column = {
    Column(KapSubtractMonths(date0.expr, date1.expr))
  }

  def k_day_of_week(date: Column): Column = Column(KapDayOfWeek(date.expr))

  def k_like(left: Column, right: Column, escapeChar: Char = '\\'): Column = Column(Like(left.expr, right.expr, escapeChar))

  def k_similar(left: Column, right: Column): Column = Column(RLike(left.expr, right.expr))

  def sum0(e: Column): Column = withAggregateFunction {
    Sum0(e.expr)
  }

  // special lit for KE.
  def k_lit(literal: Any): Column = literal match {
    case c: Column => c
    case s: Symbol => new ColumnName(s.name)
    case _ => Column(Literal(literal))
  }

  def in(value: Expression, list: Seq[Expression]): Column = Column(In(value, list))

  def k_percentile(head: Column, column: Column, precision: Int): Column =
    Column(Percentile(head.expr, precision, Some(column.expr), DoubleType).toAggregateExpression())

  def k_percentile_decode(column: Column, p: Column, precision: Int): Column =
    Column(PercentileDecode(column.expr, p.expr, Literal(precision)))

  def precise_count_distinct(column: Column): Column =
    Column(PreciseCountDistinct(column.expr, LongType).toAggregateExpression())

  def precise_count_distinct_decode(column: Column): Column =
    Column(PreciseCountDistinctDecode(column.expr))

  def precise_bitmap_uuid(column: Column): Column =
    Column(PreciseCountDistinct(column.expr, BinaryType).toAggregateExpression())

  def precise_bitmap_build(column: Column): Column =
    Column(PreciseBitmapBuildBase64WithIndex(column.expr, StringType).toAggregateExpression())

  def precise_bitmap_build_decode(column: Column): Column =
    Column(PreciseBitmapBuildBase64Decode(column.expr))

  def precise_bitmap_build_pushdown(column: Column): Column =
    Column(PreciseBitmapBuildPushDown(column.expr).toAggregateExpression())

  def approx_count_distinct(column: Column, precision: Int): Column =
    Column(ApproxCountDistinct(column.expr, precision).toAggregateExpression())

  def approx_count_distinct_decode(column: Column, precision: Int): Column =
    Column(ApproxCountDistinctDecode(column.expr, Literal(precision)))

  def k_truncate(column: Column, scale: Int): Column = {
    Column(TRUNCATE(column.expr, Literal(scale)))
  }

  def intersect_count(separator: String, upperBound: Int, columns: Column*): Column = {
    require(columns.size == 3, s"Input columns size ${columns.size} don't equal to 3.")
    val expressions = columns.map(_.expr)
    Column(IntersectCount(expressions.apply(0), expressions.apply(1), expressions.apply(2),
      k_lit(IntersectCount.RAW_STRING).expr, LongType, separator, upperBound).toAggregateExpression()
    )
  }

  def intersect_value(separator: String, upperBound: Int, columns: Column*): Column = {
    require(columns.size == 3, s"Input columns size ${columns.size} don't equal to 3.")
    val expressions = columns.map(_.expr)
    Column(IntersectCount(expressions.apply(0), expressions.apply(1), expressions.apply(2),
      k_lit(IntersectCount.RAW_STRING).expr, ArrayType(LongType, containsNull = false), separator, upperBound).toAggregateExpression()
    )
  }

  def intersect_bitmap(separator: String, upperBound: Int, columns: Column*): Column = {
    require(columns.size == 3, s"Input columns size ${columns.size} don't equal to 3.")
    val expressions = columns.map(_.expr)
    Column(IntersectCount(expressions.apply(0), expressions.apply(1), expressions.apply(2),
      k_lit(IntersectCount.RAW_STRING).expr, BinaryType, separator, upperBound).toAggregateExpression()
    )
  }


  def intersect_count_v2(filterType: Column, separator: String, upperBound: Int, columns: Column*): Column = {
    require(columns.size == 3, s"Input columns size ${columns.size} don't equal to 3.")
    val expressions = columns.map(_.expr)
    Column(IntersectCount(expressions.apply(0), expressions.apply(1), expressions.apply(2),
      filterType.expr, LongType, separator, upperBound
    ).toAggregateExpression())
  }

  def intersect_value_v2(filterType: Column, separator: String, upperBound: Int, columns: Column*): Column = {
    require(columns.size == 3, s"Input columns size ${columns.size} don't equal to 3.")
    val expressions = columns.map(_.expr)
    Column(IntersectCount(expressions.apply(0), expressions.apply(1), expressions.apply(2),
      filterType.expr, ArrayType(LongType, containsNull = false), separator, upperBound
    ).toAggregateExpression())
  }

  def intersect_bitmap_v2(filterType: Column, separator: String, upperBound: Int, columns: Column*): Column = {
    require(columns.size == 3, s"Input columns size ${columns.size} don't equal to 3.")
    val expressions = columns.map(_.expr)
    Column(IntersectCount(expressions.apply(0), expressions.apply(1), expressions.apply(2),
      filterType.expr, BinaryType, separator, upperBound
    ).toAggregateExpression())
  }

  case class TRUNCATE(child: Expression, scale: Expression)
    extends RoundBase(child, scale, BigDecimal.RoundingMode.DOWN, "ROUND_DOWN")
      with Serializable with ImplicitCastInputTypes {
    def this(child: Expression) = this(child, Literal(0))

    private lazy val scaleV: Any = scale.eval(EmptyRow)
    private lazy val _scale: Int = scaleV.asInstanceOf[Int]

    override lazy val dataType: DataType = child.dataType match {
      // if the new scale is bigger which means we are scaling up,
      // keep the original scale as `Decimal` does
      case DecimalType.Fixed(p, s) => DecimalType(p, scala.math.max(if (_scale > s) s else _scale, 0))
      case t => t
    }

    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val ce = child.genCode(ctx)
      val modeStr = "ROUND_DOWN"
      val evaluationCode = dataType match {
        case DecimalType.Fixed(_, s) =>
          s"""
             |java.math.BigDecimal dec1 = ${ce.value}.toJavaBigDecimal().movePointRight(${_scale})
             |          .setScale(0, java.math.BigDecimal.${modeStr}).movePointLeft(${_scale});
             |        ${ev.value} = new Decimal().set(scala.math.BigDecimal.exact(dec1));
             |        ${ev.isNull} = ${ev.value} == null;
         """.stripMargin
        case ByteType =>
          if (_scale < 0) {
            s"""
          ${ev.value} = new java.math.BigDecimal(${ce.value}).
            setScale(${_scale}, java.math.BigDecimal.${modeStr}).byteValue();"""
          } else {
            s"${ev.value} = ${ce.value};"
          }
        case ShortType =>
          if (_scale < 0) {
            s"""
          ${ev.value} = new java.math.BigDecimal(${ce.value}).
            setScale(${_scale}, java.math.BigDecimal.${modeStr}).shortValue();"""
          } else {
            s"${ev.value} = ${ce.value};"
          }
        case IntegerType =>
          if (_scale < 0) {
            s"""
          ${ev.value} = new java.math.BigDecimal(${ce.value}).
            setScale(${_scale}, java.math.BigDecimal.${modeStr}).intValue();"""
          } else {
            s"${ev.value} = ${ce.value};"
          }
        case LongType =>
          if (_scale < 0) {
            s"""
          ${ev.value} = new java.math.BigDecimal(${ce.value}).
            setScale(${_scale}, java.math.BigDecimal.${modeStr}).longValue();"""
          } else {
            s"${ev.value} = ${ce.value};"
          }
        case FloatType => // if child eval to NaN or Infinity, just return it.
          s"""
          if (Float.isNaN(${ce.value}) || Float.isInfinite(${ce.value})) {
            ${ev.value} = ${ce.value};
          } else {
            ${ev.value} = java.math.BigDecimal.valueOf(${ce.value}).
              setScale(${_scale}, java.math.BigDecimal.${modeStr}).floatValue();
          }"""
        case DoubleType => // if child eval to NaN or Infinity, just return it.
          s"""
          if (Double.isNaN(${ce.value}) || Double.isInfinite(${ce.value})) {
            ${ev.value} = ${ce.value};
          } else {
            ${ev.value} = java.math.BigDecimal.valueOf(${ce.value}).
              setScale(${_scale}, java.math.BigDecimal.${modeStr}).doubleValue();
          }"""
      }
      if (scaleV == null) { // if scale is null, no need to eval its child at all
        ev.copy(code =
          code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};""")
      } else {
        ev.copy(code =
          code"""
        ${ce.code}
        boolean ${ev.isNull} = ${ce.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if (!${ev.isNull}) {
          $evaluationCode
        }""")
      }
    }

    override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
      val newChildren = Seq(newLeft, newRight)
      super.legacyWithNewChildren(newChildren)
    }
  }

  def dict_encode(column: Column, dictParams: Column, bucketSize: Column): Column = {
    Column(DictEncode(column.expr, dictParams.expr, bucketSize.expr))
  }


  val builtin: Seq[FunctionEntity] = Seq(
    FunctionEntity(expression[TimestampAdd]("TIMESTAMPADD")),
    FunctionEntity(expression[TimestampDiff]("TIMESTAMPDIFF")),
    FunctionEntity(expression[Truncate]("TRUNCATE")),
    FunctionEntity(expression[DictEncode]("DICTENCODE")),
    FunctionEntity(expression[SplitPart]("split_part")),
    FunctionEntity(expression[FloorDateTime]("floor_datetime")),
    FunctionEntity(expression[CeilDateTime]("ceil_datetime")),
    FunctionEntity(expression[ReusePreciseCountDistinct]("bitmap_or")),
    FunctionEntity(expression[PreciseCardinality]("bitmap_cardinality")),
    FunctionEntity(expression[PreciseCountDistinctAndValue]("bitmap_and_value")),
    FunctionEntity(expression[PreciseCountDistinctAndArray]("bitmap_and_ids")),
    FunctionEntity(expression[PreciseCountDistinctDecode]("precise_count_distinct_decode")),
    FunctionEntity(expression[ApproxCountDistinctDecode]("approx_count_distinct_decode")),
    FunctionEntity(expression[PercentileDecode]("percentile_decode")),
    FunctionEntity(expression[PreciseBitmapBuildPushDown]("bitmap_build"))
  )
}

case class FunctionEntity(name: FunctionIdentifier,
                          info: ExpressionInfo,
                          builder: FunctionBuilder)

object FunctionEntity {
  def apply(tuple: (String, (ExpressionInfo, FunctionBuilder))): FunctionEntity = {
    new FunctionEntity(FunctionIdentifier.apply(tuple._1), tuple._2._1, tuple._2._2)
  }
}
