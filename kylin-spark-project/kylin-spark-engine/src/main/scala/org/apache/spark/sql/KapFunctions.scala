/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, DictEncode, Expression, ExpressionInfo, ImplicitCastInputTypes, In, Like, Literal, RoundBase, SplitPart, TimestampAdd, TimestampDiff, Truncate, UnaryExpression}
import org.apache.spark.sql.catalyst.util.KapDateTimeUtils
import org.apache.spark.sql.types.{AbstractDataType, DataType, DateType, IntegerType}
import org.apache.spark.sql.catalyst.expressions.ExpressionUtils

object KapFunctions {

  def dict_encode(column: Column, dictParams: Column, bucketSize: Column): Column = {
    Column(DictEncode(column.expr, dictParams.expr, bucketSize.expr))
  }

  // special lit for KE.
  def k_lit(literal: Any): Column = literal match {
    case c: Column => c
    case s: Symbol => new ColumnName(s.name)
    case _ => Column(Literal(literal))
  }

  def k_like(left: Column, right: Column): Column = Column(Like(left.expr, right.expr))

  def in(value: Expression, list: Seq[Expression]): Column = Column(In(value, list))

  def kap_day_of_week(date: Column): Column = Column(KapDayOfWeek(date.expr))

  def kap_truncate(column: Column, scale: Int): Column = {
    Column(TRUNCATE(column.expr, Literal(scale)))
  }

  def kap_subtract_months(date0: Column, date1: Column): Column = {
    Column(KapSubtractMonths(date0.expr, date1.expr))
  }

  val builtin: Seq[FunctionEntity] = Seq(
    FunctionEntity(ExpressionUtils.expression[TimestampAdd]("TIMESTAMPADD")),
    FunctionEntity(ExpressionUtils.expression[TimestampDiff]("TIMESTAMPDIFF")),
    FunctionEntity(ExpressionUtils.expression[Truncate]("TRUNCATE")),
    FunctionEntity(ExpressionUtils.expression[DictEncode]("DICTENCODE")),
    FunctionEntity(ExpressionUtils.expression[SplitPart]("split_part")))
}

case class FunctionEntity(name: FunctionIdentifier,
                          info: ExpressionInfo,
                          builder: FunctionBuilder)

case class TRUNCATE(child: Expression, scale: Expression)
        extends RoundBase(child, scale, BigDecimal.RoundingMode.DOWN, "DOWN")
                with Serializable with ImplicitCastInputTypes {
  def this(child: Expression) = this(child, Literal(0))
}

// scalastyle:on line.size.limit
case class KapSubtractMonths(a: Expression, b: Expression)
        extends BinaryExpression
                with ImplicitCastInputTypes {

  override def left: Expression = a

  override def right: Expression = b

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, DateType)

  override def dataType: DataType = IntegerType

  override def nullSafeEval(date0: Any, date1: Any): Any = {
    KapDateTimeUtils.dateSubtractMonths(date0.asInstanceOf[Int],
      date1.asInstanceOf[Int])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = KapDateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (d0, d1) => {
      s"""$dtu.dateSubtractMonths($d0, $d1)"""
    })
  }

  override def prettyName: String = "kap_months_between"
}

case class KapDayOfWeek(a: Expression)
        extends UnaryExpression
                with ImplicitCastInputTypes {

  override def child: Expression = a

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {
    val dtu = KapDateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (d) => {
      s"""$dtu.dayOfWeek($d)"""
    })
  }

  override def nullSafeEval(date: Any): Any = {
    KapDateTimeUtils.dayOfWeek(date.asInstanceOf[Int])
  }

  override def dataType: DataType = IntegerType

  override def prettyName: String = "kap_day_of_week"
}

object FunctionEntity {
  def apply(tuple: (String, (ExpressionInfo, FunctionBuilder))): FunctionEntity = {
    new FunctionEntity(FunctionIdentifier.apply(tuple._1), tuple._2._1, tuple._2._2)
  }
}
