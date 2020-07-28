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
package org.apache.spark.sql.catalyst.expressions

import org.apache.kylin.engine.spark.common.util.KylinDateTimeUtils
import org.apache.spark.dict.{NBucketDictionary, NGlobalDictionary}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._

// Returns the date that is num_months after start_date.
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage =
    "_FUNC_(start_date, num_months) - Returns the date that is `num_months` after `start_date`.",
  extended =
    """
    Examples:
      > SELECT _FUNC_('2016-08-31', 1);
       2016-09-30
  """
)
// scalastyle:on line.size.limit
case class KylinAddMonths(startDate: Expression, numMonths: Expression)
  extends BinaryExpression
    with ImplicitCastInputTypes {

  override def left: Expression = startDate

  override def right: Expression = numMonths

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, IntegerType)

  override def dataType: TimestampType = TimestampType

  override def nullSafeEval(start: Any, months: Any): Any = {
    val time = start.asInstanceOf[Long]
    val month = months.asInstanceOf[Int]
    KylinDateTimeUtils.addMonths(time, month)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = KylinDateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, m) => {
      s"""$dtu.addMonths($sd, $m)"""
    })
  }

  override def prettyName: String = "kylin_add_months"
}

// Returns the date that is num_months after start_date.
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage =
    "_FUNC_(date0, date1) - Returns the num of months between `date0` after `date1`.",
  extended =
    """
    Examples:
      > SELECT _FUNC_('2016-08-31', '2017-08-31');
       12
  """
)
// scalastyle:on line.size.limit
case class KylinSubtractMonths(a: Expression, b: Expression)
  extends BinaryExpression
    with ImplicitCastInputTypes {

  override def left: Expression = a

  override def right: Expression = b

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, DateType)

  override def dataType: DataType = IntegerType

  override def nullSafeEval(date0: Any, date1: Any): Any = {
    KylinDateTimeUtils.dateSubtractMonths(date0.asInstanceOf[Int],
      date1.asInstanceOf[Int])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = KylinDateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (d0, d1) => {
      s"""$dtu.dateSubtractMonths($d0, $d1)"""
    })
  }

  override def prettyName: String = "kylin_months_between"
}

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the sum calculated from values of a group. " +
    "It differs in that when no non null values are applied zero is returned instead of null")
case class Sum0(child: Expression)
  extends DeclarativeAggregate
    with ImplicitCastInputTypes {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function sum")

  private lazy val resultType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision + 10, scale)
    case _: IntegralType => LongType
    case _ => DoubleType
  }

  private lazy val sumDataType = resultType

  private lazy val sum = AttributeReference("sum", sumDataType)()

  private lazy val zero = Cast(Literal(0), sumDataType)

  override lazy val aggBufferAttributes = sum :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    //    /* sum = */ Literal.create(0, sumDataType)
    //    /* sum = */ Literal.create(null, sumDataType)
    Cast(Literal(0), sumDataType)
  )

  override lazy val updateExpressions: Seq[Expression] = {
    if (child.nullable) {
      Seq(
        /* sum = */
        Coalesce(
          Seq(Add(Coalesce(Seq(sum, zero)), Cast(child, sumDataType)), sum))
      )
    } else {
      Seq(
        /* sum = */
        Add(Coalesce(Seq(sum, zero)), Cast(child, sumDataType))
      )
    }
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* sum = */
      Coalesce(Seq(Add(Coalesce(Seq(sum.left, zero)), sum.right), sum.left))
    )
  }

  override lazy val evaluateExpression: Expression = sum
}

case class KylinDayOfWeek(a: Expression)
  extends UnaryExpression
    with ImplicitCastInputTypes {

  override def child: Expression = a

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override protected def doGenCode(
    ctx: CodegenContext,
    ev: ExprCode): ExprCode = {
    val dtu = KylinDateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (d) => {
      s"""$dtu.dayOfWeek($d)"""
    })
  }

  override def nullSafeEval(date: Any): Any = {
    KylinDateTimeUtils.dayOfWeek(date.asInstanceOf[Int])
  }

  override def dataType: DataType = IntegerType

  override def prettyName: String = "kylin_day_of_week"
}

case class TimestampAdd(left: Expression, mid: Expression, right: Expression) extends TernaryExpression with ExpectsInputTypes {

  override def dataType: DataType = right.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringType, TypeCollection(IntegerType, LongType), TypeCollection(DateType, TimestampType))


  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    (mid.dataType, right.dataType) match {
      case (IntegerType, DateType) => TimestampAddImpl.evaluate(input1.toString, input2.asInstanceOf[Int], input3.asInstanceOf[Int])
      case (LongType, DateType) =>
        TimestampAddImpl.evaluate(input1.toString, input2.asInstanceOf[Long], input3.asInstanceOf[Int])
      case (IntegerType, TimestampType) => TimestampAddImpl.evaluate(input1.toString, input2.asInstanceOf[Int], input3.asInstanceOf[Long])
      case (LongType, TimestampType) =>
        TimestampAddImpl.evaluate(input1.toString, input2.asInstanceOf[Long], input3.asInstanceOf[Long])
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val ta = TimestampAddImpl.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (arg1, arg2, arg3) => {
      s"""$ta.evaluate($arg1.toString(), $arg2, $arg3)"""
    })
  }

  override def children: Seq[Expression] = Seq(left, mid, right)
}

case class TimestampDiff(left: Expression, mid: Expression, right: Expression) extends TernaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringType, TypeCollection(DateType, TimestampType), TypeCollection(DateType, TimestampType))


  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    (mid.dataType, right.dataType) match {
      case (DateType, DateType) => TimestampDiffImpl.evaluate(input1.toString, input2.asInstanceOf[Int], input3.asInstanceOf[Int])
      case (DateType, TimestampType) => TimestampDiffImpl.evaluate(input1.toString, input2.asInstanceOf[Int], input3.asInstanceOf[Long])
      case (TimestampType, DateType) => TimestampDiffImpl.evaluate(input1.toString, input2.asInstanceOf[Long], input3.asInstanceOf[Int])
      case (TimestampType, TimestampType) =>
        TimestampDiffImpl.evaluate(input1.toString, input2.asInstanceOf[Long], input3.asInstanceOf[Long])
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val td = TimestampDiffImpl.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (arg1, arg2, arg3) => {
      s"""$td.evaluate($arg1.toString(), $arg2, $arg3)"""
    })
  }

  override def dataType: DataType = LongType

  override def children: Seq[Expression] = Seq(left, mid, right)
}

case class Truncate(_left: Expression, _right: Expression) extends BinaryExpression with ExpectsInputTypes {
  override def left: Expression = _left

  override def right: Expression = _right

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, LongType, DoubleType, DecimalType, IntegerType), IntegerType)


  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    val value2 = input2.asInstanceOf[Int]
    left.dataType match {
      case IntegerType => TruncateImpl.evaluate(input1.asInstanceOf[Int], value2)
      case DoubleType => TruncateImpl.evaluate(input1.asInstanceOf[Double], value2)
      case LongType => TruncateImpl.evaluate(input1.asInstanceOf[Long], value2)
      case DecimalType() => TruncateImpl.evaluate(input1.asInstanceOf[Decimal], value2)
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tr = TruncateImpl.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (arg1, arg2) => {
      s"""$tr.evaluate($arg1, $arg2)"""
    })
  }

  override def dataType: DataType = left.dataType
}

case class DictEncode(left: Expression, mid: Expression, right: Expression) extends TernaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, StringType, StringType)

  override protected def doGenCode(
    ctx: CodegenContext,
    ev: ExprCode): ExprCode = {
    val globalDictClass = classOf[NGlobalDictionary].getName
    val bucketDictClass = classOf[NBucketDictionary].getName

    val globalDictTerm = ctx.addMutableState(globalDictClass, s"${mid.simpleString.replace("[", "").replace("]", "")}_globalDict")
    val bucketDictTerm = ctx.addMutableState(bucketDictClass, s"${mid.simpleString.replace("[", "").replace("]", "")}_bucketDict")

    val dictParamsTerm = mid.simpleString
    val bucketSizeTerm = right.simpleString.toInt

    val initBucketDictFuncName = ctx.addNewFunction(s"init${bucketDictTerm.replace("[", "").replace("]", "")}BucketDict",
      s"""
         | private void init${bucketDictTerm.replace("[", "").replace("]", "")}BucketDict(int idx) {
         |   try {
         |     int bucketId = idx % $bucketSizeTerm;
         |     $globalDictTerm = new org.apache.spark.dict.NGlobalDictionary("$dictParamsTerm");
         |     $bucketDictTerm = $globalDictTerm.loadBucketDictionary(bucketId);
         |   } catch (Exception e) {
         |     throw new RuntimeException(e);
         |   }
         | }
        """.stripMargin)

    ctx.addPartitionInitializationStatement(s"$initBucketDictFuncName(partitionIndex);");

    defineCodeGen(ctx, ev, (arg1, arg2, arg3) => {
      s"""$bucketDictTerm.encode($arg1)"""
    })
  }

  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    DictEncodeImpl.evaluate(input1.toString, input2.toString, input3.toString)
  }

  override def eval(input: InternalRow): Any = {
    if (input != null) {
      super.eval(input)
    } else {
      0L
    }
  }

  override def dataType: DataType = LongType

  override def children: Seq[Expression] = Seq(left, mid, right)

  override def prettyName: String = "DICTENCODE"
}


case class SplitPart(left: Expression, mid: Expression, right: Expression) extends TernaryExpression with ExpectsInputTypes {

  override def dataType: DataType = left.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)

  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    SplitPartImpl.evaluate(input1.toString, input2.toString, input3.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val ta = SplitPartImpl.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (arg1, arg2, arg3) => {
      s"""$ta.evaluate($arg1.toString(), $arg2.toString(), $arg3)"""
    })
  }

  override def children: Seq[Expression] = Seq(left, mid, right)
}