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

import org.apache.spark.dict.{NBucketDictionary, NGlobalDictionaryV2}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, GenericArrayData, KapDateTimeUtils}
import org.apache.spark.sql.connector.read.sqlpushdown.NotSupportPushDown
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.udf._

import java.time.ZoneId
import java.util.Locale
import scala.collection.JavaConverters._

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
case class KapAddMonths(startDate: Expression, numMonths: Expression)
  extends BinaryExpression
    with ImplicitCastInputTypes
    with NotSupportPushDown {

  override def left: Expression = startDate

  override def right: Expression = numMonths

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, IntegerType)

  override def dataType: TimestampType = TimestampType

  override def nullSafeEval(start: Any, months: Any): Any = {
    val time = start.asInstanceOf[Long]
    val month = months.asInstanceOf[Int]
    KapDateTimeUtils.addMonths(time, month)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = KapDateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, m) => {
      s"""$dtu.addMonths($sd, $m)"""
    })
  }

  override def prettyName: String = "kap_add_months"

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    val newChildren = Seq(newLeft, newRight)
    super.legacyWithNewChildren(newChildren)
  }
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
case class KapSubtractMonths(a: Expression, b: Expression)
  extends BinaryExpression
    with ImplicitCastInputTypes
    with NotSupportPushDown {

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

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    val newChildren = Seq(newLeft, newRight)
    super.legacyWithNewChildren(newChildren)
  }

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

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}

case class KapDayOfWeek(a: Expression)
  extends UnaryExpression
    with ImplicitCastInputTypes
    with NotSupportPushDown {

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

  override protected def withNewChildInternal(newChild: Expression): KapDayOfWeek =
    copy(a = newChild)
}

case class TimestampAdd(left: Expression, mid: Expression, right: Expression) extends TernaryExpression with ExpectsInputTypes {

  override def dataType: DataType = getResultDataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringType, TypeCollection(IntegerType, LongType), TypeCollection(DateType, TimestampType))

  def getResultDataType(): DataType = {
    if (canConvertTimestamp()) {
      TimestampType
    } else {
      right.dataType
    }
  }

  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    (mid.dataType, right.dataType) match {
      case (IntegerType, DateType) =>
        if (canConvertTimestamp()) {
          TimestampAddImpl.evaluateTimestamp(input1.toString, input2.asInstanceOf[Int], input3.asInstanceOf[Int])
        } else {
          TimestampAddImpl.evaluateDays(input1.toString, input2.asInstanceOf[Int], input3.asInstanceOf[Int])
        }
      case (LongType, DateType) =>
        if (canConvertTimestamp()) {
          TimestampAddImpl.evaluateTimestamp(input1.toString, input2.asInstanceOf[Long], input3.asInstanceOf[Int])
        } else {
          TimestampAddImpl.evaluateDays(input1.toString, input2.asInstanceOf[Long], input3.asInstanceOf[Int])
        }
      case (IntegerType, TimestampType) =>
        TimestampAddImpl.evaluateTimestamp(input1.toString, input2.asInstanceOf[Int], input3.asInstanceOf[Long])
      case (LongType, TimestampType) =>
        TimestampAddImpl.evaluateTimestamp(input1.toString, input2.asInstanceOf[Long], input3.asInstanceOf[Long])
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val ta = TimestampAddImpl.getClass.getName.stripSuffix("$")
    (mid.dataType, right.dataType) match {
      case ((IntegerType, DateType) | (LongType, DateType)) =>
        if (canConvertTimestamp()) {
          defineCodeGen(ctx, ev, (arg1, arg2, arg3) => {
            s"""$ta.evaluateTimestamp($arg1.toString(), $arg2, $arg3)"""
          })
        } else {
          defineCodeGen(ctx, ev, (arg1, arg2, arg3) => {
            s"""$ta.evaluateDays($arg1.toString(), $arg2, $arg3)"""
          })
        }
      case (IntegerType, TimestampType) | (LongType, TimestampType) =>
        defineCodeGen(ctx, ev, (arg1, arg2, arg3) => {
          s"""$ta.evaluateTimestamp($arg1.toString(), $arg2, $arg3)"""
        })
    }
  }

  override def first: Expression = left

  override def second: Expression = mid

  override def third: Expression = right

  def canConvertTimestamp(): Boolean = {
    if (left.isInstanceOf[Literal] && left.asInstanceOf[Literal].value != null) {
      val unit = left.asInstanceOf[Literal].value.toString.toUpperCase(Locale.ROOT)
      if (TimestampAddImpl.TIME_UNIT.contains(unit) && right.dataType.isInstanceOf[DateType]) {
        return true
      }
    }
    false
  }

  override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = {
    val newChildren = Seq(newFirst, newSecond, newThird)
    super.legacyWithNewChildren(newChildren)
  }
}

case class TimestampDiff(left: Expression, mid: Expression, right: Expression) extends TernaryExpression
  with ExpectsInputTypes
  with NotSupportPushDown {

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

  override def first: Expression = left

  override def second: Expression = mid

  override def third: Expression = right

  override def dataType: DataType = LongType

  override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = {
    val newChildren = Seq(newFirst, newSecond, newThird)
    super.legacyWithNewChildren(newChildren)
  }
}

case class Truncate(_left: Expression, _right: Expression) extends BinaryExpression with ExpectsInputTypes {
  override def left: Expression = _left

  override def right: Expression = _right

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(IntegerType, LongType, DoubleType, DecimalType, IntegerType, FloatType, ShortType, ByteType), IntegerType)

  def this(exp: Expression) = this(exp, Literal(0, IntegerType))

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    val value2 = input2.asInstanceOf[Int]
    left.dataType match {
      case IntegerType => TruncateImpl.evaluate(input1.asInstanceOf[Int], value2)
      case DoubleType => TruncateImpl.evaluate(input1.asInstanceOf[Double], value2)
      case FloatType => TruncateImpl.evaluate(input1.asInstanceOf[Float], value2)
      case ShortType => TruncateImpl.evaluate(input1.asInstanceOf[Short], value2)
      case ByteType => TruncateImpl.evaluate(input1.asInstanceOf[Byte], value2)
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

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    val newChildren = Seq(newLeft, newRight)
    super.legacyWithNewChildren(newChildren)
  }
}

case class DictEncode(left: Expression, mid: Expression, right: Expression) extends TernaryExpression with ExpectsInputTypes {

  def maxFields: Int = SQLConf.get.maxToStringFields

  override def first: Expression = left

  override def second: Expression = mid

  override def third: Expression = right

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, StringType, StringType)

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {
    val globalDictClass = classOf[NGlobalDictionaryV2].getName
    val bucketDictClass = classOf[NBucketDictionary].getName
    val globalDictTerm = ctx.addMutableState(globalDictClass,
      s"${
        mid.simpleString(maxFields)
          .replace("[", "").replace("]", "")
      }_globalDict")
    val bucketDictTerm = ctx.addMutableState(bucketDictClass,
      s"${
        mid.simpleString(maxFields)
          .replace("[", "").replace("]", "")
      }_bucketDict")

    val dictParamsTerm = mid.simpleString(maxFields)
    val bucketSizeTerm = right.simpleString(maxFields).toInt

    val initBucketDictFuncName = ctx.addNewFunction(s"init${bucketDictTerm.replace("[", "").replace("]", "")}BucketDict",
      s"""
         | private void init${bucketDictTerm.replace("[", "").replace("]", "")}BucketDict(int idx) {
         |   try {
         |     int bucketId = idx % $bucketSizeTerm;
         |     $globalDictTerm = new org.apache.spark.dict.NGlobalDictionaryV2("$dictParamsTerm");
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

  override def prettyName: String = "DICTENCODE"

  override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = {
    val newChildren = Seq(newFirst, newSecond, newThird)
    super.legacyWithNewChildren(newChildren)
  }
}


case class SplitPart(left: Expression, mid: Expression, right: Expression) extends TernaryExpression with ExpectsInputTypes {

  override def dataType: DataType = left.dataType

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)

  override def first: Expression = left

  override def second: Expression = mid

  override def third: Expression = right

  override protected def nullSafeEval(input1: Any, input2: Any, input3: Any): Any = {
    SplitPartImpl.evaluate(input1.toString, input2.toString, input3.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val ta = SplitPartImpl.getClass.getName.stripSuffix("$")
    nullSafeCodeGen(ctx, ev, (arg1, arg2, arg3) => {
      s"""
          org.apache.spark.unsafe.types.UTF8String result = $ta.evaluate($arg1.toString(), $arg2.toString(), $arg3);
          if (result == null) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = result;
          }
        """
    })
  }

  override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = {
    val newChildren = Seq(newFirst, newSecond, newThird)
    super.legacyWithNewChildren(newChildren)
  }
}

case class FloorDateTime(timestamp: Expression,
                         format: Expression,
                         timeZoneId: Option[String] = None)
  extends TruncInstant with TimeZoneAwareExpression {

  override def left: Expression = timestamp

  override def right: Expression = format

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)

  override def dataType: TimestampType = TimestampType

  override def prettyName: String = "floor_datetime"

  override val instant = timestamp

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  def this(timestamp: Expression, format: Expression) = this(timestamp, format, None)

  override def eval(input: InternalRow): Any = {
    evalHelper(input, minLevel = DateTimeUtils.TRUNC_TO_SECOND) { (t: Any, level: Int) =>
      DateTimeUtils.truncTimestamp(t.asInstanceOf[Long], level, zoneId)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val tz = ctx.addReferenceObj("timeZone", zoneId, classOf[ZoneId].getName)
    codeGenHelper(ctx, ev, minLevel = DateTimeUtils.TRUNC_TO_SECOND, true) {
      (date: String, fmt: String) =>
        s"truncTimestamp($date, $fmt, $tz);"
    }
  }

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    val newChildren = Seq(newLeft, newRight)
    super.legacyWithNewChildren(newChildren)
  }
}

case class CeilDateTime(timestamp: Expression,
                        format: Expression,
                        timeZoneId: Option[String] = None)
  extends TruncInstant with TimeZoneAwareExpression {

  override def left: Expression = timestamp

  override def right: Expression = format

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)

  override def dataType: TimestampType = TimestampType

  override def prettyName: String = "ceil_datetime"

  override val instant = timestamp

  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
    copy(timeZoneId = Option(timeZoneId))

  def this(timestamp: Expression, format: Expression) = this(timestamp, format, None)

  // scalastyle:off
  override def eval(input: InternalRow): Any = {
    evalHelper(input, minLevel = DateTimeUtils.TRUNC_TO_SECOND) { (t: Any, level: Int) =>
      DateTimeUtils.ceilTimestamp(t.asInstanceOf[Long], level, zoneId)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val zid = ctx.addReferenceObj("zoneId", zoneId, classOf[ZoneId].getName)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (date, fmt) => {
      s"""$dtu.ceilTimestamp($date, $fmt, $zid)"""
    })
  }

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    val newChildren = Seq(newLeft, newRight)
    super.legacyWithNewChildren(newChildren)
  }
}

case class IntersectCountByCol(childrenExp: Seq[Expression]) extends Expression {
  override def nullable: Boolean = false

  override def children: Seq[Expression] = childrenExp

  override def eval(input: InternalRow): Long = {
    val array = children.map(_.eval(input).asInstanceOf[Array[Byte]]).toList.asJava
    IntersectCountByColImpl.evaluate(array)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val codes = children.map(_.genCode(ctx))
    val list = ctx.addMutableState("java.util.List<Byte[]>", s"bytesList",
      v => s"$v = new java.util.LinkedList();", forceInline = true)

    val ic = IntersectCountByColImpl.getClass.getName.stripSuffix("$")

    val builder = new StringBuilder()
    builder.append(s"$list.clear();\n")
    codes.map(_.value).foreach { code =>
      builder.append(s"$list.add($code);\n")
    }

    val resultCode =
      s"""
         ${builder.toString()}
         ${ev.value} = $ic.evaluate($list);"""

    builder.clear()
    codes.map(_.code).foreach { code =>
      builder.append(s"${code.code}\n")
    }

    ev.copy(code =
      code"""
        ${builder.toString()}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""", isNull = FalseLiteral)
  }

  override def dataType: DataType = LongType

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    super.legacyWithNewChildren(newChildren)
}

case class SubtractBitmapUUID(child1: Expression, child2: Expression) extends BinaryExpression with ExpectsInputTypes {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Array[Byte] = {
    val map1 = child1.eval(input).asInstanceOf[Array[Byte]]
    val map2 = child2.eval(input).asInstanceOf[Array[Byte]]
    SubtractBitmapImpl.evaluate2Bytes(map1, map2)
  }

  override def dataType: DataType = BinaryType

  override def left: Expression = child1

  override def right: Expression = child2

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sb = SubtractBitmapImpl.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (arg1, arg2) => {
      s"""$sb.evaluate2Bytes($arg1, $arg2)"""
    })
  }

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    val newChildren = Seq(newLeft, newRight)
    super.legacyWithNewChildren(newChildren)
  }
}

case class SubtractBitmapValue(child1: Expression, child2: Expression, upperBound: Int) extends BinaryExpression with ExpectsInputTypes {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): GenericArrayData = {
    val map1 = child1.eval(input).asInstanceOf[Array[Byte]]
    val map2 = child2.eval(input).asInstanceOf[Array[Byte]]
    SubtractBitmapImpl.evaluate2Values(map1, map2, upperBound)
  }

  override def dataType: DataType = ArrayType.apply(LongType)

  override def left: Expression = child1

  override def right: Expression = child2

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sb = SubtractBitmapImpl.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (arg1, arg2) => {
      s"""$sb.evaluate2Values($arg1, $arg2, $upperBound)"""
    })
  }

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    val newChildren = Seq(newLeft, newRight)
    super.legacyWithNewChildren(newChildren)
  }
}

case class PreciseCountDistinctDecode(_child: Expression)
  extends UnaryExpression with ExpectsInputTypes {

  override def child: Expression = _child

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expressionUtils = ExpressionUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (bytes) => {
      s"""$expressionUtils.preciseCountDistinctDecodeHelper($bytes)"""
    })
  }

  override protected def nullSafeEval(bytes: Any): Any = {
    ExpressionUtils.preciseCountDistinctDecodeHelper(bytes)
  }

  override def eval(input: InternalRow): Any = {
    if (input != null) {
      super.eval(input)
    } else {
      0L
    }
  }

  override def dataType: DataType = LongType

  override def prettyName: String = "precise_count_distinct_decode"

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(_child = newChild)
}

case class ApproxCountDistinctDecode(expr: Expression, precision: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  def left: Expression = expr

  def right: Expression = precision

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, IntegerType)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expressionUtils = ExpressionUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (bytes, precision) => {
      s"""$expressionUtils.approxCountDistinctDecodeHelper($bytes, $precision)"""
    })
  }

  override protected def nullSafeEval(bytes: Any, precision: Any): Any = {
    ExpressionUtils.approxCountDistinctDecodeHelper(bytes, precision)
  }

  override def eval(input: InternalRow): Any = {
    if (input != null) {
      super.eval(input)
    } else {
      0L
    }
  }

  override def dataType: DataType = LongType

  override def prettyName: String = "approx_count_distinct_decode"

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    val newChildren = Seq(newLeft, newRight)
    super.legacyWithNewChildren(newChildren)
  }
}

case class PercentileDecode(bytes: Expression, quantile: Expression, precision: Expression) extends TernaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, DecimalType, IntegerType)

  override def first: Expression = bytes

  override def second: Expression = quantile

  override def third: Expression = precision

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expressionUtils = ExpressionUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (bytes, quantile, precision) => {
      s"""$expressionUtils.percentileDecodeHelper($bytes, $quantile, $precision)"""
    })
  }

  override protected def nullSafeEval(bytes: Any, quantile: Any, precision: Any): Any = {
    ExpressionUtils.percentileDecodeHelper(bytes, quantile, precision)
  }

  override def dataType: DataType = DoubleType

  override def prettyName: String = "percentile_decode"

  override def nullable: Boolean = false

  override protected def withNewChildrenInternal(newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = {
    val newChildren = Seq(newFirst, newSecond, newThird)
    super.legacyWithNewChildren(newChildren)
  }
}