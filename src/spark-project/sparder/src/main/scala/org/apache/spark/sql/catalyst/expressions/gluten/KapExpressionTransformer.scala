/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.expressions.gluten

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression._
import org.apache.gluten.extension.ExpressionExtensionTrait
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types._
import org.apache.spark.sql.udaf._

import scala.collection.mutable.ListBuffer

class KeBitmapFunctionTransformer(val substraitExprName: String,
                                  val child: ExpressionTransformer,
                                  val original: Expression) extends UnaryExpressionTransformer with Logging {
}

class FloorDateTimeTransformer(val left: ExpressionTransformer,
                               val right: ExpressionTransformer,
                               val original: FloorDateTime) extends ExpressionTransformer with Logging {

  override def substraitExprName: String = "date_trunc"

  override def children: Seq[ExpressionTransformer] = {
    if (original.timeZoneId.isDefined) {
      Seq(right, left, LiteralTransformer(original.timeZoneId.get))
    } else {
      Seq(right, left)
    }
  }

}

case class CustomerExpressionTransformer() extends ExpressionExtensionTrait {

  /** Generate the extension expressions list, format: Sig[XXXExpression]("XXXExpressionName") */
  def expressionSigList: Seq[Sig] = Seq(
    Sig[FloorDateTime]("date_trunc"),
    Sig[CeilDateTime]("ceil_datetime"),
    Sig[KapAddMonths]("kap_add_months"),
    Sig[KapSubtractMonths]("kap_months_between"),
    Sig[YMDintBetween]("kap_ymd_int_between"),
    Sig[Truncate]("truncate"),
    Sig[KylinSplitPart]("kylin_split_part"),
    Sig[KylinInstr]("kylin_instr"),
    Sig[PreciseCardinality]("ke_bitmap_cardinality"),
    Sig[PreciseCountDistinctDecode]("ke_bitmap_cardinality"),
    Sig[ReusePreciseCountDistinct]("ke_bitmap_or_data"),
    Sig[PreciseCountDistinctAndValue]("ke_bitmap_and_value"),
    Sig[PreciseCountDistinctAndArray]("ke_bitmap_and_ids"),
    Sig[PreciseCountDistinct]("ke_bitmap_or_cardinality"),
    Sig[KylinTimestampAdd]("kylin_timestamp_add"),
    Sig[Sum0]("sum0")
  )

  /** Replace extension expression to transformer. */
  override def replaceWithExtensionExpressionTransformer(
                                                          substraitExprName: String,
                                                          expr: Expression,
                                                          attributeSeq: Seq[Attribute]): ExpressionTransformer = expr match {
    case preciseCardinality: PreciseCardinality =>
      new KeBitmapFunctionTransformer(
        substraitExprName,
        ExpressionConverter
          .replaceWithExpressionTransformer(preciseCardinality.child, attributeSeq),
        preciseCardinality
      )
    case preciseCountDistinctDecode: PreciseCountDistinctDecode =>
      new KeBitmapFunctionTransformer(
        substraitExprName,
        ExpressionConverter
          .replaceWithExpressionTransformer(preciseCountDistinctDecode.child, attributeSeq),
        preciseCountDistinctDecode
      )
    case kylinTimestampAdd: KylinTimestampAdd =>
      new TimestampAddTransformer(
        ExpressionConverter
          .replaceWithExpressionTransformer(kylinTimestampAdd.left, attributeSeq),
        ExpressionConverter
          .replaceWithExpressionTransformer(kylinTimestampAdd.mid, attributeSeq),
        ExpressionConverter
          .replaceWithExpressionTransformer(kylinTimestampAdd.right, attributeSeq),
        kylinTimestampAdd
      )
    case ceilDateTime: CeilDateTime =>
      val floorTime = FloorDateTime(ceilDateTime.left, ceilDateTime.right, ceilDateTime.timeZoneId)
      val floorAddUnitTime = KylinTimestampAdd(ceilDateTime.right, Literal(1L), floorTime)
      val equalsExp = If(EqualTo(ceilDateTime.left, floorTime), floorTime, floorAddUnitTime)
      ExpressionConverter.replaceWithExpressionTransformer(equalsExp, attributeSeq)
    case floorDateTime: FloorDateTime =>
      new FloorDateTimeTransformer(
        ExpressionConverter.replaceWithExpressionTransformer(floorDateTime.left, attributeSeq),
        ExpressionConverter.replaceWithExpressionTransformer(floorDateTime.right, attributeSeq),
        floorDateTime
      )
    case kylinSplitPart: KylinSplitPart if kylinSplitPart.second.isInstanceOf[Literal] =>
      new GenericExpressionTransformer(
        substraitExprName,
        Seq(
          ExpressionConverter.replaceWithExpressionTransformer(kylinSplitPart.first, attributeSeq),
          LiteralTransformer(kylinSplitPart.second.asInstanceOf[Literal]),
          ExpressionConverter.replaceWithExpressionTransformer(kylinSplitPart.third, attributeSeq)
        ),
        kylinSplitPart
      )
    case kylinInstr: KylinInstr =>
      val stringLocate = StringLocate(kylinInstr.second, kylinInstr.first, kylinInstr.third)
      ExpressionConverter.replaceWithExpressionTransformer(stringLocate, attributeSeq)
    case kapAddMonths: KapAddMonths =>
      val addMonths = KylinTimestampAdd(Literal("month"), kapAddMonths.right, kapAddMonths.left)
      val equalsExp = If(EqualTo(kapAddMonths.left, LastDay(kapAddMonths.left)), LastDay(addMonths), addMonths)
      ExpressionConverter.replaceWithExpressionTransformer(equalsExp, attributeSeq)

    case kapSubtractMonths: KapSubtractMonths =>
      GenericExpressionTransformer("kap_months_between",
        Seq(
          ExpressionConverter.replaceWithExpressionTransformer(kapSubtractMonths.right, attributeSeq),
          ExpressionConverter.replaceWithExpressionTransformer(kapSubtractMonths.left, attributeSeq)),
        kapSubtractMonths)
    case kapYmdIntBetween: YMDintBetween =>
      GenericExpressionTransformer("kap_ymd_int_between",
        Seq(
          ExpressionConverter.replaceWithExpressionTransformer(kapYmdIntBetween.left, attributeSeq),
          ExpressionConverter.replaceWithExpressionTransformer(kapYmdIntBetween.right, attributeSeq)),
        kapYmdIntBetween)
    case truncate: Truncate =>
      GenericExpressionTransformer(
        "truncate",
        Seq(
          ExpressionConverter.replaceWithExpressionTransformer(truncate.left, attributeSeq),
          ExpressionConverter.replaceWithExpressionTransformer(truncate.right, attributeSeq)),
        truncate)
    case _ =>
      throw new UnsupportedOperationException(
        s"${expr.getClass} or $expr is not currently supported.")
  }

  override def getAttrsIndexForExtensionAggregateExpr(
                                                       aggregateFunc: AggregateFunction,
                                                       mode: AggregateMode,
                                                       exp: AggregateExpression,
                                                       aggregateAttributeList: Seq[Attribute],
                                                       aggregateAttr: ListBuffer[Attribute],
                                                       resIndex: Int): Int = {
    var resIdx = resIndex
    exp.mode match {
      case Partial | PartialMerge =>
        val aggBufferAttr = aggregateFunc.inputAggBufferAttributes
        for (index <- aggBufferAttr.indices) {
          val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
          aggregateAttr += attr
        }
        resIdx += aggBufferAttr.size
        resIdx
      case Final | Complete =>
        aggregateAttr += aggregateAttributeList(resIdx)
        resIdx += 1
        resIdx
      case other => throw new GlutenNotSupportException(s"Unsupported aggregate mode: $other.")
    }
  }

  override def buildCustomAggregateFunction(
                                             aggregateFunc: AggregateFunction): (Option[String], Seq[DataType]) = {
    val substraitAggFuncName = aggregateFunc match {
      case countDistinct: PreciseCountDistinct =>
        countDistinct.dataType match {
          case LongType =>
            Some("ke_bitmap_or_cardinality")
          case BinaryType =>
            Some("ke_bitmap_or_data")
          case _ => throw new UnsupportedOperationException("Unsupported data type in count distinct")
        }
      case _ =>
        extensionExpressionsMapping.get(aggregateFunc.getClass)
    }
    assert(substraitAggFuncName.isDefined, s"Aggregate function ${aggregateFunc.getClass} is not supported.")
    (substraitAggFuncName, aggregateFunc.children.map(child => child.dataType))
  }
}
