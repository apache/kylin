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

import java.time.ZoneId
import java.util.Locale

import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.expression.{ConverterUtils, ExpressionTransformer}
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.kylin.guava30.shaded.common.collect.Lists
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{KylinTimestampAdd, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.sql.udf.TimestampAddImpl

class TimestampAddTransformer(val left: ExpressionTransformer,
                              val mid: ExpressionTransformer,
                              val right: ExpressionTransformer,
                              val original: KylinTimestampAdd) extends ExpressionTransformer with Logging {
  override def doTransform(args: Object): ExpressionNode = {
    val unitNode = left.doTransform(args)
    val increaseNode = mid.doTransform(args)
    val fromNode = right.doTransform(args)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName("timestamp_add", Seq(), FunctionConfig.REQ)
    )

    val expressionNodes = Lists.newArrayList(
      unitNode,
      increaseNode,
      fromNode,
      ExpressionBuilder.makeStringLiteral(ZoneId.systemDefault().toString))

    val outputType = ConverterUtils.getTypeNode(getResultDataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, outputType)
  }

  def canConvertTimestamp: Boolean = {
    original.left match {
      case literal: Literal if literal.value != null =>
        val unit = literal.value.toString.toUpperCase(Locale.ROOT)
        if (TimestampAddImpl.TIME_UNIT.contains(unit) && original.right.dataType.isInstanceOf[DateType]) {
          return true
        }
      case _ =>
    }
    false
  }


  def getResultDataType: DataType = {
    if (canConvertTimestamp) {
      TimestampType
    } else {
      original.right.dataType
    }
  }

  override def substraitExprName: String = "timestamp_add"

  override def children: Seq[ExpressionTransformer] = {
    Seq(left, mid, right)
  }
}
