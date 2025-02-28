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

import org.apache.kylin.engine.spark.utils.LogEx
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.sql.udf.SubtractBitmapImpl

@ExpressionDescription(usage = "SubtractBitmap(expr)")
@SerialVersionUID(1)
sealed abstract class SubtractBitmap(x: Expression, y: Expression)
  extends BinaryExpression with ExpectsInputTypes with Serializable with LogEx {

  override def nullable: Boolean = false

  override def left: Expression = x

  override def right: Expression = y

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType)

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    val newChildren = Seq(newLeft, newRight)
    super.legacyWithNewChildren(newChildren)
  }

  def convertToArray(x: Expression, y: Expression, input: InternalRow): (Array[Byte], Array[Byte]) = {
    val map1 = x.eval(input).asInstanceOf[Array[Byte]]
    val map2 = y.eval(input).asInstanceOf[Array[Byte]]
    (map1, map2)
  }
}

@SerialVersionUID(1)
case class SubtractBitmapUUID(x: Expression, y: Expression) extends SubtractBitmap(x, y) {

  override def eval(input: InternalRow): Array[Byte] = {
    val (map1, map2) = convertToArray(x, y, input)
    SubtractBitmapImpl.evaluate2Bytes(map1, map2)
  }

  override def dataType: DataType = BinaryType

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sb = SubtractBitmapImpl.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (arg1, arg2) => {
      s"""$sb.evaluate2Bytes($arg1, $arg2)"""
    })
  }
}

@SerialVersionUID(1)
case class SubtractBitmapAllValues(x: Expression, y: Expression, upperBound: Int) extends SubtractBitmap(x, y) {

  override def eval(input: InternalRow): GenericArrayData = {
    val (map1, map2) = convertToArray(x, y, input)
    SubtractBitmapImpl.evaluate2AllValues(map1, map2, upperBound)
  }

  override def dataType: DataType = ArrayType.apply(LongType)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sb = SubtractBitmapImpl.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (arg1, arg2) => {
      s"""$sb.evaluate2AllValues($arg1, $arg2, $upperBound)"""
    })
  }
}

@SerialVersionUID(1)
case class SubtractBitmapCount(x: Expression, y: Expression) extends SubtractBitmap(x, y) {

  override def eval(input: InternalRow): Int = {
    val (map1, map2) = convertToArray(x, y, input)
    SubtractBitmapImpl.evaluate2Count(map1, map2)
  }

  override def dataType: DataType = IntegerType

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sb = SubtractBitmapImpl.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (arg1, arg2) => {
      s"""$sb.evaluate2Count($arg1, $arg2)"""
    })
  }
}

@SerialVersionUID(1)
case class SubtractBitmapValue(x: Expression, y: Expression, limit: Int, offset: Int, upperBound: Int)
  extends SubtractBitmap(x, y) {

  override def eval(input: InternalRow): GenericArrayData = {
    val (map1, map2) = convertToArray(x, y, input)
    SubtractBitmapImpl.evaluate2Values(map1, map2, limit, offset, upperBound)
  }

  override def dataType: DataType = ArrayType.apply(LongType)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sb = SubtractBitmapImpl.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (arg1, arg2) => {
      s"""$sb.evaluate2Values($arg1, $arg2, $limit, $offset, $upperBound)"""
    })
  }
}
