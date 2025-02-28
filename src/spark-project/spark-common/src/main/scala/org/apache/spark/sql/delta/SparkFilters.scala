/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.delta

import java.sql.{Date, Timestamp}
import java.util.regex.Pattern

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}

import scala.collection.JavaConverters._

import io.delta.standalone.expressions.{And => SAnd, EqualTo => SEqualTo, Expression => SExpression, GreaterThan => SGreaterThan, GreaterThanOrEqual => SGreaterThanOrEqual, In => SIn, IsNotNull => SIsNotNull, IsNull => SIsNull, LessThan => SLessThan, LessThanOrEqual => SLessThanOrEqual, Literal => SLiteral, Not => SNot, Or => SOr}
import io.delta.standalone.types.StructType

object SparkFilters {

  private val BACKTICKS_PATTERN = Pattern.compile("([`])(.|$)")

  def convert(schema: StructType, filters: Seq[Filter]): SExpression = {
    filters.foldLeft(SLiteral.True: SExpression) { (exprs, filter) =>
      new SAnd(exprs, convert(schema, filter))
    }
  }

  def convert(schema: StructType, filter: Filter): SExpression = filter match {
    case _ if filter.getClass == classOf[AlwaysTrue] => SLiteral.True
    case _ if filter.getClass == classOf[AlwaysFalse] => SLiteral.False
    case eq: EqualTo => new SEqualTo(schema.column(unquote(eq.attribute)), literalConvert(eq.value))
    case enq: EqualNullSafe => new SEqualTo(schema.column(unquote(enq.attribute)), literalConvert(enq.value))
    case isNull: IsNull => new SIsNull(schema.column(unquote(isNull.attribute)))
    case isNotNull: IsNotNull => new SIsNotNull(schema.column(unquote(isNotNull.attribute)))
    case lt: LessThan => new SLessThan(schema.column(unquote(lt.attribute)), literalConvert(lt.value))
    case lte: LessThanOrEqual => new SLessThanOrEqual(schema.column(unquote(lte.attribute)), literalConvert(lte.value))
    case gt: GreaterThan => new SGreaterThan(schema.column(unquote(gt.attribute)), literalConvert(gt.value))
    case gte: GreaterThanOrEqual => new SGreaterThanOrEqual(schema.column(unquote(gte.attribute)), literalConvert(gte.value))
    case in: In =>
      val inValues = in.values.map(v => literalConvert(v)).toList.asJava
      new SIn(schema.column(unquote(in.attribute)), inValues)
    case and: And =>
      val left = convert(schema, and.left)
      val right = convert(schema, and.right)
      if (left != null && right != null) new SAnd(left, right) else null
    case or: Or =>
      val left = convert(schema, or.left)
      val right = convert(schema, or.right)
      if (left != null && right != null) new SOr(left, right) else null
    case not: Not =>
      if (not.child.isInstanceOf[StringContains]) {
        SLiteral.True
      } else {
        new SNot(convert(schema, not.child))
      }
    case unsupportedFilter =>
      // return 'true' to scan all partitions
      // currently unsupported filters are:
      // - StringStartsWith
      // - StringEndsWith
      // - StringContains
      // - EqualNullSafe
      SLiteral.True
  }

  private def unquote(attributeName: String) = {
    val matcher = BACKTICKS_PATTERN.matcher(attributeName)
    matcher.replaceAll("$2")
  }

  private def literalConvert(value: Any) = {
    val literal = Literal.apply(value)
    literal.dataType match {
      case IntegerType => SLiteral.of(value.asInstanceOf[Int])
      case LongType => SLiteral.of(value.asInstanceOf[Long])
      case DoubleType => SLiteral.of(value.asInstanceOf[Double])
      case FloatType => SLiteral.of(value.asInstanceOf[Float])
      case ByteType => SLiteral.of(value.asInstanceOf[Byte])
      case ShortType => SLiteral.of(value.asInstanceOf[Short])
      case StringType => SLiteral.of(value.asInstanceOf[String])
      case BooleanType => SLiteral.of(value.asInstanceOf[Boolean])
      case DateType => SLiteral.of(value.asInstanceOf[Date])
      case TimestampType => SLiteral.of(value.asInstanceOf[Timestamp])
      case _: DecimalType => SLiteral.of(value.asInstanceOf[java.math.BigDecimal])
    }
  }
}
