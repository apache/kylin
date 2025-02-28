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

import java.nio.ByteBuffer

import org.apache.kylin.measure.hllc.HLLCounter
import org.apache.kylin.measure.percentile.PercentileSerializer
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.udaf.{BitmapSerAndDeSer, BitmapSerAndDeSerObj, SumLCUtil}
import org.roaringbitmap.longlong.Roaring64NavigableMap

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object ExpressionUtils {
  def expression[T <: Expression](name: String)
                                 (implicit tag: ClassTag[T]): (String, (ExpressionInfo, FunctionBuilder)) = {

    // For `RuntimeReplaceable`, skip the constructor with most arguments, which is the main
    // constructor and contains non-parameter `child` and should not be used as function builder.
    val constructors = if (classOf[RuntimeReplaceable].isAssignableFrom(tag.runtimeClass)) {
      val all = tag.runtimeClass.getConstructors
      val maxNumArgs = all.map(_.getParameterCount).max
      all.filterNot(_.getParameterCount == maxNumArgs)
    } else {
      tag.runtimeClass.getConstructors
    }
    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor = constructors.find(_.getParameterTypes.toSeq == Seq(classOf[Seq[_]]))
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        Try(varargCtor.get.newInstance(expressions).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) =>
            // the exception is an invocation exception. To get a meaningful message, we need the
            // cause.
            throw new AnalysisException(e.getCause.getMessage)
        }
      } else {
        // Otherwise, find a constructor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f = constructors.find(_.getParameterTypes.toSeq == params).getOrElse {
          val validParametersCount = constructors
            .filter(_.getParameterTypes.forall(_ == classOf[Expression]))
            .map(_.getParameterCount).distinct.sorted
          val expectedNumberOfParameters = if (validParametersCount.length == 1) {
            validParametersCount.head.toString
          } else {
            validParametersCount.init.mkString("one of ", ", ", " and ") +
              validParametersCount.last
          }
          throw new AnalysisException(s"Invalid number of arguments for function $name. " +
            s"Expected: $expectedNumberOfParameters; Found: ${params.length}")
        }
        Try(f.newInstance(expressions: _*).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) =>
            // the exception is an invocation exception. To get a meaningful message, we need the
            // cause.
            throw new AnalysisException(e.getCause.getMessage)
        }
      }
    }

    (name, (expressionInfo[T](name), builder))
  }

  private def expressionInfo[T <: Expression : ClassTag](name: String): ExpressionInfo = {
    val clazz = scala.reflect.classTag[T].runtimeClass
    val df = clazz.getAnnotation(classOf[ExpressionDescription])
    if (df != null) {
      if (df.extended().isEmpty) {
        new ExpressionInfo(
          clazz.getCanonicalName,
          null,
          name,
          df.usage(),
          df.arguments(),
          df.examples(),
          df.note(),
          "",
          df.since(),
          "",
          "")
      } else {
        // This exists for the backward compatibility with old `ExpressionDescription`s defining
        // the extended description in `extended()`.
        new ExpressionInfo(clazz.getCanonicalName, null, name, df.usage(), df.extended())
      }
    } else {
      new ExpressionInfo(clazz.getCanonicalName, name)
    }
  }

  def preciseCountDistinctDecodeHelper(bytes: Any): Long = {
    val storageBytes = bytes.asInstanceOf[Array[Byte]]
    val roaringMap = BitmapSerAndDeSerObj.deserialize(storageBytes)
    roaringMap.getLongCardinality
  }

  def approxCountDistinctDecodeHelper(bytes: Any, precision: Any): Long = {
    val storageFormat = bytes.asInstanceOf[Array[Byte]]
    val preciseValue = precision.asInstanceOf[Int]
    if (storageFormat.nonEmpty) {
      val counter = new HLLCounter(preciseValue)
      counter.readRegisters(ByteBuffer.wrap(storageFormat))
      counter.getCountEstimate
    } else {
      0L
    }
  }

  def percentileDecodeHelper(bytes: Any, quantile: Any, precision: Any): Double = {
    val arrayBytes = bytes.asInstanceOf[Array[Byte]]
    val serializer = new PercentileSerializer(precision.asInstanceOf[Int]);
    val counter = serializer.deserialize(ByteBuffer.wrap(arrayBytes))
    counter.getResultEstimateWithQuantileRatio(quantile.asInstanceOf[Decimal].toDouble)
  }

  def sumLCDecodeHelper(bytes: Any, wrapDataType: Any): Number = {
    val arrayBytes = bytes.asInstanceOf[Array[Byte]]
    val codec = SumLCUtil.getNumericNullSafeSerializerByDataType(DataType.fromJson(wrapDataType.toString))
    val counter = SumLCUtil.decodeToSumLCCounter(arrayBytes, codec)
    if (counter == null) {
      null
    } else {
      counter.getSumLC
    }
  }

  def bitmapUuidToArray(bytes: Any): GenericArrayData = {
    val bitmapbyte = bytes.asInstanceOf[Array[Byte]]
    val bitmap: Roaring64NavigableMap = BitmapSerAndDeSer.get().deserialize(bitmapbyte)
    val cardinality = bitmap.getIntCardinality
    val longs = new Array[Long](cardinality)
    var id = 0
    val iterator = bitmap.iterator()
    while (iterator.hasNext) {
      longs(id) = iterator.next()
      id += 1
    }
    new GenericArrayData(longs)
  }
}
