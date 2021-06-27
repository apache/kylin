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

package org.apache.kylin.engine.spark.job

import java.util
import java.util.Locale

import org.apache.kylin.engine.spark.builder.CubeBuilderHelper.ENCODE_SUFFIX
import org.apache.kylin.engine.spark.metadata.cube.model.SpanningTree
import org.apache.kylin.engine.spark.metadata.{ColumnDesc, DTType, FunctionDesc, LiteralColumnDesc}
import org.apache.kylin.measure.bitmap.BitmapMeasureType
import org.apache.kylin.measure.hllc.HLLCMeasureType
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DoubleType, FloatType, ShortType, StringType, _}
import org.apache.spark.sql.udaf._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object CuboidAggregator {
  def agg(ss: SparkSession,
          dataSet: DataFrame,
          dimensions: util.Set[Integer],
          measures: util.Map[Integer, FunctionDesc],
          spanningTree: SpanningTree,
          isSparkSql: Boolean): DataFrame = {
    aggInternal(ss, dataSet, dimensions, measures, isSparkSql)
  }

  //noinspection ScalaStyle
  def aggInternal(ss: SparkSession,
                  dataSet: DataFrame,
                  dimensions: util.Set[Integer],
                  measures: util.Map[Integer, FunctionDesc],
                  isSparkSql: Boolean): DataFrame = {
    if (measures.isEmpty) {
      return dataSet
        .select(NSparkCubingUtil.getColumns(dimensions): _*)
        .dropDuplicates()
    }

    val reuseLayout = dataSet.schema.fieldNames
      .contains(measures.keySet().asScala.head.toString)

    val agg = measures.asScala.map { case(id, measure) =>
      val columns = new mutable.ListBuffer[Column]

      if (reuseLayout) {
        columns.append(col(id.toString))
      } else {
        if (measure.pra.head.isColumnType) {
          val colIndex = dataSet.schema.fieldNames.zipWithIndex.map(tp => (tp._2, tp._1)).toMap
          columns.appendAll(measure.pra.map(p =>col(p.id.toString)))
        } else {
          var value = measure.pra.head.asInstanceOf[LiteralColumnDesc].value
          value = measure.pra.head.dataType match {
            case BooleanType => value.asInstanceOf[String].toBoolean
            case ByteType => value.asInstanceOf[String].toByte
            case ShortType => value.asInstanceOf[String].toShort
            case IntegerType | DateType => value.asInstanceOf[String].toInt
            case LongType | TimestampType => value.asInstanceOf[String].toLong
            case FloatType => value.asInstanceOf[String].toFloat
            case DoubleType => value.asInstanceOf[String].toDouble
            case BinaryType => value.asInstanceOf[String].toArray
            case StringType => value.asInstanceOf[UTF8String]
          }
          columns.append(new Column(Literal.create(value, measure.pra.head.dataType)))
        }
      }

      measure.expression.toUpperCase(Locale.ROOT) match {
        case "MAX" =>
          max(columns.head).as(id.toString)
        case "MIN" =>
          min(columns.head).as(id.toString)
        case "SUM" =>
          sum(columns.head).as(id.toString)
        case "COUNT" =>
          if (reuseLayout) {
            sum(columns.head).as(id.toString)
          } else {
            count(columns.head).as(id.toString)
          }
        case "COUNT_DISTINCT" =>
          // for test
          if (isSparkSql) {
            countDistinct(columns.head).as(id.toString)
          } else {
            val cdAggregate = getCountDistinctAggregate(columns, measure.returnType, reuseLayout)
            new Column(cdAggregate.toAggregateExpression()).as(id.toString)
          }
        case "TOP_N" =>
          // Uses new TopN aggregate function
          // located in kylin-spark-project/kylin-spark-common/src/main/scala/org/apache/spark/sql/udaf/TopN.scala
          val schema = StructType(measure.pra.map { col =>
            val dateType = col.dataType
            if (col == measure) {
              StructField(s"MEASURE_${col.columnName}", dateType)
            } else {
              StructField(s"DIMENSION_${col.columnName}", dateType)
            }
          })

          if (reuseLayout) {
            new Column(ReuseTopN(measure.returnType.precision, schema, columns.head.expr)
              .toAggregateExpression()).as(id.toString)
          } else {
            new Column(EncodeTopN(measure.returnType.precision, schema, columns.head.expr, columns.drop(1).map(_.expr))
              .toAggregateExpression()).as(id.toString)
          }
        case "PERCENTILE_APPROX" =>
          val udfName = UdfManager.register(measure.returnType.toKylinDataType, measure.expression, null, !reuseLayout)
          if (!reuseLayout) {
            callUDF(udfName, columns.head.cast(StringType)).as(id.toString)
          } else {
            callUDF(udfName, columns.head).as(id.toString)
          }
        case _ =>
          max(columns.head).as(id.toString)
      }
    }.toSeq

    val df: DataFrame = if (!dimensions.isEmpty) {
      dataSet
        .groupBy(NSparkCubingUtil.getColumns(dimensions): _*)
        .agg(agg.head, agg.drop(1): _*)
    } else {
      dataSet
        .agg(agg.head, agg.drop(1): _*)
    }

    // Avoid sum(decimal) add more precision
    // For example: sum(decimal(19,4)) -> decimal(29,4)  sum(sum(decimal(19,4))) -> decimal(38,4)
    if (reuseLayout) {
      val columns = NSparkCubingUtil.getColumns(dimensions) ++ measureColumns(dataSet.schema, measures)
      df.select(columns: _*)
    } else {
      df
    }
  }

  private def getCountDistinctAggregate(columns: ListBuffer[Column],
                                        returnType: DTType,
                                        reuseLayout: Boolean): AggregateFunction = {
    var col = columns.head
    if (isBitmap(returnType)) {
      if (!reuseLayout) {
        EncodePreciseCountDistinct(wrapEncodeColumn(columns.head).expr)
      } else {
        ReusePreciseCountDistinct(col.expr)
      }
    } else {
      val precision = returnType.precision

      if (isMultiHllcCol(columns, returnType)) {
        col = wrapMutilHllcColumn(columns: _*)
      }
      if (!reuseLayout) {
        EncodeApproxCountDistinct(col.expr, precision)
      } else {
        ReuseApproxCountDistinct(col.expr, precision)
      }
    }
  }

  private def constructTopNSchema(parameters: List[ColumnDesc]): StructType = {
    val measure = parameters.head
    val schema = StructType(parameters.map { col =>
      if (col == measure) {
        StructField(s"MEASURE_${col.columnName}", col.dataType)
      } else {
        StructField(s"DIMENSION_${col.columnName}", col.dataType)
      }
    })
    schema
  }

  private def isMultiHllcCol(columns: ListBuffer[Column], returnDataType: DTType) = {
    columns.length > 1 && returnDataType.dataType.startsWith(HLLCMeasureType.DATATYPE_HLLC)
  }

  private def isBitmap(returnDataType: DTType) = {
    returnDataType.dataType.equalsIgnoreCase(BitmapMeasureType.DATATYPE_BITMAP)
  }

  private def measureColumns(schema: StructType, measures: util.Map[Integer, FunctionDesc]): mutable.Iterable[Column] = {
    measures.asScala.map { e =>
      e._2.expression.toUpperCase(Locale.ROOT) match {
        case "SUM" =>
          val measureId = e._1.toString
          val dataType = schema.find(_.name.equals(measureId)).map(_.dataType).get
          col(measureId).cast(dataType).as(measureId)
        case _ =>
          val measureId = e._1.toString
          col(measureId)
      }
    }
  }

  def wrapEncodeColumn(column: Column): Column = {
    new Column(column.toString() + ENCODE_SUFFIX)
  }

  def wrapMutilHllcColumn(columns: Column*): Column = {
    var col: Column = when(isnull(columns.head), null)
    for (inputCol <- columns.drop(1)) {
      col = col.when(isnull(inputCol), null)
    }

    col = col.otherwise(hash(columns: _*))
    col
  }
}