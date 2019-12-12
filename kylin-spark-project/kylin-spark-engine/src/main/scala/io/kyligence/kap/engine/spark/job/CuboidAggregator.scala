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

package io.kyligence.kap.engine.spark.job

import java.util

import io.kyligence.kap.engine.spark.builder.DFBuilderHelper.ENCODE_SUFFIX
import org.apache.kylin.engine.spark.metadata.cube.model.DataModel.Measure
import org.apache.kylin.engine.spark.metadata.cube.model.{CubeJoinedFlatTableDesc, DataSegment, SpanningTree}
import org.apache.kylin.measure.bitmap.BitmapMeasureType
import org.apache.kylin.measure.hllc.HLLCMeasureType
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{StringType, _}
import org.apache.spark.sql.udaf._
import org.apache.spark.sql.util.SparkTypeUtil
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

object CuboidAggregator {
  def agg(ss: SparkSession,
          dataSet: DataFrame,
          dimensions: util.Set[Integer],
          measures: util.Map[Integer, Measure],
          seg: DataSegment,
          spanningTree: SpanningTree): DataFrame = {
    val needJoin = spanningTree match {
      // when merge cuboid spanning tree is null
      case null => true
      case _ => ParentSourceChooser.needJoinLookupTables(seg.getModel, spanningTree)
    }
    val flatTableDesc =
      new CubeJoinedFlatTableDesc(seg.getCube, seg.getSegRange, needJoin)
    aggInternal(ss, dataSet, dimensions, measures, flatTableDesc, isSparkSql = false)
  }

  def aggInternal(ss: SparkSession,
                  dataSet: DataFrame,
                  dimensions: util.Set[Integer],
                  measures: util.Map[Integer, Measure],
                  flatTableDesc: CubeJoinedFlatTableDesc,
                  isSparkSql: Boolean): DataFrame = {
    if (measures.isEmpty) {
      return dataSet
        .select(NSparkCubingUtil.getColumns(dimensions): _*)
        .dropDuplicates()
    }

    val reuseLayout = dataSet.schema.fieldNames
      .contains(measures.keySet().asScala.head.toString)

    val agg = measures.asScala.map { e =>
      val function = e._2.getFunction
      val parameters = function.getParameters.asScala.toList
      val columns = new mutable.ListBuffer[Column]
      val returnDataType = function.getReturnDataType
      val expression = function.getExpression
      if (parameters.head.isColumnType) {
        if (reuseLayout) {
          columns.append(col(e._1.toString))
        } else {
          val colIndex = dataSet.schema.fieldNames.zipWithIndex.map(tp => (tp._2, tp._1)).toMap
          columns.appendAll(parameters.map(p => col(colIndex.apply(flatTableDesc.getColumnIndex(p.getColRef)))))
        }
      } else {
        if (reuseLayout) {
          columns.append(col(e._1.toString))
        } else {
          val value = parameters.head.getValue
          if (expression.equalsIgnoreCase("SUM")) {
            columns.append(lit(value).cast(SparkTypeUtil.toSparkType(returnDataType)))
          } else {
            columns.append(lit(value))
          }
        }
      }

      expression.toUpperCase match {
        case "MAX" =>
          max(columns.head).as(e._1.toString)
        case "MIN" =>
          min(columns.head).as(e._1.toString)
        case "SUM" =>
          sum(columns.head).as(e._1.toString)
        case "COUNT" =>
          if (reuseLayout) {
            sum(columns.head).as(e._1.toString)
          } else {
            count(columns.head).as(e._1.toString)
          }
        case "COUNT_DISTINCT" =>
          // for test
          if (isSparkSql) {
            countDistinct(columns.head).as(e._1.toString)
          } else {
            if (!reuseLayout) {
              var col = columns.head
              if (returnDataType.getName.equalsIgnoreCase(BitmapMeasureType.DATATYPE_BITMAP)) {
                col = wrapEncodeColumn(columns.head)
                new Column(EncodePreciseCountDistinct(col.expr).toAggregateExpression()).as(e._1.toString)
              } else if (columns.length > 1 && returnDataType.getName.startsWith(HLLCMeasureType.DATATYPE_HLLC)) {
                col = wrapMutilHllcColumn(columns: _*)
                new Column(EncodeApproxCountDistinct(col.expr, returnDataType.getPrecision).toAggregateExpression())
                  .as(e._1.toString)
              } else {
                new Column(EncodeApproxCountDistinct(col.expr, returnDataType.getPrecision).toAggregateExpression())
                  .as(e._1.toString)
              }
            } else {
              var col = columns.head
              if (returnDataType.getName.equalsIgnoreCase(BitmapMeasureType.DATATYPE_BITMAP)) {
                new Column(ReusePreciseCountDistinct(col.expr).toAggregateExpression()).as(e._1.toString)
              } else if (columns.length > 1 && returnDataType.getName.startsWith(HLLCMeasureType.DATATYPE_HLLC)) {
                col = wrapMutilHllcColumn(columns: _*)
                new Column(ReuseApproxCountDistinct(col.expr, returnDataType.getPrecision).toAggregateExpression())
                  .as(e._1.toString)
              } else {
                new Column(ReuseApproxCountDistinct(col.expr, returnDataType.getPrecision).toAggregateExpression())
                  .as(e._1.toString)
              }
            }
          }
        case "TOP_N" =>
          val measure = parameters.head.getColRef.getColumnDesc
          val schema = StructType(parameters.map(_.getColRef.getColumnDesc).map { col =>
            val dateType = SparkTypeUtil.toSparkType(col.getType)
            if (col == measure) {
              StructField(s"MEASURE_${col.getName}", dateType)
            } else {
              StructField(s"DIMENSION_${col.getName}", dateType)
            }
          })

          val udfName = UdfManager.register(returnDataType,
            expression, schema, !reuseLayout)
          callUDF(udfName, columns: _*).as(e._1.toString)
        case "PERCENTILE_APPROX" =>
          val udfName = UdfManager.register(returnDataType, expression, null, !reuseLayout)
          if (!reuseLayout) {
            callUDF(udfName, columns.head.cast(StringType)).as(e._1.toString)
          } else {
            callUDF(udfName, columns.head).as(e._1.toString)
          }
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

  private def measureColumns(schema: StructType, measures: util.Map[Integer, Measure]): mutable.Iterable[Column] = {
    measures.asScala.map { e =>
      e._2.getFunction.getExpression.toUpperCase match {
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