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

import org.apache.kylin.common.KapConfig
import org.apache.kylin.engine.spark.builder.DFBuilderHelper.ENCODE_SUFFIX
import org.apache.kylin.metadata.cube.cuboid.NSpanningTree
import org.apache.kylin.metadata.cube.model.{NCubeJoinedFlatTableDesc, NDataSegment}
import org.apache.kylin.metadata.model.NDataModel.Measure
import org.apache.kylin.measure.bitmap.BitmapMeasureType
import org.apache.kylin.measure.hllc.HLLCMeasureType
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.udaf._
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.util.SparderTypeUtil.toSparkType
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Literal

import java.util
import java.util.Locale
import scala.collection.JavaConverters._
import scala.collection.mutable

object CuboidAggregator {

  def agg(dataset: DataFrame,
          dimensions: util.Set[Integer],
          measures: util.Map[Integer, Measure],
          seg: NDataSegment,
          spanningTree: NSpanningTree): DataFrame = {

    val needJoin = spanningTree match {
      // when merge cuboid st is null
      case null => true
      case _ => DFChooser.needJoinLookupTables(seg.getModel, spanningTree)
    }

    val flatTableDesc =
      new NCubeJoinedFlatTableDesc(seg.getIndexPlan, seg.getSegRange, needJoin)

    val columnIndex =
      dataset.schema.fieldNames.zipWithIndex.map(tp => (tp._2, tp._1)).toMap

    aggregate(dataset, dimensions, measures, //
      (colRef: TblColRef) => columnIndex.apply(flatTableDesc.getColumnIndex(colRef)) )
  }

  /**
    * Avoid compilation error when invoking aggregate in java
    * incompatible types: Function1 is not a functional interface
    *
    * @param dataset
    * @param dimensions
    * @param measures
    * @param tableDesc
    * @param isSparkSQL
    * @return
    */
  def aggregateJava(dataset: DataFrame,
                    dimensions: util.Set[Integer],
                    measures: util.Map[Integer, Measure],
                    tableDesc: NCubeJoinedFlatTableDesc,
                    isSparkSQL: Boolean = false): DataFrame = {

    val columnIndex =
      dataset.schema.fieldNames.zipWithIndex.map(tp => (tp._2, tp._1)).toMap

    aggregate(dataset, dimensions, measures, //
      (colRef: TblColRef) => columnIndex.apply(tableDesc.getColumnIndex(colRef)), isSparkSQL)
  }

  def aggregate(dataset: DataFrame,
                dimensions: util.Set[Integer],
                measures: util.Map[Integer, Measure],
                columnIdFunc: TblColRef => String,
                isSparkSQL: Boolean = false): DataFrame = {
    if (measures.isEmpty) {
      return dataset
        .select(NSparkCubingUtil.getColumns(dimensions): _*)
        .dropDuplicates()
    }

    val reuseLayout = dataset.schema.fieldNames
      .contains(measures.keySet().asScala.head.toString)

    var taggedColIndex: Int = -1

    val agg = measures.asScala.map { measureEntry =>
      val measure = measureEntry._2
      val function = measure.getFunction
      val parameters = function.getParameters.asScala.toList
      val columns = new mutable.ListBuffer[Column]
      val returnType = function.getReturnDataType
      if (parameters.head.isColumnType) {
        if (reuseLayout) {
          columns.append(col(measureEntry._1.toString))
        } else {
          columns.appendAll(parameters.map(p => col(columnIdFunc.apply(p.getColRef))))
        }
      } else {
        if (reuseLayout) {
          columns.append(col(measureEntry._1.toString))
        } else {
          val par = parameters.head.getValue
          if (function.getExpression.equalsIgnoreCase("SUM")) {
            columns.append(lit(par).cast(SparderTypeUtil.toSparkType(returnType)))
          } else {
            columns.append(lit(par))
          }
        }
      }

      function.getExpression.toUpperCase(Locale.ROOT) match {
        case "MAX" =>
          max(columns.head).as(measureEntry._1.toString)
        case "MIN" =>
          min(columns.head).as(measureEntry._1.toString)
        case "SUM" =>
          sum(columns.head).as(measureEntry._1.toString)
        case "COUNT" =>
          if (reuseLayout) {
            sum(columns.head).as(measureEntry._1.toString)
          } else {
            count(columns.head).as(measureEntry._1.toString)
          }
        case "COUNT_DISTINCT" =>
          // add for test
          if (isSparkSQL) {
            countDistinct(columns.head).as(measureEntry._1.toString)
          } else {
            var cdCol = columns.head
            val isBitmap = returnType.getName.equalsIgnoreCase(BitmapMeasureType.DATATYPE_BITMAP)
            val isHllc = returnType.getName.startsWith(HLLCMeasureType.DATATYPE_HLLC)

            if (isBitmap && parameters.size == 2) {
              require(measures.size() == 1, "Opt intersect count can only has one measure.")
              if (!reuseLayout) {
                taggedColIndex = columnIdFunc.apply(parameters.last.getColRef).toInt
                val tagCol = col(taggedColIndex.toString)
                val separator = KapConfig.getInstanceFromEnv.getIntersectCountSeparator
                cdCol = wrapEncodeColumn(columns.head)
                new Column(OptIntersectCount(cdCol.expr, split(tagCol, s"\\$separator").expr).toAggregateExpression())
                  .as(s"map_${measureEntry._1.toString}")
              } else {
                new Column(ReusePreciseCountDistinct(cdCol.expr).toAggregateExpression())
                  .as(measureEntry._1.toString)
              }
            } else {
              if (!reuseLayout) {
                if (isBitmap) {
                  cdCol = wrapEncodeColumn(columns.head)
                  new Column(EncodePreciseCountDistinct(cdCol.expr).toAggregateExpression())
                    .as(measureEntry._1.toString)
                } else if (columns.length > 1 && isHllc) {
                  cdCol = wrapMutilHllcColumn(columns: _*)
                  new Column(EncodeApproxCountDistinct(cdCol.expr, returnType.getPrecision).toAggregateExpression())
                    .as(measureEntry._1.toString)
                } else {
                  new Column(EncodeApproxCountDistinct(cdCol.expr, returnType.getPrecision).toAggregateExpression())
                    .as(measureEntry._1.toString)
                }
              } else {
                if (isBitmap) {
                  new Column(ReusePreciseCountDistinct(cdCol.expr).toAggregateExpression())
                    .as(measureEntry._1.toString)
                } else if (columns.length > 1 && isHllc) {
                  cdCol = wrapMutilHllcColumn(columns: _*)
                  new Column(ReuseApproxCountDistinct(cdCol.expr, returnType.getPrecision).toAggregateExpression())
                    .as(measureEntry._1.toString)
                } else {
                  new Column(ReuseApproxCountDistinct(cdCol.expr, returnType.getPrecision).toAggregateExpression())
                    .as(measureEntry._1.toString)
                }
              }
            }
          }
        case "TOP_N" =>

          val measure = function.getParameters.get(0).getColRef.getColumnDesc

          val schema = StructType(parameters.map(_.getColRef.getColumnDesc).map { col =>
            val dateType = toSparkType(col.getType)
            if (col == measure) {
              StructField(s"MEASURE_${col.getName}", dateType)
            } else {
              StructField(s"DIMENSION_${col.getName}", dateType)
            }
          })

          if (reuseLayout) {
            new Column(ReuseTopN(returnType.getPrecision, schema, columns.head.expr)
              .toAggregateExpression()).as(measureEntry._1.toString)
          } else {
            new Column(EncodeTopN(returnType.getPrecision, schema, columns.head.expr, columns.drop(1).map(_.expr))
              .toAggregateExpression()).as(measureEntry._1.toString)
          }
        case "PERCENTILE_APPROX" =>
          new Column(Percentile(columns.head.expr, returnType.getPrecision)
            .toAggregateExpression()).as(measureEntry._1.toString)
        case "COLLECT_SET" =>
          if (reuseLayout) {
            array_distinct(flatten(collect_set(columns.head))).as(measureEntry._1.toString)
          } else {
            collect_set(columns.head).as(measureEntry._1.toString)
          }
        case "CORR" =>
          new Column(Literal(null, DoubleType)).as(measureEntry._1.toString)
      }
    }.toSeq

    val dim = if (taggedColIndex != -1 && !reuseLayout) {
      val d = new util.HashSet[Integer](dimensions)
      d.remove(taggedColIndex)
      d
    } else {
      dimensions
    }

    val df: DataFrame = if (!dim.isEmpty) {
      dataset
        .groupBy(NSparkCubingUtil.getColumns(dim): _*)
        .agg(agg.head, agg.drop(1): _*)
    } else {
      dataset
        .agg(agg.head, agg.drop(1): _*)
    }

    // Avoid sum(decimal) add more precision
    // For example: sum(decimal(19,4)) -> decimal(29,4)  sum(sum(decimal(19,4))) -> decimal(38,4)
    if (reuseLayout) {
      val columns = NSparkCubingUtil.getColumns(dimensions) ++ measureColumns(dataset.schema, measures)
      df.select(columns: _*)
    } else {
      if (taggedColIndex != -1) {
        val icCol = df.schema.fieldNames.filter(_.contains("map")).head
        val fieldsWithoutIc = df.schema.fieldNames.filter(!_.contains(icCol))

        val cdMeasureName = icCol.split("_").last
        val newSchema = fieldsWithoutIc.:+(taggedColIndex.toString).:+(cdMeasureName)

        val exploded = fieldsWithoutIc.map(col).:+(explode(col(icCol)))
        df.select(exploded: _*).toDF(newSchema: _*)
      } else {
        df
      }
    }
  }

  private def measureColumns(schema: StructType, measures: util.Map[Integer, Measure]): mutable.Iterable[Column] = {
    measures.asScala.map { mea =>
      val measureId = mea._1.toString
      mea._2.getFunction.getExpression.toUpperCase(Locale.ROOT) match {
        case "SUM" =>
          val dataType = schema.find(_.name.equals(measureId)).get.dataType
          col(measureId).cast(dataType).as(measureId)
        case _ =>
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
