/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */
package org.apache.kylin.query.runtime.plans

import java.util
import java.util.concurrent.ConcurrentHashMap

import com.google.common.collect.{Lists, Sets}
import org.apache.calcite.DataContext
import org.apache.kylin.common.QueryContext
import org.apache.kylin.cube.CubeInstance
import org.apache.kylin.metadata.model._
import org.apache.kylin.metadata.tuple.TupleInfo
import org.apache.kylin.query.relnode.{OLAPRel, OLAPTableScan}
import org.apache.kylin.query.SchemaProcessor
import org.apache.kylin.query.runtime.SparkOperation
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparderContext, _}
import org.apache.spark.sql.utils.SparkTypeUtil
import org.apache.spark.utils.{LogEx, LogUtils}

import scala.collection.JavaConverters._

// scalastyle:off
object TableScanPlan extends LogEx {


  val cacheDf: ThreadLocal[ConcurrentHashMap[String, DataFrame]] = new ThreadLocal[ConcurrentHashMap[String, DataFrame]] {
    override def initialValue: ConcurrentHashMap[String, DataFrame] = {
      new ConcurrentHashMap[String, DataFrame]
    }
  }

  def createOLAPTable(rel: OLAPRel, dataContext: DataContext): DataFrame = logTime("table scan", info = true) {

    val session: SparkSession = SparderContext.getSparkSession


    /////////////////////////////////////////////
    SparkOperation.createEmptyDataFrame(StructType(Seq()))
  }


  private def processTopN(topNMetric: FunctionDesc, df: DataFrame, topNFieldIndex: Int, tupleInfo: TupleInfo, tableName: String): (DataFrame, Map[Int, Column]) = {
    // support TopN measure
    val topNField = df.schema.fields
      .zipWithIndex
      .filter(_._1.dataType.isInstanceOf[ArrayType])
      .map(_.swap)
      .toMap
      .get(topNFieldIndex)
    require(topNField.isDefined)
    // data like this:
    //   [2012-01-01,4972.2700,WrappedArray([623.45,[10000392,7,2012-01-01]],[47.49,[10000029,4,2012-01-01]])]

    // inline array, one record may output multi records:
    //   [2012-01-01, 4972.2700, 623.45,[10000392,7,2012-01-01]]
    //   [2012-01-01, 4972.2700, 47.49,[10000029,4,2012-01-01]]

    val inlinedSelectExpr = df.schema.fields.filter(_ != topNField.get).map(_.name) :+ s"inline(${topNField.get.name})"
    val inlinedDF = df.selectExpr(inlinedSelectExpr: _*)

    // flatten multi dims in TopN measure, will not increase record number, a total flattened struct:
    //   [2012-01-01, 4972.2700, 623.45, 10000392, 7, 2012-01-01]
    val flattenedSelectExpr = inlinedDF.schema.fields.dropRight(1).map(_.name) :+ s"${inlinedDF.schema.fields.last.name}.*"
    val flattenedDF = inlinedDF.selectExpr(flattenedSelectExpr: _*)

    val topNLiteralColumn = getTopNLiteralColumn(topNMetric)

    val literalTupleIdx = topNLiteralColumn.filter(tupleInfo.hasColumn).map(tupleInfo.getColumnIndex)

    val numericCol = getTopNNumericColumn(topNMetric)
    val numericTupleIdx: Int =
      if (numericCol != null) {
        // for TopN, the aggr must be SUM
        val sumFunc = FunctionDesc.newInstance(FunctionDesc.FUNC_SUM,
          ParameterDesc.newInstance(numericCol), numericCol.getType.toString)
        tupleInfo.getFieldIndex(sumFunc.getRewriteFieldName)
      } else {
        val countFunction = FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT,
          ParameterDesc.newInstance("1"), "bigint")
        tupleInfo.getFieldIndex(countFunction.getRewriteFieldName)
      }

    val dimCols = topNLiteralColumn.toArray
    val dimWithType = literalTupleIdx.zipWithIndex.map(index => {
      val column = dimCols(index._2)
      (SchemaProcessor.genTopNSchema(tableName,
        index._1, column.getIdentity.replaceAll("\\.", "_")),
        SparkTypeUtil.toSparkType(column.getType))
    })

    val sumCol = tupleInfo.getAllColumns.get(numericTupleIdx)
    val sumColName = s"A_SUM_${sumCol.getName}_$numericTupleIdx"
    val measureSchema = StructField(sumColName, DoubleType)
    // flatten schema.
    val newSchema = StructType(df.schema.filter(_.name != topNField.get.name)
      ++ dimWithType.map(tp => StructField(tp._1, tp._2)).+:(measureSchema))

    val topNMapping = literalTupleIdx.zipWithIndex.map(index => {
      (index._1, col(SchemaProcessor.genTopNSchema(tableName,
        index._1, dimCols(index._2).getIdentity.replaceAll("\\.", "_"))))
    }).+:(numericTupleIdx, col(sumColName)).toMap


    (flattenedDF.toDF(newSchema.fieldNames: _*), topNMapping)
  }

  private def getTopNLiteralColumn(functionDesc: FunctionDesc): List[TblColRef] = {
    if (!functionDesc.getParameter.isColumnType) {
      return functionDesc.getParameter.getColRefs.asScala.toList
    }
    functionDesc.getParameter.getColRefs.asScala.drop(1).toList
  }

  private def getTopNNumericColumn(functionDesc: FunctionDesc): TblColRef = {
    if (functionDesc.getParameter.isColumnType) {
      return functionDesc.getParameter.getColRef
    }
    null
  }

  // copy from NCubeTupleConverter
  def getTupleIdx(
    selectedDimensions: util.Set[TblColRef],
    selectedMetrics: util.Set[FunctionDesc],
    tupleInfo: TupleInfo): Array[Int] = {
    var tupleIdx: Array[Int] =
      new Array[Int](selectedDimensions.size + selectedMetrics.size)

    var i = 0
    // pre-calculate dimension index mapping to tuple
    selectedDimensions.asScala.foreach(
      dim => {
        tupleIdx(i) =
          if (tupleInfo.hasColumn(dim)) tupleInfo.getColumnIndex(dim) else -1
        i += 1
      }
    )

    selectedMetrics.asScala.foreach(
      metric => {
        if (metric.needRewrite) {
          val rewriteFieldName = metric.getRewriteFieldName
          tupleIdx(i) =
            if (tupleInfo.hasField(rewriteFieldName))
              tupleInfo.getFieldIndex(rewriteFieldName)
            else -1
        } else { // a non-rewrite metrics (like sum, or dimension playing as metrics) is like a dimension column
          val col = metric.getParameter.getColRef
          tupleIdx(i) =
            if (tupleInfo.hasColumn(col)) tupleInfo.getColumnIndex(col) else -1
        }
        i += 1
      }
    )
    tupleIdx
  }

  def createLookupTable(rel: OLAPTableScan, dataContext: DataContext): DataFrame = {
    SparkOperation.createEmptyDataFrame(StructType(Seq()))

  }

  def createSingleRow(rel: OLAPRel, dataContext: DataContext): DataFrame = {
    val session = SparderContext.getSparkSession
    val rows = List.fill(1)(Row.fromSeq(List[Object]()))
    val rdd = session.sparkContext.makeRDD(rows)
    session.createDataFrame(rdd, StructType(List[StructField]()))
  }
}
