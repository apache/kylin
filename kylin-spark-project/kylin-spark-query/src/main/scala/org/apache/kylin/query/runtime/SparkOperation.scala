/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
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
 */
package org.apache.kylin.query.runtime

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.GroupingSets
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Row, SparderContext}

object SparkOperation {

  def createEmptyDataFrame(structType: StructType): DataFrame = {
    SparderContext.getSparkSession
      .createDataFrame(new java.util.ArrayList[Row], structType)
  }

  def createEmptyRDD(): RDD[InternalRow] = {
    SparderContext.getSparkSession.sparkContext.emptyRDD[InternalRow]
  }

  def createConstantDataFrame(rows: java.util.List[Row], structType: StructType): DataFrame = {
    SparderContext.getSparkSession.createDataFrame(rows, structType)
  }

  def agg(aggArgc: AggArgc): DataFrame = {
    if (aggArgc.agg.nonEmpty && aggArgc.group.nonEmpty) {
      aggArgc.dataFrame
        .groupBy(aggArgc.group: _*)
        .agg(aggArgc.agg.head, aggArgc.agg.drop(1): _*)
    } else if (aggArgc.agg.isEmpty && aggArgc.group.nonEmpty) {
      aggArgc.dataFrame.dropDuplicates(aggArgc.group.map(_.toString()))
    } else if (aggArgc.agg.nonEmpty && aggArgc.group.isEmpty) {
      aggArgc.dataFrame.agg(aggArgc.agg.head, aggArgc.agg.drop(1): _*)
    } else {
      aggArgc.dataFrame
    }
  }
}

case class AggArgc(dataFrame: DataFrame, group: List[Column], agg: List[Column])