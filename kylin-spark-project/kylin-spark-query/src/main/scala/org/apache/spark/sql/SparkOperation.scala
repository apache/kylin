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
package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.GroupingSets
import org.apache.spark.sql.types.StructType

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
    if (aggArgc.agg.nonEmpty && aggArgc.group.nonEmpty &&
      !aggArgc.isSimpleGroup && aggArgc.groupSets.nonEmpty) {
      // for grouping sets, group by cube or group by rollup
      val groupingSets = GroupingSets(aggArgc.groupSets.map(groupSet => groupSet.map(_.expr)),
        aggArgc.group.map(_.expr),
        aggArgc.dataFrame.queryExecution.logical,
        aggArgc.group.map(_.named) ++ aggArgc.agg.map(_.named)
      )
      Dataset.ofRows(aggArgc.dataFrame.sparkSession, groupingSets)
    } else if (aggArgc.agg.nonEmpty && aggArgc.group.nonEmpty) {
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

case class AggArgc(dataFrame: DataFrame, group: List[Column], agg: List[Column],
                   groupSets: List[List[Column]], isSimpleGroup: Boolean)