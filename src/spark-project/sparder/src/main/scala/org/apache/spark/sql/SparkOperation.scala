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
import org.apache.spark.sql.catalyst.expressions.GroupingSets
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.types.StructType

object SparkOperation {
  //    def createDictPushDownTable(args: CreateDictPushdownTableArgc): DataFrame = {
  //      val spark = SparderEnv.getSparkSession
  //      spark.read
  //        .format(args.fileFormat)
  //        .option(SparderConstants.PARQUET_FILE_FILE_TYPE, SparderConstants.PARQUET_FILE_CUBE_TYPE)
  //        .option(SparderConstants.KYLIN_SCAN_GTINFO_BYTES, args.gtinfo)
  //  //      .option(
  //  //        ParquetFormatConstants.KYLIN_BUNDLE_READER,
  //  //        KapConfig.getInstanceFromEnv.getBundleReader)
  //        .option(SparderConstants.CUBOID_ID, args.cuboid)
  //        .option(SparderConstants.DICT_PATCH, args.dictPatch)
  //        .option(SparderConstants.DICT, args.dict)
  //        .option(SparderConstants.TABLE_ALIAS, args.tableName)
  //        .option(SparderConstants.STORAGE_TYPE, args.storageType)
  //        .schema(args.schema)
  //        .option(SparderConstants.PAGE_FILTER_PUSH_DOWN, args.pushdown)
  //        .option(SparderConstants.BINARY_FILTER_PUSH_DOWN, args.binaryFilterPushdown)
  //        .option("segmentId", args.uuid)
  //        .option(SparderConstants.DIAGNOSIS_WRITER_TYPE, args.diagnosisWriterType)
  //        .load(args.filePath: _*)
  //        .as(args.tableName)
  //    }

  def createEmptyDataFrame(structType: StructType): DataFrame = {
    SparderEnv.getSparkSession
      .createDataFrame(new java.util.ArrayList[Row], structType)
  }

  def createEmptyRDD(): RDD[InternalRow] = {
    SparderEnv.getSparkSession.sparkContext.emptyRDD[InternalRow]
  }

  def createConstantDataFrame(rows: java.util.List[Row], structType: StructType): DataFrame = {
    SparderEnv.getSparkSession.createDataFrame(rows, structType)
  }

  def agg(aggArgc: AggArgc): DataFrame = {
    if (aggArgc.agg.nonEmpty && aggArgc.group.nonEmpty && !aggArgc.isSimpleGroup && aggArgc.groupSets.nonEmpty) {
      Dataset.ofRows(
        aggArgc.dataFrame.sparkSession,
        Aggregate(
          Seq(GroupingSets(aggArgc.groupSets.map(gs => gs.map(_.expr)),
            aggArgc.group.map(_.expr))),
          aggArgc.group.map(_.named) ++ aggArgc.agg.map(_.named),
          aggArgc.dataFrame.queryExecution.logical
        )
      )
    } else if (aggArgc.agg.nonEmpty && aggArgc.group.nonEmpty) {
      aggArgc.dataFrame
        .groupBy(aggArgc.group: _*)
        .agg(aggArgc.agg.head, aggArgc.agg.drop(1): _*)
    } else if (aggArgc.agg.isEmpty && aggArgc.group.nonEmpty) {
      aggArgc.dataFrame.groupBy(aggArgc.group: _*).agg(count(lit("1"))).select(aggArgc.group: _*)
    } else if (aggArgc.agg.nonEmpty && aggArgc.group.isEmpty) {
      aggArgc.dataFrame.agg(aggArgc.agg.head, aggArgc.agg.drop(1): _*)
    } else {
      aggArgc.dataFrame
    }
  }

  /*
    /**
      * Collect all elements from a spark plan.
      */
    private def collectFromPlan(plan: SparkPlan, deserializer: Expression): Array[Row] = {
      // This projection writes output to a `InternalRow`, which means applying this projection is not
      // thread-safe. Here we create the projection inside this method to make `Dataset` thread-safe.
      val objProj = GenerateSafeProjection.generate(deserializer :: Nil)
      plan.executeCollect().map { row =>
        // The row returned by SafeProjection is `SpecificInternalRow`, which ignore the data type
        // parameter of its `get` method, so it's safe to use null here.
        objProj(row).get(0, null).asInstanceOf[Row]
      }
    }
  */
}

case class AggArgc(dataFrame: DataFrame, group: List[Column], agg: List[Column], groupSets: List[List[Column]], isSimpleGroup: Boolean)