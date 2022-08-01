/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.datasource.storage.UnsafelyInsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.adaptive.{AdaptiveExecutionContext, AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, PartitionSpec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.hive.QueryMetricUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

class SparkQueryMetricUtilsSuite extends QueryTest with SharedSparkSession {

  test("sparkPlan metrics for scanBytes and ScanRows") {
    val tempPath = Utils.createTempDir().getAbsolutePath
    val pathSeq = Seq(new Path(tempPath))
    val relation = HadoopFsRelation(
      location = new InMemoryFileIndex(spark, pathSeq, Map.empty, None),
      partitionSchema = PartitionSpec.emptySpec.partitionColumns,
      dataSchema = StructType.fromAttributes(Nil),
      bucketSpec = Some(BucketSpec(2, Nil, Nil)),
      fileFormat = new ParquetFileFormat(),
      options = Map.empty)(spark)
    val layoutFileSourceScanExec =
      LayoutFileSourceScanExec(relation, Nil,
        relation.dataSchema, Nil, None, None, Nil, None)

    val collectScanMetrics0 = QueryMetricUtils.collectScanMetrics(layoutFileSourceScanExec)
    assert(0 == collectScanMetrics0._1.get(0))
    assert(0 == collectScanMetrics0._2.get(0))

    val dataWritingCommand = UnsafelyInsertIntoHadoopFsRelationCommand(null, null, null, null)
    val dataWritingCommandExec = DataWritingCommandExec(dataWritingCommand, layoutFileSourceScanExec)
    dataWritingCommandExec.metrics("numOutputRows").+=(1000)
    dataWritingCommandExec.metrics("numOutputBytes").+=(56698)
    val collectScanMetrics = QueryMetricUtils.collectScanMetrics(dataWritingCommandExec)
    assert(0 == collectScanMetrics._1.get(0))
    assert(0 == collectScanMetrics._2.get(0))

    layoutFileSourceScanExec.metrics("numOutputRows").+=(2000)
    layoutFileSourceScanExec.metrics("readBytes").+=(16691)
    val adaptiveExecutionContext = AdaptiveExecutionContext(spark.newSession(), null)
    val adaptiveSparkPlanExec = new AdaptiveSparkPlanExec(layoutFileSourceScanExec,
      adaptiveExecutionContext, null, false, false)
    val collectScanMetrics2 = QueryMetricUtils.collectScanMetrics(adaptiveSparkPlanExec)
    assert(2000 == collectScanMetrics2._1.get(0))
    assert(16691 == collectScanMetrics2._2.get(0))

    layoutFileSourceScanExec.metrics("numOutputRows").+=(3000)
    layoutFileSourceScanExec.metrics("readBytes").+=(6291)
    val collectLimitExec = CollectLimitExec(1, 0, layoutFileSourceScanExec)
    val collectScanMetrics3 = QueryMetricUtils.collectScanMetrics(collectLimitExec)
    assert(5000 == collectScanMetrics3._1.get(0))
    assert(22982 == collectScanMetrics3._2.get(0))


    val kylinFileSourceScanExec =
      new KylinFileSourceScanExec(relation, Nil,
        relation.dataSchema, Nil, None, None, Nil, None, false, 10)
    kylinFileSourceScanExec.metrics("numOutputRows").+=(300)
    kylinFileSourceScanExec.metrics("readBytes").+=(5691)
    val collectScanMetrics4 = QueryMetricUtils.collectScanMetrics(kylinFileSourceScanExec)
    assert(300 == collectScanMetrics4._1.get(0))
    assert(5691 == collectScanMetrics4._2.get(0))

    val dataWritingCommandExec2 = DataWritingCommandExec(dataWritingCommand, kylinFileSourceScanExec)
    val collectScanMetrics5 = QueryMetricUtils.collectScanMetrics(dataWritingCommandExec2)
    assert(300 == collectScanMetrics5._1.get(0))
    assert(5691 == collectScanMetrics5._2.get(0))

    val adaptiveSparkPlanExec2 = new AdaptiveSparkPlanExec(kylinFileSourceScanExec,
      adaptiveExecutionContext, null, false, false)
    kylinFileSourceScanExec.metrics("numOutputRows").+=(600)
    kylinFileSourceScanExec.metrics("readBytes").+=(1161)
    val collectScanMetrics6 = QueryMetricUtils.collectScanMetrics(adaptiveSparkPlanExec2)
    assert(900 == collectScanMetrics6._1.get(0))
    assert(6852 == collectScanMetrics6._2.get(0))

    kylinFileSourceScanExec.metrics("numOutputRows").+=(700)
    kylinFileSourceScanExec.metrics("readBytes").+=(2295)
    val collectLimitExec2 = CollectLimitExec(1, 0, kylinFileSourceScanExec)
    val collectScanMetrics7 = QueryMetricUtils.collectScanMetrics(collectLimitExec2)
    assert(1600 == collectScanMetrics7._1.get(0))
    assert(9147 == collectScanMetrics7._2.get(0))


    val fileSourceScanExec =
      FileSourceScanExec(relation, Nil,
        relation.dataSchema, Nil, None, None, Nil, None, false)
    fileSourceScanExec.metrics("numOutputRows").+=(50)
    fileSourceScanExec.metrics("readBytes").+=(461)
    val collectScanMetrics8 = QueryMetricUtils.collectScanMetrics(fileSourceScanExec)
    assert(50 == collectScanMetrics8._1.get(0))
    assert(461 == collectScanMetrics8._2.get(0))

    val dataWritingCommandExec3 = DataWritingCommandExec(dataWritingCommand, fileSourceScanExec)
    val collectScanMetrics9 = QueryMetricUtils.collectScanMetrics(dataWritingCommandExec3)
    assert(50 == collectScanMetrics9._1.get(0))
    assert(461 == collectScanMetrics9._2.get(0))

    val adaptiveSparkPlanExec3 = new AdaptiveSparkPlanExec(fileSourceScanExec,
      adaptiveExecutionContext, null, false, false)
    fileSourceScanExec.metrics("numOutputRows").+=(400)
    fileSourceScanExec.metrics("readBytes").+=(1289)
    val collectScanMetrics10 = QueryMetricUtils.collectScanMetrics(adaptiveSparkPlanExec3)
    assert(0 == collectScanMetrics10._1.get(0))
    assert(0 == collectScanMetrics10._2.get(0))

    fileSourceScanExec.metrics("numOutputRows").+=(400)
    fileSourceScanExec.metrics("readBytes").+=(2210)
    val collectLimitExec3 = CollectLimitExec(1, 0, fileSourceScanExec)
    val collectScanMetrics11 = QueryMetricUtils.collectScanMetrics(collectLimitExec3)
    assert(850 == collectScanMetrics11._1.get(0))
    assert(3960 == collectScanMetrics11._2.get(0))

    val collectScanMetrics12 = QueryMetricUtils.collectScanMetrics(null)
    assert(null == collectScanMetrics12._1)
    assert(null == collectScanMetrics12._2)

    val collectScanMetrics13 = QueryMetricUtils.collectTaskRelatedMetrics(null, null)
    assert(0 == collectScanMetrics13._1)
    assert(0 == collectScanMetrics13._2)
    assert(0 == collectScanMetrics13._3)

    val collectScanMetrics14 = QueryMetricUtils.collectAdaptiveSparkPlanExecMetrics(layoutFileSourceScanExec,
      0, 0)
    assert(5000 == collectScanMetrics14._1)
    assert(22982 == collectScanMetrics14._2)
    val collectScanMetrics15 = QueryMetricUtils.collectAdaptiveSparkPlanExecMetrics(kylinFileSourceScanExec,
      0, 0)
    assert(1600 == collectScanMetrics15._1)
    assert(9147 == collectScanMetrics15._2)
    val collectScanMetrics16 = QueryMetricUtils.collectAdaptiveSparkPlanExecMetrics(fileSourceScanExec,
      0, 0)
    assert(850 == collectScanMetrics16._1)
    assert(3960 == collectScanMetrics16._2)
    val collectScanMetrics17 = QueryMetricUtils.collectAdaptiveSparkPlanExecMetrics(dataWritingCommandExec,
      0, 0)
    assert(5000 == collectScanMetrics17._1)
    assert(22982 == collectScanMetrics17._2)
    val shuffleExchangeExec =
      new ShuffleExchangeExec(layoutFileSourceScanExec.outputPartitioning, layoutFileSourceScanExec)
    val shuffleQueryStageExec = ShuffleQueryStageExec(1, shuffleExchangeExec, shuffleExchangeExec)
    val collectScanMetrics18 = QueryMetricUtils.collectAdaptiveSparkPlanExecMetrics(shuffleQueryStageExec,
      0, 0)
    assert(5000 == collectScanMetrics18._1)
    assert(22982 == collectScanMetrics18._2)
    val hashAggregateExec = new HashAggregateExec(Option(Seq.empty[Expression]), Nil, Nil, Nil,
      1, Nil, null)
    val collectScanMetrics19 = QueryMetricUtils.collectAdaptiveSparkPlanExecMetrics(hashAggregateExec,
      2, 3)
    assert(2 == collectScanMetrics19._1)
    assert(3 == collectScanMetrics19._2)

  }

  test("sparkPlan metrics for scanBytes and ScanRows2") {
    val tempPath = Utils.createTempDir().getAbsolutePath
    val pathSeq = Seq(new Path(tempPath))
    val relation = HadoopFsRelation(
      location = new InMemoryFileIndex(spark, pathSeq, Map.empty, None),
      partitionSchema = PartitionSpec.emptySpec.partitionColumns,
      dataSchema = StructType.fromAttributes(Nil),
      bucketSpec = Some(BucketSpec(2, Nil, Nil)),
      fileFormat = new ParquetFileFormat(),
      options = Map.empty)(spark)
    val layoutFileSourceScanExec =
      LayoutFileSourceScanExec(relation, Nil,
        relation.dataSchema, Nil, None, None, Nil, None)

    val dataWritingCommand = UnsafelyInsertIntoHadoopFsRelationCommand(null, null, null, null)
    val dataWritingCommandExec = DataWritingCommandExec(dataWritingCommand, layoutFileSourceScanExec)
    dataWritingCommandExec.metrics("numOutputRows").+=(2000)
    dataWritingCommandExec.metrics("numOutputBytes").+=(26698)
    val collectScanMetrics = QueryMetricUtils.collectScanMetrics(dataWritingCommandExec)
    assert(0 == collectScanMetrics._1.get(0))
    assert(0 == collectScanMetrics._2.get(0))
    val collectScanMetrics2 = QueryMetricUtils.collectAdaptiveSparkPlanExecMetrics(dataWritingCommandExec, 1, 1)
    assert(1 == collectScanMetrics2._1)
    assert(1 == collectScanMetrics2._2)

  }

}
