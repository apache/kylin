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

package org.apache.spark.sql.execution.gluten

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.FileSourceScanExecTransformer
import org.apache.kylin.common.KylinConfig
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, BloomAndRangeFilterExpression, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasource.ShardSpec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.collection.BitSet

class KylinFileSourceScanExecTransformer(@transient relation: HadoopFsRelation,
                                         output: Seq[Attribute],
                                         requiredSchema: StructType,
                                         partitionFilters: Seq[Expression],
                                         optionalBucketSet: Option[BitSet],
                                         val optionalShardSpec: Option[ShardSpec],
                                         optionalNumCoalescedBuckets: Option[Int],
                                         dataFilters: Seq[Expression],
                                         tableIdentifier: Option[TableIdentifier],
                                         disableBucketedScan: Boolean = false,
                                         sourceScanRows: Long)
  extends FileSourceScanExecTransformer(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan) {

  def getSourceScanRows: Long = {
    sourceScanRows
  }

  override def getPartitions: Seq[InputPartition] =
    BackendsApiManager.getTransformerApiInstance.genInputPartitionSeq(
      relation,
      requiredSchema,
      selectedPartitions,
      output,
      bucketedScan,
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      disableBucketedScan,
      filterExprs())

  // Copy from KylinFileSourceScanExec
  @transient override lazy val selectedPartitions: Array[PartitionDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    val rangeRuntimeFilters = dataFilters.filter(_.isInstanceOf[BloomAndRangeFilterExpression])
      .flatMap(filter => filter.asInstanceOf[BloomAndRangeFilterExpression].rangeRow)
    logInfo(s"Extra runtime filters from BloomAndRangeFilterExpression to " +
      s"prune segment: ${rangeRuntimeFilters.mkString(",")}")
    val collectFilters = dataFilters ++ rangeRuntimeFilters
    val ret = relation.location.listFiles(partitionFilters, collectFilters)
    val timeTakenMs = ((System.nanoTime() - startTime) + optimizerMetadataTimeNs) / 1000 / 1000

    metrics("numFiles").add(ret.map(_.files.size.toLong).sum)
    metrics("metadataTime").add(timeTakenMs)

    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
      metrics("numFiles") :: metrics("metadataTime") :: Nil)

    ret.toArray
  }

  // Copy from KylinFileSourceScanExec
  override lazy val (outputPartitioning, outputOrdering): (Partitioning, Seq[SortOrder]) = {
    val shardSpec = if (KylinConfig.getInstanceFromEnv.isShardingJoinOptEnabled) {
      optionalShardSpec
    } else {
      None
    }

    shardSpec match {
      case Some(spec) =>

        def toAttribute(colName: String): Option[Attribute] =
          output.find(_.name == colName)

        val shardCols = spec.shardColumnNames.flatMap(toAttribute)
        val partitioning = if (shardCols.size == spec.shardColumnNames.size) {
          HashPartitioning(shardCols, spec.numShards)
        } else {
          UnknownPartitioning(0)
        }

        val sortColumns = spec.sortColumnNames.map(toAttribute).takeWhile(_.isDefined).map(_.get)
        val sortOrder = if (sortColumns.nonEmpty) {
          sortColumns.map(SortOrder(_, Ascending))
        } else {
          Nil
        }

        (partitioning, sortOrder)
      case _ =>
        (UnknownPartitioning(0), Nil)
    }
  }

  override val nodeNamePrefix: String = "KylinNativeFile"
}
