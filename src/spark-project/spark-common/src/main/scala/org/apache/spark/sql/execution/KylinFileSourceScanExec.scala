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

package org.apache.spark.sql.execution

import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}
import org.apache.kylin.common.KylinConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.datasource.{FilePruner, ShardSpec}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

// scalastyle:off
class KylinFileSourceScanExec(
     @transient relation: HadoopFsRelation,
     output: Seq[Attribute],
     requiredSchema: StructType,
     partitionFilters: Seq[Expression],
     val optionalShardSpec: Option[ShardSpec],
     optionalNumCoalescedBuckets: Option[Int],
     dataFilters: Seq[Expression],
     tableIdentifier: Option[TableIdentifier],
     disableBucketedScan: Boolean = false,
     sourceScanRows: Long) extends LayoutFileSourceScanExec(
  relation, output, requiredSchema, partitionFilters, None, optionalNumCoalescedBuckets, dataFilters, tableIdentifier, disableBucketedScan) {

  def getSourceScanRows: Long = {
    sourceScanRows
  }

  @transient override lazy val selectedPartitions: Array[PartitionDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    val ret = relation.location.listFiles(partitionFilters, dataFilters)
    val timeTakenMs = ((System.nanoTime() - startTime) + optimizerMetadataTimeNs) / 1000 / 1000

    metrics("numFiles").add(ret.map(_.files.size.toLong).sum)
    metrics("metadataTime").add(timeTakenMs)

    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
      metrics("numFiles") :: metrics("metadataTime") :: Nil)

    ret.toArray
  }

  override lazy val inputRDD: RDD[InternalRow] = {
    val readFile: (PartitionedFile) => Iterator[InternalRow] =
      relation.fileFormat.buildReaderWithPartitionValues(
        sparkSession = relation.sparkSession,
        dataSchema = relation.dataSchema,
        partitionSchema = relation.partitionSchema,
        requiredSchema = requiredSchema,
        filters = pushedDownFilters,
        options = relation.options,
        hadoopConf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))

    optionalShardSpec match {
      case Some(spec) =>
        createShardingReadRDD(spec, readFile, selectedPartitions, relation)
      case _ =>
        createNonShardingReadRDD(readFile, selectedPartitions, relation)
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

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

  /**
   * Copied from org.apache.spark.sql.execution.FileSourceScanExec#createBucketedReadRDD
   *
   * Create an RDD for sharding reads.
   * The non-sharding variant of this function is [[createNonShardingReadRDD]].
   *
   * The algorithm is pretty simple: each RDD partition being returned should include all the files
   * with the same shard id from all the given Hive partitions.
   *
   * @param shardSpec          the sharding spec.
   * @param readFile           a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation         [[HadoopFsRelation]] associated with the read.
   */
  private def createShardingReadRDD(shardSpec: ShardSpec,
                                    readFile: (PartitionedFile) => Iterator[InternalRow],
                                    selectedPartitions: Seq[PartitionDirectory],
                                    fsRelation: HadoopFsRelation): RDD[InternalRow] = {
    logInfo(s"Planning with ${shardSpec.numShards} shards")
    val filesToPartitionId =
      selectedPartitions.flatMap { p =>
        p.files.map { f =>
          val hosts = getBlockHosts(getBlockLocations(f), 0, f.getLen)
          PartitionedFile(p.values, f.getPath.toUri.toString, 0, f.getLen, hosts)
        }
      }.groupBy {
        f => FilePruner.getPartitionId(new Path(f.filePath))
      }

    val filePartitions = Seq.tabulate(shardSpec.numShards) { shardId =>
      FilePartition(shardId, filesToPartitionId.getOrElse(shardId, Nil).toArray)
    }

    new FileScanRDD(fsRelation.sparkSession, readFile, filePartitions)
  }

  /**
   * Copied from org.apache.spark.sql.execution.FileSourceScanExec#createNonBucketedReadRDD, no hacking.
   *
   * Create an RDD for non-sharding reads.
   * The sharding variant of this function is [[createShardingReadRDD]].
   *
   * @param readFile           a function to read each (part of a) file.
   * @param selectedPartitions Hive-style partition that are part of the read.
   * @param fsRelation         [[HadoopFsRelation]] associated with the read.
   */
  private def createNonShardingReadRDD(readFile: (PartitionedFile) => Iterator[InternalRow],
                                       selectedPartitions: Seq[PartitionDirectory],
                                       fsRelation: HadoopFsRelation): RDD[InternalRow] = {

    val defaultMaxSplitBytes =
      fsRelation.sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = fsRelation.sparkSession.sessionState.conf.filesOpenCostInBytes

    val maxSplitBytes = if (KylinConfig.getInstanceFromEnv.readSourceWithDefaultParallelism()) {
      val defaultParallelism = fsRelation.sparkSession.sparkContext.defaultParallelism
      val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
      val bytesPerCore = totalBytes / defaultParallelism

      Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
    } else {
      defaultMaxSplitBytes
    }
    logInfo(s"Planning scan with bin packing, max size is: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes.")

    val splitFiles = selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        val blockLocations = getBlockLocations(file)
        if (fsRelation.fileFormat.isSplitable(
          fsRelation.sparkSession, fsRelation.options, file.getPath)) {
          (0L until file.getLen by maxSplitBytes).map { offset =>
            val remaining = file.getLen - offset
            val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
            val hosts = getBlockHosts(blockLocations, offset, size)
            PartitionedFile(
              partition.values, file.getPath.toUri.toString, offset, size, hosts)
          }
        } else {
          val hosts = getBlockHosts(blockLocations, 0, file.getLen)
          Seq(PartitionedFile(
            partition.values, file.getPath.toUri.toString, 0, file.getLen, hosts))
        }
      }
    }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val partitions = new ArrayBuffer[FilePartition]
    val currentFiles = new ArrayBuffer[PartitionedFile]
    var currentSize = 0L

    /** Close the current partition and move to the next. */
    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        val newPartition =
          FilePartition(
            partitions.size,
            currentFiles.toArray) // Copy to a new Array.
        partitions += newPartition
      }
      currentFiles.clear()
      currentSize = 0
    }

    // Assign files to partitions using "Next Fit Decreasing"
    splitFiles.foreach { file =>
      if (currentSize + file.length > maxSplitBytes) {
        closePartition()
      }
      // Add the given file to the current partition.
      currentSize += file.length + openCostInBytes
      currentFiles += file
    }
    closePartition()
    new FileScanRDD(fsRelation.sparkSession, readFile, partitions)
  }

  private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case f => Array.empty[BlockLocation]
  }

  // Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
  // pair that represents a segment of the same file, find out the block that contains the largest
  // fraction the segment, and returns location hosts of that block. If no such block can be found,
  // returns an empty array.
  private def getBlockHosts(blockLocations: Array[BlockLocation], offset: Long, length: Long): Array[String] = {
    val candidates = blockLocations.map {
      // The fragment starts from a position within this block
      case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>
        b.getHosts -> (b.getOffset + b.getLength - offset).min(length)

      // The fragment ends at a position within this block
      case b if offset <= b.getOffset && offset + length < b.getLength =>
        b.getHosts -> (offset + length - b.getOffset).min(length)

      // The fragment fully contains this block
      case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>
        b.getHosts -> b.getLength

      // The fragment doesn't intersect with this block
      case b =>
        b.getHosts -> 0L
    }.filter { case (hosts, size) =>
      size > 0L
    }

    if (candidates.isEmpty) {
      Array.empty[String]
    } else {
      val (hosts, _) = candidates.maxBy { case (_, size) => size }
      hosts
    }
  }

  override def clone(): SparkPlan = {
    val plan = new KylinFileSourceScanExec(
      relation,
      output,
      requiredSchema,
      partitionFilters,
      optionalShardSpec,
      optionalNumCoalescedBuckets,
      dataFilters,
      tableIdentifier,
      disableBucketedScan,
      sourceScanRows
    )
    plan.copyTagsFrom(this)
    plan
  }

}
