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
//package org.apache.spark.sql.execution.datasource
//
//import io.kyligence.kap.engine.spark.job.NSparkCubingUtil
//import io.kyligence.kap.metadata.cube.model.NDataSegment
//import org.apache.hadoop.fs.Path
//import org.apache.kylin.common.QueryContext
//import org.apache.spark.sql.execution.datasources._
//import org.apache.spark.sql.{AnalysisException, SparkSession}
//import org.apache.spark.sql.catalyst.catalog.{CatalogTable, ExternalCatalogUtils}
//import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BoundReference, Cast, Expression, InterpretedPredicate, Literal, Predicate}
//import org.apache.spark.sql.catalyst.InternalRow
//import org.apache.spark.sql.internal.SQLConf
//import org.apache.spark.sql.types.StructType
//
//import scala.collection.JavaConverters._
//
//class LayoutFileIndex(
//  sparkSession: SparkSession,
//  catalogTable: CatalogTable,
//  override val sizeInBytes: Long,
//  segments: Seq[NDataSegment]) extends CatalogFileIndex(sparkSession, catalogTable, sizeInBytes) with ResetShufflePartition {
//
//  private val fileStatusCache = ShardFileStatusCache.getFileStatusCache(sparkSession)
//
//  lazy val paths: Seq[Path] = {
//    segments
//      .map(seg => new Path(NSparkCubingUtil.getStoragePath(seg, catalogTable.identifier.table.toLong)))
//  }
//
//  override def rootPaths: Seq[Path] = paths
//
//  override def filterPartitions(partitionFilters: Seq[Expression]): InMemoryFileIndex = {
//    val index = if (catalogTable.partitionColumnNames.nonEmpty) {
//      val partitionSchema = catalogTable.partitionSchema
//      val partitionColumnNames = catalogTable.partitionColumnNames.toSet
//      val nonPartitionPruningPredicates = partitionFilters.filterNot {
//        _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
//      }
//      if (nonPartitionPruningPredicates.nonEmpty) {
//        throw new AnalysisException("Expected only partition pruning predicates:" + nonPartitionPruningPredicates)
//      }
//       var partitionPaths = segments
//         .flatMap { seg =>
//           val layout = seg.getLayout(catalogTable.identifier.table.toLong)
//           val data = layout.getPartitionValues
//           val baseDir = NSparkCubingUtil.getStoragePath(seg, layout.getLayoutId)
//           data.asScala.map(dt => (dt, baseDir + "/" + dt))
//         }.map { tp =>
//        val spec = PartitioningUtils.parsePathFragment(tp._1)
//        val row = InternalRow.fromSeq(partitionSchema.map { field =>
//          val partValue = if (spec(field.name) == ExternalCatalogUtils.DEFAULT_PARTITION_NAME) {
//            null
//          } else {
//            spec(field.name)
//          }
//          Cast(Literal(partValue), field.dataType, Option(SQLConf.get.sessionLocalTimeZone)).eval()
//        })
//        PartitionPath(row, new Path(tp._2))
//      }
//      if (partitionFilters.nonEmpty) {
//        val boundPredicate =
//          Predicate.create(partitionFilters.reduce(And).transform {
//            case att: AttributeReference =>
//              val index = partitionSchema.indexWhere(_.name == att.name)
//              BoundReference(index, partitionSchema(index).dataType, nullable = true)
//          })
//        partitionPaths = partitionPaths.filter(partitionPath => boundPredicate.eval(partitionPath.values))
//      }
//      new PrunedInMemoryFileIndex(sparkSession, fileStatusCache, PartitionSpec(partitionSchema, partitionPaths))
//    } else {
//      new InMemoryFileIndex(
//        sparkSession, rootPaths, Map.empty[String, String], userSpecifiedSchema = Option(table.schema))
//    }
//    QueryContext.current().record("partition_pruning")
//    setShufflePartitions($"${data}", index.allFiles().map(_.getLen).sum, sparkSession)
//    QueryContext.current().record("fetch_file_status")
//
//    index
//  }
//
//  override def partitionSchema: StructType = catalogTable.partitionSchema
//}
//private class PrunedInMemoryFileIndex(
//  sparkSession: SparkSession,
//  fileStatusCache: FileStatusCache,
//  override val partitionSpec: PartitionSpec)
//  extends InMemoryFileIndex(
//    sparkSession,
//    partitionSpec.partitions.map(_.path),
//    Map.empty,
//    Some(partitionSpec.partitionColumns),
//    fileStatusCache)