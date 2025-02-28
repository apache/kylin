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
package org.apache.spark.sql.manager

import org.apache.hadoop.fs.Path
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.metadata.model.{ColumnDesc, NTableMetadataManager}
import org.apache.kylin.query.util.PartitionsFilter.{PARTITIONS, PARTITION_COL}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.execution.utils.DeriveTableColumnInfo
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{SparderEnv, SparkOperation}

import scala.collection.mutable.ListBuffer

// scalastyle:off
object SparderLookupManager extends Logging {

  def create(name: String,
             sourcePath: String,
             kylinConfig: KylinConfig): LogicalPlan = {
    val names = name.split("@")
    val projectName = names.apply(0)
    val tableName = names.apply(1)
    val metaMgr = NTableMetadataManager.getInstance(kylinConfig, projectName)
    val tableDesc = metaMgr.getTableDesc(tableName)
    val dfTableName = Integer.toHexString(System.identityHashCode(name))

    val orderedCol = new ListBuffer[(ColumnDesc, Int)]
    var partitionCol: (ColumnDesc, Int) = null
    for ((col, index) <- tableDesc.getColumns.zipWithIndex) {
      if (!col.getName.equals(tableDesc.getSnapshotPartitionCol)) {
        orderedCol.append((col, index))
      } else {
        partitionCol = (col, index)
      }
    }

    val originSchema = StructType(orderedCol.map { case (col, _) => StructField(col.getName, SparderTypeUtil.toSparkType(col.getType)) })
    val sparkSession = SparderEnv.getSparkSession
    val plan = if (sourcePath.contains("/Internal/")) {
      val sql = f"select * from INTERNAL_CATALOG.$projectName.$tableName"
      sparkSession.sql(sql).queryExecution.analyzed
    } else {
      val options = new scala.collection.mutable.HashMap[String, String]
      if (partitionCol != null) {
        orderedCol.append(partitionCol)
        options.put(PARTITION_COL, tableDesc.getSnapshotPartitionCol)
        options.put(PARTITIONS, String.join(",", tableDesc.getSnapshotPartitions.keySet()))
        options.put("mapreduce.input.pathFilter.class", "org.apache.kylin.query.util.PartitionsFilter")
      }
      // create relation
      val resourcePath = new Path(KapConfig.getInstanceFromEnv.getReadHdfsWorkingDirectory + sourcePath)
      val fileIndex = new InMemoryFileIndex(sparkSession, Seq(resourcePath), options.toMap, Option(originSchema))
      val fsRelation = HadoopFsRelation(
        fileIndex,
        partitionSchema = fileIndex.partitionSchema,
        dataSchema = originSchema,
        bucketSpec = None,
        new ParquetFileFormat,
        options.toMap)(sparkSession)
      LogicalRelation(fsRelation)
    }
    val aliasCols = orderedCol.map {
      case (c, index) =>
        col(c.getName).as(DeriveTableColumnInfo(dfTableName, index, c.getName).toString)
    }
    SparkOperation.project(aliasCols, plan)
  }

  def getOrCreate(name: String,
                  sourcePath: String,
                  kylinConfig: KylinConfig): LogicalPlan = {
    create(name, sourcePath, kylinConfig)
  }
}
