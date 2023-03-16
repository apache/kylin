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

import java.util.concurrent.TimeUnit
import org.apache.kylin.guava30.shaded.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.metadata.model.NTableMetadataManager
import org.apache.kylin.metadata.model.ColumnDesc
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.utils.DeriveTableColumnInfo
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparderEnv}
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.kylin.query.util.PartitionsFilter.PARTITION_COL
import org.apache.kylin.query.util.PartitionsFilter.PARTITIONS

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

// scalastyle:off
object SparderLookupManager extends Logging {
  val DEFAULT_MAXSIZE = 100
  val DEFAULT_EXPIRE_TIME = 1
  val DEFAULT_TIME_UNIT = TimeUnit.HOURS

  val sourceCache: Cache[String, Dataset[Row]] = CacheBuilder.newBuilder
    .maximumSize(DEFAULT_MAXSIZE)
    .expireAfterWrite(DEFAULT_EXPIRE_TIME, DEFAULT_TIME_UNIT)
    .removalListener(new RemovalListener[String, Dataset[Row]]() {
      override def onRemoval(
                              notification: RemovalNotification[String, Dataset[Row]]): Unit = {
        logInfo("Remove lookup table from spark : " + notification.getKey)
        notification.getValue.unpersist()
      }
    })
    .build
    .asInstanceOf[Cache[String, Dataset[Row]]]

  def create(name: String,
             sourcePath: String,
             kylinConfig: KylinConfig): Dataset[Row] = {
    val names = name.split("@")
    val projectName = names.apply(0)
    val tableName = names.apply(1)
    val metaMgr = NTableMetadataManager.getInstance(kylinConfig, projectName)
    val tableDesc = metaMgr.getTableDesc(tableName)
    val cols = tableDesc.getColumns.toList

    var columns = cols.filter(col => !col.getName.equals(tableDesc.getSnapshotPartitionCol))
    if (tableDesc.getSnapshotPartitionCol != null) {
      columns = columns :+ tableDesc.findColumnByName(tableDesc.getSnapshotPartitionCol)
    }
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
    val options = new scala.collection.mutable.HashMap[String, String]
    if (partitionCol != null) {
      orderedCol.append(partitionCol)
      options.put(PARTITION_COL, tableDesc.getSnapshotPartitionCol)
      options.put(PARTITIONS, String.join(",", tableDesc.getSnapshotPartitions.keySet()))
      options.put("mapreduce.input.pathFilter.class", "org.apache.kylin.query.util.PartitionsFilter")
    }
    val originSchema = StructType(orderedCol.map { case (col, index) => StructField(col.getName, SparderTypeUtil.toSparkType(col.getType)) })
    val schema = StructType(orderedCol.map { case (col, index) => StructField(DeriveTableColumnInfo(dfTableName, index, col.getName).toString, SparderTypeUtil.toSparkType(col.getType)) })
    val resourcePath = KapConfig.getInstanceFromEnv.getReadHdfsWorkingDirectory + sourcePath

    SparderEnv.getSparkSession.read.options(options)
      .schema(originSchema)
      .parquet(resourcePath)
      .toDF(schema.fieldNames: _*)
  }

  def getOrCreate(name: String,
                  sourcePath: String,
                  kylinConfig: KylinConfig): DataFrame = {
    val value = sourceCache.getIfPresent(sourcePath)
    if (value != null) {
      value
    } else {
      create(name, sourcePath, kylinConfig)
    }
  }
}
