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
package org.apache.kylin.query.runtime

import java.util.concurrent.TimeUnit

import org.apache.kylin.shaded.com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.TableMetadataManager
import org.apache.kylin.query.DeriveTableColumnInfo
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparderContext}
import org.apache.spark.sql.utils.SparkTypeUtil

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

  def create(
    name: String,
    sourcePath: String,
    kylinConfig: KylinConfig): Dataset[Row] = {
    val names = name.split("@")
    val projectName = names.apply(0)
    val tableName = names.apply(1)
    val metaMgr = TableMetadataManager.getInstance(kylinConfig)
    val tableDesc = metaMgr.getTableDesc(tableName, projectName)
    val columns = tableDesc.getColumns
    val dfTableName = Integer.toHexString(System.identityHashCode(name))
    val schema = StructType(Range(0, columns.size).map(
      index => {
        StructField(DeriveTableColumnInfo(dfTableName,
          index,
          columns(index).getName).toString,
          SparkTypeUtil.toSparkType(columns(index).getType))
      }))
    val rsourcePath = kylinConfig.getHdfsWorkingDirectory + sourcePath
    SparderContext.getSparkSession.read
      .schema(StructType(tableDesc.getColumns.map(column => StructField(column.getName, SparkTypeUtil.toSparkType(column.getType))).toSeq))
      .parquet(rsourcePath)
      .toDF(schema.fieldNames: _*)
  }

  def getOrCreate(
    name: String,
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
