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

package org.apache.kylin.engine.spark.builder

import java.io.IOException
import java.util
import java.util.Locale

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.exception.{CommonErrorCode, KylinException}
import org.apache.kylin.common.util.{DateFormat, HadoopUtil}
import org.apache.kylin.engine.spark.utils.SparkDataSource._
import org.apache.kylin.metadata.cube.model.NBatchConstants
import org.apache.kylin.metadata.table.InternalTableDesc
import org.apache.kylin.metadata.table.InternalTableDesc.StorageType
import org.apache.kylin.metadata.table.InternalTablePartition.DefaultPartitionConditionBuilder
import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.implicits.stringLongEncoder
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}

import io.delta.tables.{ClickhouseTable, DeltaTable}

class InternalTableLoader extends Logging {

  val BUCKET_COLUMN = "clickhouse.bucketColumnNames"
  val BUCKET_NUM = "clickhouse.numBuckets"
  val PRIMARY_KEY = "clickhouse.primaryKey"
  val ORDER_BY_KEY = "clickhouse.orderByKey"
  val STORAGE_POLICY = "clickhouse.storage_policy"
  val OVERWRITE = "overwrite"
  val APPEND = "append"

  private[this] var _onlyLoadSchema: Boolean = false

  private def onlyLoadSchema: Boolean = _onlyLoadSchema

  def onlyLoadSchema(value: Boolean): Unit = {
    _onlyLoadSchema = value
  }


  @throws[IOException]
  def loadInternalTable(ss: SparkSession,
                        table: InternalTableDesc,
                        isRefresh: String,
                        startDate: String,
                        endDate: String,
                        storagePolicy: String,
                        incremental: Boolean): Unit = {
    val location = table.generateInternalTableLocation
    var sourceData = getSourceData(ss, table, startDate, endDate, incremental)
    val tablePartition = table.getTablePartition
    val bucketColumn = table.getBucketColumn
    val bucketNum = table.getBucketNumber
    val primaryKey = table.getTblProperties.get(NBatchConstants.P_PRIMARY_KEY)
    val orderByKey = table.getTblProperties.get(NBatchConstants.P_ORDER_BY_KEY)
    val outPutMode = OVERWRITE

    if (tablePartition != null && table.isSortByPartitionEnabled) {
      val partitionColumn = tablePartition.getPartitionColumns
      sourceData = sourceData.sort(partitionColumn(0), partitionColumn.slice(1, partitionColumn.length): _*)
    }
    var writer = sourceData.write.option(STORAGE_POLICY, storagePolicy)

    if (tablePartition != null) {
      val partitionColumn = tablePartition.getPartitionColumns
      writer = writer.partitionBy(partitionColumn: _*)
    }
    if (bucketColumn != null) {
      if (bucketNum <= 0) {
        throw new KylinException(CommonErrorCode.INTERNAL_TABLE_INVALID_BUCKET_NUMBER, "Invalid bucket number")
      }
      writer = writer.option(BUCKET_COLUMN, bucketColumn)
        .option(BUCKET_NUM, bucketNum)
    }
    // primaryKey must be sub-set and prefix of orderByKey so that orderByKey should be configured together
    if (primaryKey != null) {
      writer = writer.option(PRIMARY_KEY, primaryKey)
        .option(ORDER_BY_KEY, orderByKey)
    } else if (orderByKey != null) {
      writer = writer.option(ORDER_BY_KEY, orderByKey)
    }
    val format = table.getStorageType.getFormat
    if (incremental) {
      val dateFormat = table.getTablePartition.getDatePartitionFormat
      logInfo(f"Refresh dynamic partitions [${DateFormat.formatToDateStr(startDate.toLong, dateFormat)}," +
        f" ${DateFormat.formatToDateStr(endDate.toLong, dateFormat)})")
      ss.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    }
    writer.format(format).mode(outPutMode).save(location)
  }

  def getSourceData(ss: SparkSession,
                    table: InternalTableDesc,
                    startDate: String,
                    endDate: String,
                    incremental: Boolean): Dataset[Row] = {
    val tableDS = if (this.onlyLoadSchema) ss.table(table.getTableDesc).limit(0) else ss.table(table.getTableDesc)
    if (incremental) {
      val partitionColumn = table.getTablePartition.getPartitionColumns()(0)
      val dateFormat = table.getTablePartition.getDatePartitionFormat
      val condition = DefaultPartitionConditionBuilder.buildDateRangeCondition(partitionColumn, dateFormat,
        startDate, endDate)
      tableDS.where(condition)
    } else {
      tableDS
    }
  }


  def truncateDataInFileSystem(locations: util.ArrayList[String], isInternalTableRootPath: Boolean = true): Unit = {
    val fs = HadoopUtil.getWorkingFileSystem
    locations.forEach(location => {
      try {
        val filePath = new Path(location)
        logInfo(s"Trying to delete $filePath")
        if (fs.exists(filePath)) {
          if (!isInternalTableRootPath) {
            // delete partition dirs
            fs.delete(filePath, true)
            logInfo(s"Deleted: $filePath")
          } else {
            val statuses = fs.listStatus(filePath)
            for (status <- statuses) {
              if (!status.getPath.getName.equals("_delta_log") || !status.getPath.getName.startsWith("_")) {
                fs.delete(status.getPath, true)
                logInfo(s"Deleted: ${status.getPath}")
              }
            }

          }
        } else {
          logInfo(s"$filePath does not exist, skip it")
        }
      } catch {
        case e: IOException =>
          logError(f"Failed to delete internal table on $location", e)
      }
    })
  }

  // return Array[(partitionValue, storageSize, fileCount), ...]
  def getPartitionInfos(ss: SparkSession, internalTable: InternalTableDesc): Array[Row] = {
    val internalTableDeltaLog = DeltaLog.forTable(ss, internalTable.getLocation)
    val allFiles = internalTableDeltaLog.unsafeVolatileSnapshot.allFiles
    val partitionCol = internalTable.getTablePartition.getPartitionColumns()(0).toUpperCase(Locale.ROOT)
    val partitionInfos = allFiles.filter(_.partitionValues.contains(partitionCol))
      .map(addFile => (addFile.partitionValues(partitionCol), addFile.size))
    val groupedSizeSum = partitionInfos
      .groupBy("_1")
      .agg(functions.sum("_2").alias("totalSize"), functions.count("_2").alias("rowCount"))
    groupedSizeSum.collect()
  }

  @throws[IOException]
  def dropPartitions(ss: SparkSession,
                     internalTable: InternalTableDesc,
                     partitionValues: String): Unit = {
    val sparkTable = internalTable.getStorageType match {
      case StorageType.GLUTEN => ClickhouseTable.forPath(ss, internalTable.generateInternalTableLocation)
      case StorageType.DELTALAKE => DeltaTable.forPath(ss, internalTable.generateInternalTableLocation)
      case _ => null
    }
    if (StringUtils.isEmpty(partitionValues)) {
      logInfo(s"Dropping all partitions for table: $internalTable")
      deleteDeltaMetaData(sparkTable)
      val locations = new util.ArrayList[String]()
      locations.add(internalTable.getLocation)
      truncateDataInFileSystem(locations)
    } else {
      val partitionCol = internalTable.getTablePartition.getPartitionColumns()(0)
      val toDeletedPaths = new util.ArrayList[String]()
      val values = partitionValues.split(",")
      val deleteStatementBuilder = StringBuilder.newBuilder
      val toDeletedPartitionValues = new util.ArrayList[String]()
      values.foreach {
        partitionValue =>
          deleteStatementBuilder.clear()
          deleteStatementBuilder.append(partitionCol)
            .append(" = ")
            .append("'" + partitionValue + "'")
          val subPath = partitionCol.toUpperCase(Locale.ROOT) + "=" + partitionValue
          val pathName = new Path(internalTable.getLocation, subPath).toString
          toDeletedPaths.add(pathName)
          toDeletedPartitionValues.add(deleteStatementBuilder.toString())
      }
      if (!toDeletedPartitionValues.isEmpty) {
        val partitionCondition = StringUtils.join(toDeletedPartitionValues, " or ")
        logInfo(s"Dropping partitions for table: $internalTable, partition condition: $partitionCondition")
        deleteDeltaMetaData(sparkTable, partitionCondition)
      }
      if (!toDeletedPaths.isEmpty) {
        truncateDataInFileSystem(toDeletedPaths, isInternalTableRootPath = false)
      }
    }

    //    vacuum takes a long time to execute. Temporarily, use filesystem delete to delete files
    //    logInfo(s"Deleting unused files for table: $internalTable")
    //    ss.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    //    clickhouseTable.vacuum(0)
  }

  private def deleteDeltaMetaData(table: DeltaTable, partition: String = null): Unit = {
    if (table != null) {
      if (partition != null) {
        table.delete(partition)
      } else {
        table.delete()
      }
    }
  }
}
