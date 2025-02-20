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

import org.apache.kylin.common.persistence.transaction.{UnitOfWork, UnitOfWorkParams}
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.engine.spark.utils.LogUtils
import org.apache.kylin.metadata.datatype.DataType
import org.apache.kylin.metadata.model.{NTableMetadataManager, TableDesc, TableExtDesc}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.utils.ProxyThreadUtils

import java.io.IOException
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class SnapshotPartitionBuilder extends SnapshotBuilder {

  @throws[IOException]
  def buildSnapshot(ss: SparkSession, table: TableDesc, partitionCol: String, partitions: java.util.Set[String]): Unit = {
    executeBuildSnapshot(ss, table, partitionCol, partitions.asScala.toSet)
  }

  def checkPointForPartition(project: String, tableName: String, results: List[(String, Result)]): Unit = {
    // define the updating operations
    class TableUpdateOps extends UnitOfWork.Callback[TableDesc] {
      override def process(): TableDesc = {
        val tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv, project)
        val originTable = tableMetadataManager.getTableDesc(tableName)
        tableMetadataManager.updateTableExt(tableName, (copyForWrite: TableExtDesc) => {
          results.foreach(item => {
            val partition = item._1
            val result = item._2
            if (result.totalRows != -1) {
              copyForWrite.setTotalRows(copyForWrite.getTotalRows + result.totalRows - originTable.getPartitionRow(partition))
            }
          })
        })
        tableMetadataManager.updateTableDesc(tableName, (copyForWrite: TableDesc) => {
          results.foreach(item => {
            val partition = item._1
            val result = item._2
            if (result.totalRows != -1) {
              copyForWrite.putPartitionSize(partition, result.originalSize)
              copyForWrite.setSnapshotTotalRows(copyForWrite.getSnapshotTotalRows + result.totalRows -
                copyForWrite.getPartitionRow(partition))
              copyForWrite.putPartitionRow(partition, result.totalRows)
            } else {
              // -1 in partitionSize means not build
              copyForWrite.putPartitionSize(partition, 0)
              copyForWrite.putPartitionRow(partition, 0)
            }
          })
        })
        tableMetadataManager.getTableDesc(tableName)
      }
    }
    val params = UnitOfWorkParams.builder.unitName(project).maxRetry(3).useProjectLock(true)
      .processor((new TableUpdateOps).asInstanceOf[UnitOfWork.Callback[Nothing]]).build()
    UnitOfWork.doInTransactionWithRetry(params)
    log.info(s"check point partitions for $tableName , partition size: ${results.size}")
  }

  def executeBuildSnapshot(ss: SparkSession, table: TableDesc, partitionCol: String, partitions: Set[String]): Unit = {
    val baseDir = KapConfig.getInstanceFromEnv.getMetadataWorkingDirectory
    val resourcePath = table.getTempSnapshotPath
    val snapshotTablePath = baseDir + '/' + resourcePath

    val kylinConf = KylinConfig.getInstanceFromEnv
    val snapshotParallelBuildTimeoutSeconds = kylinConf.snapshotParallelBuildTimeoutSeconds()
    val maxThread = if (kylinConf.snapshotPartitionBuildMaxThread() >= 2) kylinConf.snapshotPartitionBuildMaxThread() else 2
    val service = Executors.newFixedThreadPool(maxThread)
    implicit val executorContext = ExecutionContext.fromExecutorService(service)

    val futures = partitions.map { partition =>
      Future {
        wrapConfigExecute[(String, Result)](() => {
          (partition, buildSingleSnapshotWithoutMd5(ss, table, partitionCol, partition, snapshotTablePath))
        }, table.getIdentity + ":" + partition)
      }
    }

    try {
      val eventualTuples = Future.sequence(futures.toList)
      // only throw the first exception
      val results = ProxyThreadUtils.awaitResult(eventualTuples, snapshotParallelBuildTimeoutSeconds seconds)
      checkPointForPartition(table.getProject, table.getIdentity, results)
    } finally {
      ProxyThreadUtils.shutdown(service)
    }
  }


  def newFilter(partitionCol: String, partition: String, colType: DataType): String = {
    if (colType.isDate) {
      "`" + partitionCol + "`" + "= cast('" + partition + "' as date)"
    } else if (colType.isNumberFamily) {
      "`" + partitionCol + "`" + "= " + partition + ""
    } else {
      "`" + partitionCol + "`" + "= '" + partition + "'"
    }
  }

  def buildSingleSnapshotWithoutMd5(ss: SparkSession, tableDesc: TableDesc,
                                    partitionCol: String, partition: String, snapshotTablePath: String): Result = {
    var sourceData = getSourceData(ss, tableDesc)
    sourceData = sourceData.filter(newFilter(partitionCol, partition, tableDesc.findColumnByName(partitionCol).getType))

    sourceData = sourceData.selectExpr(sourceData.columns.filter(!_.equals(partitionCol)).map("`" + _ + "`"): _*)

    var newPartition = partition.replaceAll(" ", "_")
    newPartition = newPartition.replaceAll(":", "_")

    val partitionName = partitionCol + '=' + newPartition
    val resourcePath = snapshotTablePath + "/" + partitionName


    val (repartitionNum, sizeMB) = decideSparkJobArg(sourceData)

    ss.sparkContext.setJobDescription(s"Build table snapshot ${tableDesc.getIdentity}.")
    lazy val snapshotInfo = Map(
      "source" -> tableDesc.getIdentity,
      "snapshot" -> snapshotTablePath,
      "sizeMB" -> sizeMB,
      "partition" -> repartitionNum,
      "buildPartition" -> partition
    )
    logInfo(s"Building snapshot: ${LogUtils.jsonMap(snapshotInfo)}")


    if (repartitionNum == 0) {
      sourceData.write.mode(SaveMode.Overwrite).parquet(resourcePath)
    } else {
      sourceData.repartition(repartitionNum).write.mode(SaveMode.Overwrite).parquet(resourcePath)
    }
    val (originSize, totalRows) = computeSnapshotSize(sourceData)
    Result(snapshotTablePath, originSize, totalRows)
  }

}
