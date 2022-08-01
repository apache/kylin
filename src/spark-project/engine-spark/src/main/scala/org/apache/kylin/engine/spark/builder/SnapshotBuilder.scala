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
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, Executors}
import java.util.{Objects, UUID}
import com.google.common.collect.Maps
import org.apache.kylin.engine.spark.NSparkCubingEngine
import org.apache.kylin.engine.spark.job.{DFChooser, KylinBuildEnv}
import org.apache.kylin.engine.spark.utils.{FileNames, LogUtils}
import org.apache.kylin.metadata.model.{NDataModel, NTableMetadataManager}
import org.apache.kylin.metadata.project.NProjectManager
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig
import org.apache.kylin.common.persistence.transaction.UnitOfWork
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.metadata.model.{TableDesc, TableExtDesc}
import org.apache.kylin.source.SourceFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.utils.ResourceDetectUtils
import org.apache.spark.sql.{Dataset, Encoders, Row, SparderEnv, SparkSession}
import org.apache.spark.utils.ProxyThreadUtils

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.Breaks._
import scala.util.{Failure, Success, Try}

class SnapshotBuilder(var jobId: String) extends Logging with Serializable {

  def this() = this(null)

  private val MD5_SUFFIX = ".md5"
  private val PARQUET_SUFFIX = ".parquet"
  private val MB = 1024 * 1024
  protected val kylinConfig = KylinConfig.getInstanceFromEnv
  protected val needCollectStat = KapConfig.getInstanceFromEnv.isRecordSourceUsage;

  @transient
  private val parquetPathFilter: PathFilter = new PathFilter {
    override def accept(path: Path): Boolean = {
      path.getName.endsWith(PARQUET_SUFFIX)
    }
  }

  @transient
  private val md5PathFilter: PathFilter = new PathFilter {
    override def accept(path: Path): Boolean = {
      path.getName.endsWith(MD5_SUFFIX)
    }
  }

  @throws[IOException]
  def buildSnapshot(ss: SparkSession, toBuildTables: java.util.Set[TableDesc]): Unit = {
    buildSnapshot(ss, toBuildTables.asScala.toSet)
  }

  @throws[IOException]
  def buildSnapshot(ss: SparkSession, model: NDataModel, ignoredSnapshotTables: util.Set[String]): Unit = {
    val toBuildTableDesc = distinctTableDesc(model, ignoredSnapshotTables)
    buildSnapshot(ss, toBuildTableDesc)
  }

  @throws[IOException]
  private def buildSnapshot(ss: SparkSession, tables: Set[TableDesc]): Unit = {
    val baseDir = KapConfig.getInstanceFromEnv.getMetadataWorkingDirectory
    val toBuildTables = tables
    val kylinConf = KylinConfig.getInstanceFromEnv
    if (toBuildTables.isEmpty) {
      return
    }

    // scalastyle:off
    val resultMap = executeBuildSnapshot(ss, toBuildTables, baseDir, kylinConf.isSnapshotParallelBuildEnabled, kylinConf.snapshotParallelBuildTimeoutSeconds)

    // when all snapshots skipped, no need to update meta.
    if (resultMap.isEmpty) {
      return
    }

    // update metadata
    // maybe partial snapshots skipped
    updateMeta(toBuildTables.filter(tbl => resultMap.containsKey(tbl.getIdentity)), resultMap)
  }

  // scalastyle:of
  private def updateMeta(toBuildTableDesc: Set[TableDesc], resultMap: util.Map[String, Result]): Unit = {
    val project = toBuildTableDesc.iterator.next.getProject
    toBuildTableDesc.foreach(table => {
      updateTableSnapshot(project, table, resultMap)
      updateTable(project, table.getIdentity, resultMap)
    }
    )
  }

  private def updateTableSnapshot(project: String, table: TableDesc, resultMap: util.Map[String, Result]): Unit = {
    // define the updating operations
    class TableUpdateOps extends UnitOfWork.Callback[TableDesc] {
      override def process(): TableDesc = {
        val tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv, project)
        val copy = tableMetadataManager.copyForWrite(table)
        copy.setLastSnapshotPath(resultMap.get(copy.getIdentity).path)
        val fs = HadoopUtil.getWorkingFileSystem
        val baseDir = KapConfig.getInstanceFromEnv.getMetadataWorkingDirectory
        var snapshotSize = 0L
        val fullSnapShotPath = baseDir + copy.getLastSnapshotPath
        try {
          snapshotSize = HadoopUtil.getContentSummary(fs, new Path(fullSnapShotPath)).getLength
        } catch {
          case e: Throwable => logWarning(s"Fetch snapshot size for ${copy.getIdentity} from ${fullSnapShotPath}", e)
        }
        copy.setLastSnapshotSize(snapshotSize)
        copy.setSnapshotLastModified(System.currentTimeMillis)
        tableMetadataManager.updateTableDesc(copy)
        copy
      }
    }
    UnitOfWork.doInTransactionWithRetry(new TableUpdateOps, project)
  }

  private def updateTable(project: String, table: String, map: util.Map[String, Result]): Unit = {
    // define the updating operations
    class TableUpdateOps extends UnitOfWork.Callback[TableExtDesc] {
      override def process(): TableExtDesc = {
        val tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv, project)
        var tableExt = tableMetadataManager.getOrCreateTableExt(table)
        tableExt = tableMetadataManager.copyForWrite(tableExt)
        var tableDesc = tableMetadataManager.getTableDesc(table);
        var tableDescCopy = tableMetadataManager.copyForWrite(tableDesc)

        val result = map.get(tableDescCopy.getIdentity)
        if (result.totalRows != -1) {
          tableExt.setOriginalSize(result.originalSize)
          tableExt.setTotalRows(result.totalRows)
          tableDescCopy.setSnapshotTotalRows(result.totalRows)
        }
        tableMetadataManager.saveTableExt(tableExt)
        tableMetadataManager.updateTableDesc(tableDescCopy)
        tableExt
      }
    }
    UnitOfWork.doInTransactionWithRetry(new TableUpdateOps, project)
  }

  @throws[IOException]
  def calculateTotalRows(ss: SparkSession, model: NDataModel, ignoredSnapshotTables: util.Set[String]): Unit = {
    if (!needCollectStat) {
      return
    }
    val toCalculateTableDesc = toBeCalculateTableDesc(model, ignoredSnapshotTables)
    val map = new ConcurrentHashMap[String, Result]

    toCalculateTableDesc.foreach(tableDesc => {
      val totalRows = calculateTableTotalRows(tableDesc.getLastSnapshotPath, tableDesc, ss)
      map.put(tableDesc.getIdentity, Result("", -1, totalRows))
    })

    toCalculateTableDesc.foreach(table => {
      updateTable(model.getProject, table.getIdentity, map)
    })
  }

  def calculateTableTotalRows(snapshotPath: String, tableDesc: TableDesc, ss: SparkSession): Long = {
    val baseDir = KapConfig.getInstanceFromEnv.getMetadataWorkingDirectory
    val fs = HadoopUtil.getWorkingFileSystem
    try {
      if (snapshotPath != null) {
        val path = new Path(baseDir, snapshotPath)
        if (fs.exists(path)) {
          logInfo(s"Calculate table ${tableDesc.getIdentity}'s total rows from snapshot ${path}")
          val totalRows = ss.read.parquet(path.toString).count()
          logInfo(s"Table ${tableDesc.getIdentity}'s total rows is ${totalRows}'")
          return totalRows
        }
      }
    } catch {
      case e: Throwable => logWarning(s"Calculate table ${tableDesc.getIdentity}'s total rows exception", e)
    }
    logInfo(s"Calculate table ${tableDesc.getIdentity}'s total rows from source data")
    val sourceData = getSourceData(ss, tableDesc)
    val totalRows = sourceData.count()
    logInfo(s"Table ${tableDesc.getIdentity}'s total rows is ${totalRows}'")
    totalRows
  }

  // scalastyle:off
  def executeBuildSnapshot(ss: SparkSession, toBuildTableDesc: Set[TableDesc], baseDir: String,
                           isParallelBuild: Boolean, snapshotParallelBuildTimeoutSeconds: Int): util.Map[String, Result] = {
    val snapSizeMap = Maps.newConcurrentMap[String, Result]
    val fs = HadoopUtil.getWorkingFileSystem
    val kylinConf = KylinConfig.getInstanceFromEnv
    val project = toBuildTableDesc.iterator.next.getProject
    val stepCheckpoint = getStepCheckpoint(kylinConf.getJobTmpDir(project), fs)

    if (isParallelBuild) {
      val service = Executors.newCachedThreadPool()
      implicit val executorContext = ExecutionContext.fromExecutorService(service)
      val futures = toBuildTableDesc.map(tableDesc =>
        Future {
          var config: SetAndUnsetThreadLocalConfig = null
          try {
            if (stepCheckpoint.exists(_.canSkip(tableDesc))) {
              logInfo(s"Skip snapshot ${tableDesc.getIdentity}")
            } else {
              config = KylinConfig.setAndUnsetThreadLocalConfig(kylinConf)
              buildSingleSnapshotWithoutMd5(ss, tableDesc, baseDir, snapSizeMap)
              // do step checkpoint
              stepCheckpoint.map(_.checkpoint(tableDesc))
            }
          } catch {
            case exception: Exception =>
              logError(s"Error for build snapshot table with $tableDesc", exception)
              throw exception
          } finally {
            if (config != null) {
              config.close()
            }
          }
        }
      )
      try {
        val eventualTuples = Future.sequence(futures.toList)
        // only throw the first exception
        ProxyThreadUtils.awaitResult(eventualTuples, snapshotParallelBuildTimeoutSeconds seconds)
      } catch {
        case e: Exception =>
          ProxyThreadUtils.shutdown(service)
          throw e
      }
    } else {
      toBuildTableDesc.foreach(tableDesc => {
        if (stepCheckpoint.exists(_.canSkip(tableDesc))) {
          logInfo(s"Skip snapshot ${tableDesc.getIdentity}")
        } else {
          buildSingleSnapshot(ss, tableDesc, baseDir, fs, snapSizeMap)
          // do step checkpoint
          stepCheckpoint.map(_.checkpoint(tableDesc))
        }
      })
    }
    snapSizeMap
  }


  private def isIgnoredSnapshotTable(tableDesc: TableDesc, ignoredSnapshotTables: java.util.Set[String]): Boolean = {
    if (ignoredSnapshotTables == null || tableDesc.getLastSnapshotPath == null) {
      return false
    }
    ignoredSnapshotTables.contains(tableDesc.getIdentity)

  }

  private def toBeCalculateTableDesc(model: NDataModel, ignoredSnapshotTables: java.util.Set[String]): Set[TableDesc] = {
    val project = model.getRootFactTable.getTableDesc.getProject
    val tableManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv, project)
    val toBuildTableDesc = model.getJoinTables.asScala
      .filter(lookupDesc => {
        val tableDesc = lookupDesc.getTableRef.getTableDesc
        val isLookupTable = model.isLookupTable(lookupDesc.getTableRef)
        isLookupTable && !isIgnoredSnapshotTable(tableDesc, ignoredSnapshotTables)
      })
      .map(_.getTableRef.getTableDesc)
      .filter(tableDesc => {
        val tableExtDesc = tableManager.getTableExtIfExists(tableDesc)
        tableExtDesc == null || tableExtDesc.getTotalRows == 0L
      })
      .toSet

    val toBuildTableDescTableName = toBuildTableDesc.map(_.getIdentity)
    logInfo(s"table to be calculate total rows: $toBuildTableDescTableName")
    toBuildTableDesc
  }

  def distinctTableDesc(model: NDataModel, ignoredSnapshotTables: java.util.Set[String]): Set[TableDesc] = {
    val toBuildTableDesc = model.getJoinTables.asScala
      .filter(lookupDesc => {
        val tableDesc = lookupDesc.getTableRef.getTableDesc
        val isLookupTable = model.isLookupTable(lookupDesc.getTableRef)
        isLookupTable && !isIgnoredSnapshotTable(tableDesc, ignoredSnapshotTables)
      })
      .map(_.getTableRef.getTableDesc)
      .toSet

    val toBuildTableDescTableName = toBuildTableDesc.map(_.getIdentity)
    logInfo(s"table snapshot to be build: $toBuildTableDescTableName")
    toBuildTableDesc
  }

  def getSourceData(ss: SparkSession, tableDesc: TableDesc): Dataset[Row] = {
    val params = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv)
      .getProject(tableDesc.getProject).getLegalOverrideKylinProps
    SourceFactory
      .createEngineAdapter(tableDesc, classOf[NSparkCubingEngine.NSparkCubingSource])
      .getSourceData(tableDesc, ss, params)
  }

  def getFileMd5(file: FileStatus): String = {
    val dfs = HadoopUtil.getWorkingFileSystem
    val in = dfs.open(file.getPath)
    Try(DigestUtils.md5Hex(in)) match {
      case Success(md5) =>
        in.close()
        md5
      case Failure(error) =>
        in.close()
        logError(s"building snapshot get file: ${file.getPath} md5 error,msg: ${error.getMessage}")
        throw new IOException(s"Failed to generate file: ${file.getPath} md5 ", error)
    }
  }

  def buildSingleSnapshot(ss: SparkSession, tableDesc: TableDesc, baseDir: String, fs: FileSystem, resultMap: util.Map[String, Result]): Unit = {
    val sourceData = getSourceData(ss, tableDesc)
    val tablePath = FileNames.snapshotFile(tableDesc)
    var snapshotTablePath = tablePath + "/" + UUID.randomUUID
    val resourcePath = baseDir + "/" + snapshotTablePath
    sourceData.coalesce(1).write.parquet(resourcePath)
    val (originSize, totalRows) = computeSnapshotSize(sourceData)
    val currSnapFile = fs.listStatus(new Path(resourcePath), parquetPathFilter).head
    val currSnapMd5 = getFileMd5(currSnapFile)
    val md5Path = resourcePath + "/" + "_" + currSnapMd5 + MD5_SUFFIX

    var isReuseSnap = false
    val existPath = baseDir + "/" + tablePath
    val existSnaps = fs.listStatus(new Path(existPath))
      .filterNot(_.getPath.getName == new Path(snapshotTablePath).getName)
    breakable {
      for (snap <- existSnaps) {
        Try(fs.listStatus(snap.getPath, md5PathFilter)) match {
          case Success(list) =>
            list.headOption match {
              case Some(file) =>
                val md5Snap = file.getPath.getName
                  .replace(MD5_SUFFIX, "")
                  .replace("_", "")
                if (currSnapMd5 == md5Snap) {
                  snapshotTablePath = tablePath + "/" + snap.getPath.getName
                  fs.delete(new Path(resourcePath), true)
                  isReuseSnap = true
                  break()
                }
              case None =>
                logInfo(s"Snapshot path: ${snap.getPath} not exists snapshot file")
            }
          case Failure(error) =>
            logInfo(s"File not found", error)
        }
      }
    }

    if (!isReuseSnap) {
      fs.createNewFile(new Path(md5Path))
      logInfo(s"Create md5 file: ${md5Path} for snap: ${currSnapFile}")
    }

    resultMap.put(tableDesc.getIdentity, Result(snapshotTablePath, originSize, totalRows))
  }

  def buildSingleSnapshotWithoutMd5(ss: SparkSession, tableDesc: TableDesc, baseDir: String,
                                    resultMap: ConcurrentMap[String, Result]): Unit = {
    val sourceData = getSourceData(ss, tableDesc)
    val tablePath = FileNames.snapshotFile(tableDesc)
    val snapshotTablePath = tablePath + "/" + UUID.randomUUID
    val resourcePath = baseDir + "/" + snapshotTablePath
    val (repartitionNum, sizeMB) = try {
      val sizeInMB = ResourceDetectUtils.getPaths(sourceData.queryExecution.sparkPlan)
        .map(path => HadoopUtil.getContentSummary(path.getFileSystem(SparderEnv.getHadoopConfiguration()), path).getLength)
        .sum * 1.0 / MB
      val num = Math.ceil(sizeInMB / KylinBuildEnv.get().kylinConfig.getSnapshotShardSizeMB).intValue()
      (num, sizeInMB)
    } catch {
      case t: Throwable =>
        logWarning("Error occurred when estimate repartition number.", t)
        (0, 0)
    }
    ss.sparkContext.setJobDescription(s"Build table snapshot ${tableDesc.getIdentity}.")
    lazy val snapshotInfo = Map(
      "source" -> tableDesc.getIdentity,
      "snapshot" -> snapshotTablePath,
      "sizeMB" -> sizeMB,
      "partition" -> repartitionNum
    )
    logInfo(s"Building snapshot: ${LogUtils.jsonMap(snapshotInfo)}")
    if (repartitionNum == 0) {
      sourceData.write.parquet(resourcePath)
    } else {
      sourceData.repartition(repartitionNum).write.parquet(resourcePath)
    }
    val (originSize, totalRows) = computeSnapshotSize(sourceData)
    resultMap.put(tableDesc.getIdentity, Result(snapshotTablePath, originSize, totalRows))
  }

  private[builder] def computeSnapshotSize(sourceData: Dataset[Row]): (Long, Long) = {
    if (!needCollectStat) {
      return (-1L, -1L)
    }
    val columnSize = sourceData.columns.length
    val ds = sourceData.mapPartitions {
      iter =>
        var totalSize = 0L
        var totalRows = 0L
        iter.foreach(row => {
          for (i <- 0 until columnSize) {
            val value = row.get(i)
            val strValue = if (value == null) null
            else value.toString
            totalSize += DFChooser.utf8Length(strValue)
          }
          totalRows += 1
        })
        List((totalSize, totalRows)).toIterator
    }(Encoders.tuple(Encoders.scalaLong, Encoders.scalaLong))

    if (ds.isEmpty) {
      (0L, 0L)
    } else {
      ds.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    }
  }

  private[builder] def wrapConfigExecute[R](callable: () => R, taskInfo: String): R = {
    var config: SetAndUnsetThreadLocalConfig = null
    try {
      config = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig)
      callable.apply()
    } catch {
      case exception: Exception =>
        logError(s"Error for build snapshot table with $taskInfo", exception)
        throw exception
    } finally {
      if (config != null) {
        config.close();
      }
    }
  }

  private[builder] def decideSparkJobArg(sourceData: Dataset[Row]): (Int, Double) = {
    try {
      val sizeInMB = ResourceDetectUtils.getPaths(sourceData.queryExecution.sparkPlan)
        .map(path => HadoopUtil.getContentSummary(path.getFileSystem(SparderEnv.getHadoopConfiguration()), path).getLength)
        .sum * 1.0 / MB
      val num = Math.ceil(sizeInMB / KylinBuildEnv.get().kylinConfig.getSnapshotShardSizeMB).intValue()
      (num, sizeInMB)
    } catch {
      case t: Throwable =>
        logWarning("Error occurred when estimate repartition number.", t)
        (0, 0D)
    }
  }

  private def getStepCheckpoint(jobTmp: String, fs: FileSystem): Option[StepCheckpointSnapshot] = {
    if (Objects.isNull(jobId)) {
      logInfo("jobId is null, wouldn't checkpoint snapshot step.")
      None
    } else {
      logInfo(s"jobId $jobId, would checkpoint snapshot step.")
      Some(new StepCheckpointSnapshot(s"$jobTmp$jobId", fs))
    }
  }

  case class Result(path: String, originalSize: Long, totalRows: Long)

}
