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
import java.util.UUID
import java.util.concurrent.Executors

import org.apache.kylin.shaded.com.google.common.collect.Maps
import org.apache.kylin.engine.spark.job.KylinBuildEnv
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.engine.spark.metadata.{SegmentInfo, TableDesc}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.utils.ResourceDetectUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.utils.ProxyThreadUtils
import org.apache.kylin.engine.spark.utils.SparkDataSource._
import org.apache.kylin.engine.spark.utils.FileNames
import org.apache.spark.sql.functions.{count, countDistinct}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.Breaks._
import scala.util.{Failure, Success, Try}

class CubeSnapshotBuilder extends Logging {

  var ss: SparkSession = _
  var seg: SegmentInfo = _

  private val MD5_SUFFIX = ".md5"
  private val PARQUET_SUFFIX = ".parquet"
  private val MB = 1024 * 1024

  def this(seg: SegmentInfo, ss: SparkSession) {
    this()
    this.seg = seg
    this.ss = ss
  }

  private val ParquetPathFilter: PathFilter = new PathFilter {
    override def accept(path: Path): Boolean = {
      path.getName.endsWith(PARQUET_SUFFIX)
    }
  }

  private val Md5PathFilter: PathFilter = new PathFilter {
    override def accept(path: Path): Boolean = {
      path.getName.endsWith(MD5_SUFFIX)
    }
  }

  @throws[IOException]
  def buildSnapshot: SegmentInfo = {
    logInfo(s"Building snapshots for: $seg")
    val newSnapMap = Maps.newHashMap[String, String]
    val fs = HadoopUtil.getWorkingFileSystem
    val kylinConf = seg.kylinconf
    val baseDir = kylinConf.getHdfsWorkingDirectory
    val toBuildTableDesc = seg.snapshotTables
    if (kylinConf.isSnapshotParallelBuildEnabled) {
      val service = Executors.newCachedThreadPool()
      implicit val executorContext = ExecutionContext.fromExecutorService(service)
      val futures = toBuildTableDesc
        .map {
          tableInfo =>
            Future[(String, String)] {
              if (kylinConf.isUTEnv) {
                Thread.sleep(1000L)
              }
              try {
                KylinConfig.setAndUnsetThreadLocalConfig(kylinConf)
                buildSnapshotWithoutMd5(tableInfo, baseDir)
              } catch {
                case exception: Exception =>
                  logError(s"Error for build snapshot table with ${tableInfo.identity}", exception)
                  throw exception
              }
            }
        }
      // scalastyle:off
      try {
        val eventualTuples = Future.sequence(futures.toList)
        // only throw the first exception
        val result = ProxyThreadUtils.awaitResult(eventualTuples, kylinConf.snapshotParallelBuildTimeoutSeconds seconds)
        if (result.nonEmpty) {
          seg.updateSnapshot(result.toMap)
        }
      } catch {
        case e: Exception =>
          ProxyThreadUtils.shutdown(service)
          throw e
      }
    } else {
      toBuildTableDesc.foreach {
        tableDesc =>
          val tuple = buildSingleSnapshot(tableDesc, baseDir, fs)
          newSnapMap.put(tuple._1, tuple._2)
      }
    }
    // make a copy of the changing segment, avoid changing the cached object
    // To do: add snapshots to segment with copy
    seg
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

  def buildSingleSnapshot(tableInfo: TableDesc, baseDir: String, fs: FileSystem): (String, String) = {
    val sourceData = ss.table(tableInfo)
    val tablePath = FileNames.snapshotFile(tableInfo, seg.project)
    var snapshotTablePath = tablePath + "/" + UUID.randomUUID
    val resourcePath = baseDir + "/" + snapshotTablePath
    sourceData.coalesce(1).write.parquet(resourcePath)

    val currSnapFile = fs.listStatus(new Path(resourcePath), ParquetPathFilter).head
    val currSnapMd5 = getFileMd5(currSnapFile)
    val md5Path = resourcePath + "/" + "_" + currSnapMd5 + MD5_SUFFIX

    var isReuseSnap = false
    val existPath = baseDir + "/" + tablePath
    val existSnaps = fs.listStatus(new Path(existPath))
      .filterNot(_.getPath.getName == new Path(snapshotTablePath).getName)
    breakable {
      for (snap <- existSnaps) {
        Try(fs.listStatus(snap.getPath, Md5PathFilter)) match {
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

    (tableInfo.identity, snapshotTablePath)
  }
  import org.apache.kylin.engine.spark.utils.SparkDataSource._

  def checkDupKey() = {
    val joinDescs = seg.joindescs
    joinDescs.foreach {
      joinDesc =>
        val tableInfo = joinDesc.lookupTable
        // Build snapshot when DataModelDesc.JoinTableDesc.TableKind is TableKind.LOOKUP
        if (seg.snapshotTables.exists(t => t.identity.equals(tableInfo.identity))) {
          val lookupTableName = tableInfo.tableName
          val df = ss.table(tableInfo)
          val countColumn = df.count()
          val lookupTablePKS = joinDesc.PKS.map(lookupTablePK => lookupTablePK.columnName)
          val countDistinctColumn = df.agg(countDistinct(lookupTablePKS.head, lookupTablePKS.tail: _*)).collect().map(_.getLong(0)).head
          if (countColumn != countDistinctColumn) {
            throw new IllegalStateException(s"Failed to build lookup table ${lookupTableName} snapshot for Dup key found, key= ${lookupTablePKS.mkString(",")}")
          }
        } else {
          logInfo("Skip check duplicate primary key on table : " + tableInfo.identity)
        }
    }
  }

  def buildSnapshotWithoutMd5(tableInfo: TableDesc, baseDir: String): (String, String) = {
    val sourceData = ss.table(tableInfo)
    val tablePath = FileNames.snapshotFile(tableInfo, seg.project)
    val snapshotTablePath = tablePath + "/" + UUID.randomUUID
    val resourcePath = baseDir + "/" + snapshotTablePath
    val repartitionNum = try {
      val sizeInMB = ResourceDetectUtils.getPaths(sourceData.queryExecution.sparkPlan)
        .map(path => HadoopUtil.getContentSummary(path.getFileSystem(HadoopUtil.getCurrentConfiguration), path).getLength)
        .sum * 1.0 / MB
      val num = Math.ceil(sizeInMB / KylinBuildEnv.get().kylinConfig.getSnapshotShardSizeMB).intValue()
      logInfo(s"Table size is $sizeInMB MB, repartition num is set to $num.")
      num
    } catch {
      case t: Throwable =>
        logWarning("Error occurred when estimate repartition number.", t)
        0
    }
    ss.sparkContext.setJobDescription(s"Build table snapshot ${tableInfo.identity}.")
    if (repartitionNum == 0) {
      logInfo(s"Error may occurred or table size is 0, skip repartition.")
      sourceData.write.parquet(resourcePath)
    } else {
      logInfo(s"Repartition snapshot to $repartitionNum partition.")
      sourceData.repartition(repartitionNum).write.parquet(resourcePath)
    }
    (tableInfo.identity, snapshotTablePath)
  }
}
