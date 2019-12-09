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

package io.kyligence.kap.engine.spark.builder

import java.io.IOException
import java.util.UUID
import java.util.concurrent.Executors

import com.google.common.collect.Maps
import io.kyligence.kap.engine.spark.NSparkCubingEngine
import io.kyligence.kap.engine.spark.job.KylinBuildEnv
import io.kyligence.kap.engine.spark.utils.FileNames
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.engine.spark.metadata.cube.model.{Cube, DataModel, DataSegment, TableDesc}
import org.apache.kylin.engine.spark.metadata.cube.source.SourceFactory
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.utils.ResourceDetectUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.utils.ProxyThreadUtils

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.Breaks._
import scala.util.{Failure, Success, Try}

class DFSnapshotBuilder extends Logging {

  var ss: SparkSession = _
  var seg: DataSegment = _
  var cube: Cube = _

  private val MD5_SUFFIX = ".md5"
  private val PARQUET_SUFFIX = ".parquet"
  private val MB = 1024 * 1024

  def this(seg: DataSegment, ss: SparkSession) {
    this()
    this.seg = seg
    this.ss = ss
  }

  def this(cube: Cube, ss: SparkSession) {
    this()
    this.cube = cube
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
  def buildSnapshot: DataSegment = {
    logInfo(s"Building snapshots for: $seg")
    val model = seg.getCube.getModel
    val newSnapMap = Maps.newHashMap[String, String]
    val fs = HadoopUtil.getWorkingFileSystem
    val kylinConf = seg.getCube.getConfig
    val baseDir = kylinConf.getReadHdfsWorkingDirectory
    val toBuildTableDesc = distinctTableDesc(model)
    if (kylinConf.isSnapshotParallelBuildEnabled) {
      val service = Executors.newCachedThreadPool()
      implicit val executorContext = ExecutionContext.fromExecutorService(service)
      val futures = toBuildTableDesc
        .map {
          tableDesc =>
            Future[(String, String)] {
              if (kylinConf.isUTEnv) {
                Thread.sleep(1000L)
              }
              try {
                KylinConfig.setAndUnsetThreadLocalConfig(kylinConf)
                buildSnapshotWithoutMd5(tableDesc, baseDir)
              } catch {
                case exception: Exception =>
                  logError(s"Error for build snapshot table with $tableDesc", exception)
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
          newSnapMap.putAll(result.toMap.asJava)
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
    val cube = seg.getCube
    // make a copy of the changing segment, avoid changing the cached object
    // To do: add snapshots to segment with copy
    seg
  }

  def distinctTableDesc(model: DataModel): Set[TableDesc] = {
    model.getJoinTables.asScala
      .filter(lookupDesc => {
        val tableDesc = lookupDesc.getTableRef.getTableDesc
        val isLookupTable = model.isLookupTable(lookupDesc.getTableRef)
        isLookupTable && seg.getSnapshots.get(tableDesc.getIdentity) == null
      })
      .map(_.getTableRef.getTableDesc)
      .toSet
  }

  def getSourceData(tableDesc: TableDesc): Dataset[Row] = {
    SourceFactory
      .createEngineAdapter(tableDesc, classOf[NSparkCubingEngine.NSparkCubingSource])
      .getSourceData(tableDesc, ss, Maps.newHashMap[String, String])
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

  def buildSingleSnapshot(tableDesc: TableDesc, baseDir: String, fs: FileSystem): (String, String) = {
    val sourceData = getSourceData(tableDesc)
    val tablePath = FileNames.snapshotFile(tableDesc)
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

    (tableDesc.getIdentity, snapshotTablePath)
  }

  def buildSnapshotWithoutMd5(tableDesc: TableDesc, baseDir: String): (String, String) = {
    val sourceData = getSourceData(tableDesc)
    val tablePath = FileNames.snapshotFile(tableDesc)
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
    ss.sparkContext.setJobDescription(s"Build table snapshot ${tableDesc.getIdentity}.")
    if (repartitionNum == 0) {
      logInfo(s"Error may occurred or table size is 0, skip repartition.")
      sourceData.write.parquet(resourcePath)
    } else {
      logInfo(s"Repartition snapshot to $repartitionNum partition.")
      sourceData.repartition(repartitionNum).write.parquet(resourcePath)
    }
    (tableDesc.getIdentity, snapshotTablePath)
  }
}
