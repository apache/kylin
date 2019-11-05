/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.builder

import java.io.IOException
import java.util.UUID
import java.util.concurrent.Executors

import com.google.common.collect.Maps
import io.kyligence.kap.engine.spark.NSparkCubingEngine
import io.kyligence.kap.engine.spark.job.KylinBuildEnv
import io.kyligence.kap.engine.spark.utils.FileNames
import io.kyligence.kap.metadata.cube.model.{NDataflowManager, NDataflowUpdate, NDataSegment}
import io.kyligence.kap.metadata.model.NDataModel
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.metadata.model.TableDesc
import org.apache.kylin.source.SourceFactory
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
  var seg: NDataSegment = _

  private val MD5_SUFFIX = ".md5"
  private val PARQUET_SUFFIX = ".parquet"
  private val MB = 1024 * 1024

  def this(seg: NDataSegment, ss: SparkSession) {
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
  def buildSnapshot: NDataSegment = {
    logInfo(s"Building snapshots for: $seg")
    val model = seg.getDataflow.getModel
    val newSnapMap = Maps.newHashMap[String, String]
    val fs = HadoopUtil.getWorkingFileSystem
    val baseDir = KapConfig.wrap(seg.getConfig).getReadHdfsWorkingDirectory
    val toBuildTableDesc = distinctTableDesc(model)
    val kylinConf = KylinConfig.getInstanceFromEnv
    if (seg.getConfig.isSnapshotParallelBuildEnabled) {
      val service = Executors.newCachedThreadPool()
      implicit val executorContext = ExecutionContext.fromExecutorService(service)
      val futures = toBuildTableDesc
        .map {
          tableDesc =>
            Future[(String, String)] {
              if (seg.getConfig.isUTEnv) {
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
        val result = ProxyThreadUtils.awaitResult(eventualTuples, seg.getConfig.snapshotParallelBuildTimeoutSeconds seconds)
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
    val dataflow = seg.getDataflow
    // make a copy of the changing segment, avoid changing the cached object
    val dfCopy = dataflow.copy
    val segCopy = dfCopy.getSegment(seg.getId)
    val update = new NDataflowUpdate(dataflow.getUuid)
    segCopy.getSnapshots.putAll(newSnapMap)
    update.setToUpdateSegs(segCopy)
    val updatedDataflow = NDataflowManager.getInstance(seg.getConfig, seg.getProject).updateDataflow(update)
    updatedDataflow.getSegment(seg.getId)
  }

  def distinctTableDesc(model: NDataModel): Set[TableDesc] = {
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
