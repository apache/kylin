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

package org.apache.spark.sql.hive.utils

import java.io.IOException
import java.nio.charset.Charset
import java.util.concurrent.Executors
import java.util.{Map => JMap}

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.guava30.shaded.common.collect.Maps
import org.apache.kylin.metadata.cube.model.{DimensionRangeInfo, LayoutEntity}
import org.apache.kylin.query.util.QueryInterruptChecker
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.sources.NBaseRelation
import org.apache.spark.util.ThreadUtils

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

object ResourceDetectUtils extends Logging {
  private val json = new Gson()

  private val errorMsgLog: String = "Interrupted at the stage of get paths in ResourceDetectUtils."

  def getPaths(plan: SparkPlan, isResourceDetectJob: Boolean = false): Seq[Path] = {
    var paths = Seq.empty[Path]
    plan.foreach {
      case plan: FileSourceScanExec =>
        val info = "Current step: get Partition file status of FileSourceScanExec."
        paths ++= getFilePaths(plan.relation.location, plan.partitionFilters, plan.dataFilters, info, isResourceDetectJob)
      case plan: LayoutFileSourceScanExec =>
        val info = "Current step: get Partition file status of LayoutFileSourceScanExec."
        paths ++= getFilePaths(plan.relation.location, plan.partitionFilters, plan.dataFilters, info, isResourceDetectJob)
      case plan: InMemoryTableScanExec =>
        val _plan = plan.relation.cachedPlan
        paths ++= getPaths(_plan)
      case plan: HiveTableScanExec =>
        if (plan.relation.isPartitioned) {
          plan.rawPartitions.foreach { partition =>
            QueryInterruptChecker.checkThreadInterrupted(errorMsgLog,
              "Current step: get Partition file status of HiveTableScanExec.")
            paths ++= partition.getPath
          }
        } else {
          paths :+= new Path(plan.relation.tableMeta.location)
        }
      case plan: RowDataSourceScanExec =>
        plan.relation match {
          case relation: NBaseRelation =>
            paths :+= relation.location
          case _ =>
        }
      case _ =>
    }
    paths
  }

  def getFilePaths(fileIndex: FileIndex, partitionFilters: Seq[Expression], dataFilters: Seq[Expression]
                   , info: String, isResourceDetectJob: Boolean): Seq[Path] = {
    var paths = Seq.empty[Path]
    if (fileIndex.partitionSchema.nonEmpty) {
      var newPartitionFilters = partitionFilters
      if (isResourceDetectJob) {
        logInfo("The job is resource detect job, add filterNot of SubqueryExpression to the job.")
        newPartitionFilters = partitionFilters.filterNot(SubqueryExpression.hasSubquery)
      }
      val selectedPartitions = fileIndex.listFiles(newPartitionFilters, dataFilters)
      selectedPartitions.flatMap(partition => {
        QueryInterruptChecker.checkThreadInterrupted(errorMsgLog, info)
        partition.files
      }).foreach(file => {
        paths :+= file.getPath
      })
    } else {
      paths ++= fileIndex.rootPaths
    }
    paths
  }

  def checkPartitionFilter(plan: SparkPlan): Boolean = {
    var isIncludeFilter = false
    plan.foreach {
      case plan: FileSourceScanExec =>
        isIncludeFilter ||= plan.partitionFilters.nonEmpty
      case plan: LayoutFileSourceScanExec =>
        isIncludeFilter ||= plan.partitionFilters.nonEmpty
      case plan: HiveTableScanExec =>
        if (plan.relation.isPartitioned) {
          isIncludeFilter ||= plan.rawPartitions.nonEmpty
        }
      case _ =>
    }
    isIncludeFilter
  }

  def getPartitions(plan: SparkPlan): String = {
    val leafNodePartitionsLengthMap: mutable.Map[String, Int] = mutable.Map()
    var pNum = 0
    plan.foreach {
      case node: LeafExecNode =>
        val pn = node match {
          case ree: ReusedExchangeExec if ree.child.isInstanceOf[BroadcastExchangeExec] => 1
          case _ => leafNodePartitionsLengthMap.getOrElseUpdate(node.nodeName, node.execute().partitions.length)
        }
        pNum = pNum + pn
        logInfo(s"${node.nodeName} partition size $pn")
      case _ =>
    }
    logInfo(s"Partition num $pNum")
    pNum.toString
  }

  @throws[IOException]
  protected def listSourcePath(shareDir: Path): java.util.Map[String, java.util.Map[String, Double]] = {
    val fs = HadoopUtil.getWorkingFileSystem
    val resourcePaths = Maps.newHashMap[String, java.util.Map[String, Double]]()
    if (fs.exists(shareDir)) {
      val fileStatuses = fs.listStatus(shareDir, new PathFilter {
        override def accept(path: Path): Boolean = {
          path.toString.endsWith(ResourceDetectUtils.fileName())
        }
      })
      for (file <- fileStatuses) {
        val fileName = file.getPath.getName
        val segmentId = fileName.substring(0, fileName.indexOf(ResourceDetectUtils.fileName) - 1)
        val map = ResourceDetectUtils.readResourcePathsAs[java.util.Map[String, Double]](file.getPath)
        resourcePaths.put(segmentId, map)
      }
    }
    // return size with unit
    resourcePaths
  }

  def findCountDistinctMeasure(layouts: java.util.Collection[LayoutEntity]): Boolean = {
    for (layoutEntity <- layouts.asScala) {
      for (measure <- layoutEntity.getOrderedMeasures.values.asScala) {
        if (measure.getFunction.getExpression.equalsIgnoreCase("COUNT_DISTINCT")) return true
      }
    }
    false
  }

  def getResourceSize(kylinConfig: KylinConfig, configuration: Configuration, paths: Path*): Long = {
    val resourceSize = {
      if (kylinConfig.isConcurrencyFetchDataSourceSize) {
        getResourceSizeWithTimeoutByConcurrency(kylinConfig, Duration.Inf, configuration, paths: _*)
      } else {
        getResourceSizBySerial(kylinConfig, configuration, paths: _*)
      }
    }
    resourceSize
  }

  def getResourceSizBySerial(kylinConfig: KylinConfig, configuration: Configuration, paths: Path*): Long = {
    paths.map(path => {
      QueryInterruptChecker.checkThreadInterrupted(errorMsgLog, "Current step: get resource size.")
      val fs = path.getFileSystem(configuration)
      if (fs.exists(path)) {
        HadoopUtil.getContentSummaryFromHdfsKylinConfig(fs, path, kylinConfig).getLength
      } else {
        0L
      }
    }).sum
  }

  def getResourceSizeWithTimeoutByConcurrency(kylinConfig: KylinConfig, timeout: Duration,
                                              configuration: Configuration, paths: Path*): Long = {
    val threadNumber = kylinConfig.getConcurrencyFetchDataSourceSizeThreadNumber
    logInfo(s"Get resource size concurrency, thread number is $threadNumber")
    val executor = Executors.newFixedThreadPool(threadNumber)
    implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)
    val futures: Seq[Future[Long]] = getResourceSize(kylinConfig, configuration, executionContext, paths: _*)
    try {
      val combinedFuture = Future.sequence(futures)
      val results: Seq[Long] = ThreadUtils.awaitResult(combinedFuture, timeout)
      results.sum
    } finally {
      executor.shutdownNow()
    }
  }

  def getResourceSize(kylinConfig: KylinConfig, configuration: Configuration,
                      executionContext: ExecutionContextExecutor, paths: Path*): Seq[Future[Long]] = {
    paths.map { path =>
      Future {
        val fs = path.getFileSystem(configuration)
        if (fs.exists(path)) {
          HadoopUtil.getContentSummaryFromHdfsKylinConfig(fs, path, kylinConfig).getLength
        } else {
          0L
        }
      }(executionContext)
    }
  }

  def getResourceSize(kylinConfig: KylinConfig, paths: Path*): Long = {
    getResourceSize(kylinConfig, HadoopUtil.getCurrentConfiguration, paths: _*)
  }

  def getMaxResourceSize(shareDir: Path): Long = {
    ResourceDetectUtils.listSourcePath(shareDir)
      .values.asScala
      .flatMap(value => value.values().asScala)
      .max
      .longValue()
  }

  def getSegmentSourceSize(shareDir: Path): java.util.Map[String, Long] = {
    // For a number without fractional part, Gson would convert it as Double,need to convert Double to Long
    ResourceDetectUtils.listSourcePath(shareDir).asScala
      .filter(_._2.keySet().contains("-1"))
      .map(tp => (tp._1, tp._2.get("-1").longValue()))
      .toMap
      .asJava
  }

  def write(path: Path, item: Object): Unit = {
    val fs = HadoopUtil.getWorkingFileSystem()
    var out: FSDataOutputStream = null
    try {
      out = fs.create(path)
      val str = json.toJson(item)
      val bytes = str.getBytes(Charset.defaultCharset)
      out.writeInt(bytes.length)
      out.write(bytes)
    } finally {
      if (out != null) {
        out.close()
      }
    }
  }

  def selectMaxValueInFiles(files: Array[FileStatus]): String = {
    files.map(f => readResourcePathsAs[JMap[String, Double]](f.getPath).values().asScala.max).max.toString
  }


  def readDetectItems(path: Path): JMap[String, String] = readResourcePathsAs[JMap[String, String]](path)

  def readSegDimRangeInfo(path: Path): java.util.Map[String, DimensionRangeInfo] = {
    if (HadoopUtil.getWorkingFileSystem().exists(path)) {
      readResourcePathsAs[java.util.Map[String, DimensionRangeInfo]](path)
    } else {
      null
    }
  }

  def readResourcePathsAs[T](path: Path): T = {
    log.info(s"Read resource paths form $path")
    val fs = HadoopUtil.getWorkingFileSystem
    var in: FSDataInputStream = null
    try {
      in = fs.open(path)
      val i = in.readInt()
      val bytes = new Array[Byte](i)
      in.readFully(bytes)
      json.fromJson(new String(bytes, Charset.defaultCharset), new TypeToken[T]() {}.getType)
    } finally {
      if (in != null) {
        in.close()
      }
    }
  }


  def fileName(): String = {
    "resource_paths.json"
  }

  val cubingDetectItemFileSuffix: String = "cubing_detect_items.json"

  val samplingDetectItemFileSuffix: String = "sampling_detect_items.json"

  val countDistinctSuffix: String = "count_distinct.json"
}
