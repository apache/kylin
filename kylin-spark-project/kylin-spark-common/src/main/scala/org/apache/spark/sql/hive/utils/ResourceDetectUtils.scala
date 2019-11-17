/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
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
 *
 */

package org.apache.spark.sql.hive.utils

import java.io.IOException
import java.util.{Map => JMap}

import com.google.common.collect.Maps
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import io.kyligence.kap.metadata.cube.model.LayoutEntity
import org.apache.hadoop.fs._
import org.apache.kylin.common.util.HadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.hive.execution.HiveTableScanExec

import scala.collection.JavaConverters._

object ResourceDetectUtils extends Logging {
  private val json = new Gson()

  def getPaths(plan: SparkPlan): Seq[Path] = {
    var paths = Seq.empty[Path]
    plan.foreach {
      case plan: FileSourceScanExec =>
        paths ++= plan.relation.location.rootPaths
      case plan: InMemoryTableScanExec =>
        val _plan = plan.relation.cachedPlan
        paths ++= getPaths(_plan)
      case plan: HiveTableScanExec =>
        if (plan.relation.isPartitioned) {
          plan.rawPartitions.foreach { partition =>
            paths ++= partition.getPath
          }
        } else {
          paths :+= new Path(plan.relation.tableMeta.location)
        }
      case _ =>
    }
    paths
  }

  @throws[IOException]
  protected def listSourcePath(shareDir: Path): java.util.Map[String, java.util.Map[String, java.util.List[String]]] = {
    val fs = HadoopUtil.getWorkingFileSystem
    val fileStatuses = fs.listStatus(shareDir, new PathFilter{
      override def accept(path: Path): Boolean = {
        path.toString.endsWith(ResourceDetectUtils.fileName())
      }
    })
    // segmnet -> (layout_ID, path)
    val resourcePaths = Maps.newHashMap[String, java.util.Map[String, java.util.List[String]]]()
    for (file <- fileStatuses) {
      val fileName = file.getPath.getName
      val segmentId = fileName.substring(0, fileName.indexOf(ResourceDetectUtils.fileName) - 1)
      val map = ResourceDetectUtils.readResourcePathsAs[java.util.Map[String, java.util.List[String]]](file.getPath)
        resourcePaths.put(segmentId, map)
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

  def getResourceSize(paths: Path*): Long = {
    paths.map(path => {
      val fs = path.getFileSystem(HadoopUtil.getCurrentConfiguration)
      if (fs.exists(path)) {
        HadoopUtil.getContentSummary(fs, path).getLength
      } else {
        0L
      }
    }).sum
  }

  def getMaxResourceSize(shareDir: Path): Long = {
    ResourceDetectUtils.listSourcePath(shareDir)
      .values.asScala
      .flatMap(value => value.values().asScala.map(v => getResourceSize(v.asScala.map(path => new Path(path)): _*)))
      .max
  }

  def getSegmentSourceSize(shareDir: Path): java.util.Map[String, Long] = {
    ResourceDetectUtils.listSourcePath(shareDir).asScala
      .filter(_._2.keySet().contains("-1"))
      .map(tp => (tp._1, getResourceSize(tp._2.get("-1").asScala.map(path => new Path(path)): _*)))
      .toMap
      .asJava
  }

  def write(path: Path, item: Object): Unit = {
    val fs = HadoopUtil.getWorkingFileSystem()
    var out: FSDataOutputStream = null
    try {
      out = fs.create(path)
      val str = json.toJson(item)
      val bytes = str.getBytes
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

  def readResourcePathsAs[T](path: Path): T = {
    log.info(s"Read resource paths form $path")
    val fs = HadoopUtil.getWorkingFileSystem
    var in: FSDataInputStream = null
    try {
      in = fs.open(path)
      val i = in.readInt()
      val bytes = new Array[Byte](i)
      in.readFully(bytes)
      json.fromJson(new String(bytes), new TypeToken[T]() {}.getType)
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