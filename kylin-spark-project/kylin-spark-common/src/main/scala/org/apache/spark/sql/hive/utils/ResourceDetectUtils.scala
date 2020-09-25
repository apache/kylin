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
import java.util.{Map => JMap}

import org.apache.kylin.shaded.com.google.common.collect.Maps
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.hadoop.fs._
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity
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
        if (measure.expression.equalsIgnoreCase("COUNT_DISTINCT")) return true
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
      val bytes = str.getBytes(Charset.defaultCharset())
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
      json.fromJson(new String(bytes, Charset.defaultCharset()), new TypeToken[T]() {}.getType)
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