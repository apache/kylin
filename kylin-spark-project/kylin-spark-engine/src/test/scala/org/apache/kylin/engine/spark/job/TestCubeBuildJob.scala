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

package org.apache.kylin.engine.spark.job

import java.io.File
import java.util

import org.apache.kylin.shaded.com.google.common.collect.Lists
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{ContentSummary, FSDataOutputStream, Path}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.{DateFormat, HadoopUtil}
import org.apache.kylin.cube.CubeManager
import org.apache.kylin.engine.spark.metadata.MetadataConverter
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity
import org.apache.kylin.engine.spark.storage.ParquetStorage
import org.apache.kylin.engine.spark.utils.{BuildUtils, Repartitioner}
import org.apache.kylin.metadata.model.SegmentRange.TSRange
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Column, Dataset, Row}
import org.junit.Assert
import org.mockito.Mockito.{when, mock => jmock}
import org.scalamock.scalatest.MockFactory
import org.scalatest.WordSpec



class TestCubeBuildJob extends WordSpec with MockFactory with SharedSparkSession with LocalMetadata {
  private val path = "./test"
  private val tempPath = path + CubeBuildJob.TEMP_DIR_SUFFIX
  private val storage = new ParquetStorage()

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    FileUtils.deleteQuietly(new File(path))
    FileUtils.deleteQuietly(new File(tempPath))
  }

  def generateOriginData(): Dataset[Row] = {
    var schema = new StructType
    schema = schema.add("1", StringType)
    schema = schema.add("2", StringType)
    val data = Seq(
      Row("0", "a"),
      Row("1", "b"),
      Row("2", "a"),
      Row("3", "b"),
      Row("4", "a"),
      Row("5", "b"),
      Row("6", "a"),
      Row("7", "b"),
      Row("8", "a"),
      Row("9", "b"))

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  "repartition" when {
    "cuboid have shardByColumns" should {
      "repartition for shardByColumns" in {
        val origin = generateOriginData()
        val repartitionNum = 2
        storage.saveTo(tempPath, origin, spark)
        val sortCols = origin.schema.fields.map(f => new Column(f.name))
        val repartitioner = genMockHelper(2, Lists.newArrayList(Integer.valueOf(2)))
        repartitioner.doRepartition(storage, path, repartitioner.getRepartitionNumByStorage, sortCols, spark)
        val files = new File(path).listFiles().filter(_.getName.endsWith(".parquet")).sortBy(_.getName)
        assert(files.length == repartitionNum)
        storage.getFrom(files.apply(0).getPath, spark).collect().map(_.getString(1)).foreach {
          value =>
            assert(value == "a")
        }

        storage.getFrom(files.apply(1).getPath, spark).collect().map(_.getString(1)).foreach {
          value =>
            assert(value == "b")
        }
      }
    }

    "average file size is too small" should {
      "repartition for file size" in {
        val origin = generateOriginData()
        storage.saveTo(tempPath, origin, spark)
        val repartitionNum = 3
        val sortCols = origin.schema.fields.map(f => new Column(f.name))
        val repartitioner = genMockHelper(3)
        repartitioner.doRepartition(storage, path, repartitioner.getRepartitionNumByStorage, sortCols, spark)
        val files = new File(path).listFiles().filter(_.getName.endsWith(".parquet"))
        assert(files.length == repartitionNum)
      }
    }

    "neither have shardByColumns or average file size is too small and goal file is exist" should {
      "do not repartition" in {
        val origin = generateOriginData()
        storage.saveTo(tempPath, origin, spark)
        val mockHelper = genMockHelper(1)

        var stream: FSDataOutputStream = null
        try {
          stream = HadoopUtil.getWorkingFileSystem().create(new Path(path))
          stream.writeUTF("test")
        } finally {
          if (stream != null) {
            stream.close()
          }
        }
        val sortCols = origin.schema.fields.map(f => new Column(f.name))
        mockHelper.doRepartition(storage, path, mockHelper.getRepartitionNumByStorage, sortCols, spark)
        val files = new File(path).listFiles().filter(_.getName.endsWith(".parquet"))
        assert(files.length == spark.conf.get("spark.sql.shuffle.partitions").toInt)
      }
    }

  }

  "layout" should{
    "has countDistinct" in {
      val cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv)
      val cube = cubeManager.getCube("ci_left_join_cube")
      var seg = cube.getLastSegment
      if (cube.getLastSegment == null) {
        val range = new TSRange(0L, DateFormat.stringToMillis("2015-01-01"))
        seg = cubeManager.appendSegment(cube, range)
      }
      val layoutEntities = MetadataConverter.extractEntityList2JavaList(seg.getCubeInstance)
      val iter = layoutEntities.iterator
      while (iter.hasNext) {
        val entity = iter.next
        Assert.assertTrue(BuildUtils.findCountDistinctMeasure(entity))
      }
    }
  }

  def genMockHelper(repartitionNum: Int, isShardByColumn: util.List[Integer] = null): Repartitioner = {
    val sc = jmock(classOf[ContentSummary])
    when(sc.getFileCount).thenReturn(1L)
    when(sc.getLength).thenReturn(repartitionNum * 1024 * 1024L)
    val layout = new LayoutEntity
    layout.setRows(repartitionNum * 100)
    layout.setId(100l)
    val helper = new Repartitioner(1, 1, layout, 100, sc, isShardByColumn)
    Assert.assertEquals(repartitionNum, helper.getRepartitionNumByStorage)
    helper
  }
}
