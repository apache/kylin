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

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{ContentSummary, FSDataOutputStream, Path}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.engine.spark.storage.ParquetStorage
import org.apache.kylin.engine.spark.utils.{Repartitioner, StorageUtils}
import org.apache.kylin.guava30.shaded.common.collect.Lists
import org.apache.kylin.metadata.cube.model.NIndexPlanManager
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFooterReader
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.junit.Assert
import org.mockito.Mockito.{when, mock => jmock}
import org.scalamock.scalatest.MockFactory
import org.scalatest.wordspec.AnyWordSpec

class TestDFBuildJob extends AnyWordSpec with MockFactory with SharedSparkSession with LocalMetadata {
  private val path = "./test"
  private val tempPath = path + DFBuildJob.TEMP_DIR_SUFFIX
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
        val repartitioner = genMockHelper(2, Lists.newArrayList(Integer.valueOf(1), Integer.valueOf(2)),
          Lists.newArrayList(Integer.valueOf(2)))
        repartitioner.doRepartition(path, path + "_temp", repartitioner.getRepartitionNumByStorage, spark)
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
        val repartitioner = genMockHelper(3, Lists.newArrayList(Integer.valueOf(1), Integer.valueOf(2)))
        repartitioner.doRepartition(path, path + "_temp", repartitioner.getRepartitionNumByStorage, spark)
        val files = new File(path).listFiles().filter(_.getName.endsWith(".parquet"))
        assert(files.length == repartitionNum)
      }
    }

    "neither have shardByColumns or average file size is too small and goal file is exist" should {
      "do not repartition" in {
        val origin = generateOriginData()
        storage.saveTo(tempPath, origin, spark)
        val mockHelper = genMockHelper(1, Lists.newArrayList(Integer.valueOf(1), Integer.valueOf(2)))

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
        mockHelper.doRepartition(path, path + "_temp", mockHelper.getRepartitionNumByStorage, spark)
        val files = new File(path).listFiles().filter(_.getName.endsWith(".parquet"))
        assert(files.length == spark.conf.get("spark.sql.shuffle.partitions").toInt)
      }
    }

    "needResetRowGroup is true" should {
      "reset parquet.block.size, parquet.page.size.row.check.max" in {
        def generateData(ss: SparkSession): Dataset[Row] = {
          var schema = new StructType
          schema = schema.add("1", IntegerType)
          schema = schema.add("2", StringType)
          val data: Seq[Row] = Range(0, 1000).map(i => Row(i, if (i % 2 == 0) "a" else "b")).toSeq
          ss.createDataFrame(ss.sparkContext.parallelize(data), schema)
        }

        val origin = generateData(spark)
        val repartitionNum = 1
        storage.saveTo(tempPath, origin, spark)
        val tempFiles = new File(tempPath).listFiles().filter(_.getName.endsWith(".parquet")).sortBy(_.getName)
        assert(tempFiles.length == 4)
        val tempFileBlockSize = tempFiles.map(file => {
          ParquetFooterReader.readFooter(HadoopUtil.getCurrentConfiguration,
            new Path(tempFiles(0).getAbsolutePath), ParquetMetadataConverter.NO_FILTER).getBlocks.size()
        }).sum
        assert(tempFileBlockSize == 4)
        val sortCols = origin.schema.fields.map(f => new Column(f.name))
        val repartitioner = genMockHelper(repartitionNum, Lists.newArrayList(Integer.valueOf(1), Integer.valueOf(2)),
          Lists.newArrayList(Integer.valueOf(2)), needResetRowGroup = true)
        repartitioner.doRepartition(path, path + "_temp", repartitioner.getRepartitionNumByStorage, spark)
        val files = new File(path).listFiles().filter(_.getName.endsWith(".parquet")).sortBy(_.getName)
        assert(files.length == repartitionNum)

        val fileBlocks = ParquetFooterReader.readFooter(HadoopUtil.getCurrentConfiguration,
          new Path(files(0).getAbsolutePath), ParquetMetadataConverter.NO_FILTER).getBlocks
        assert(fileBlocks.size > tempFileBlockSize)
      }
    }
  }

  "layout" should {
    "has countDistinct" in {
      val manager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv, "default")
      val entity = manager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a").getIndexEntity(1000000).getLastLayout
      Assert.assertTrue(StorageUtils.findCountDistinctMeasure(entity))
    }
  }

  def genMockHelper(repartitionNum: Int, sortByColumns: util.List[Integer], isShardByColumn: util.List[Integer] = null,
                    needResetRowGroup: Boolean = false): Repartitioner = {
    val sc = jmock(classOf[ContentSummary])
    when(sc.getFileCount).thenReturn(1L)
    when(sc.getLength).thenReturn(repartitionNum * 1024 * 1024L)
    val helper = new Repartitioner(1, 1, needResetRowGroup,
      1, 1, repartitionNum * 100, 100, sc, isShardByColumn, sortByColumns, true)
    Assert.assertEquals(repartitionNum, helper.getRepartitionNumByStorage)
    helper
  }
}
