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

package io.kyligence.kap.engine.spark.job

import java.io.File
import java.util

import com.google.common.collect.Lists
import io.kyligence.kap.engine.spark.storage.ParquetStorage
import io.kyligence.kap.engine.spark.utils.{BuildUtils, Repartitioner}
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{ContentSummary, FSDataOutputStream, Path}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Column, Dataset, Row}
import org.junit.Assert
import org.mockito.Mockito.{when, mock => jmock}
import org.scalamock.scalatest.MockFactory
import org.scalatest.WordSpec



class TestDFBuildJob extends WordSpec with MockFactory with SharedSparkSession with LocalMetadata {
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
         val manager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv, "default")
      val entity = manager.getIndexPlan("741ca86a-1f13-46da-a59f-95fb68615e3a").getIndexEntity(1000000).getLastLayout
      Assert.assertTrue(BuildUtils.findCountDistinctMeasure(entity))
    }
  }

  def genMockHelper(repartitionNum: Int, isShardByColumn: util.List[Integer] = null): Repartitioner = {
    val sc = jmock(classOf[ContentSummary])
    when(sc.getFileCount).thenReturn(1L)
    when(sc.getLength).thenReturn(repartitionNum * 1024 * 1024L)
    val helper = new Repartitioner(1, 1, repartitionNum * 100, 100, sc, isShardByColumn)
    Assert.assertEquals(repartitionNum, helper.getRepartitionNumByStorage)
    helper
  }
}
