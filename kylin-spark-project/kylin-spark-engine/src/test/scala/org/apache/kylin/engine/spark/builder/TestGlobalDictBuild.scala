/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.builder

import java.io.File
import java.util.Set

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.RandomStringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.DateFormat
import org.apache.kylin.cube.{CubeInstance, CubeManager, CubeSegment}
import org.apache.kylin.engine.spark.metadata.{ColumnDesc, MetadataConverter, SegmentInfo}
import org.apache.kylin.job.engine.JobEngineConfig
import org.apache.kylin.job.impl.threadpool.DefaultScheduler
import org.apache.kylin.job.lock.MockJobLock
import org.apache.kylin.metadata.model.SegmentRange.TSRange
import org.apache.spark.TaskContext
import org.apache.spark.dict.{NGlobalDictMetaInfo, NGlobalDictionary}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.junit.Assert

import scala.collection.JavaConverters.setAsJavaSetConverter
import scala.collection.mutable

class TestGlobalDictBuild extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  private val CUBE_NAME = "ci_left_join_cube"

  def getTestConfig: KylinConfig = {
    val config = KylinConfig.getInstanceFromEnv
    config
  }

  test("global dict build and checkout bucket resize strategy") {
    init()
    FileUtils.deleteQuietly(new File("/tmp/kylin"))
    val cubeMgr: CubeManager = CubeManager.getInstance(getTestConfig)
    Assert.assertTrue(getTestConfig.getHdfsWorkingDirectory.startsWith("file:"))
    val cube: CubeInstance = cubeMgr.getCube(CUBE_NAME)
    var seg = cube.getLastSegment
    if (cube.getLastSegment == null) {
      val range = new TSRange(0L, DateFormat.stringToMillis("2015-01-01"))
      seg = cubeMgr.appendSegment(cube, range)
    }
    val segInfo = MetadataConverter.getSegmentInfo(seg.getCubeInstance, seg.getUuid, seg.getName, seg.getStorageLocationIdentifier)
    val dictColSet = setAsJavaSetConverter(segInfo.toBuildDictColumns).asJava
    seg.getConfig.setProperty("kylin.dictionary.globalV2-threshold-bucket-size", "100")

    // When to resize the dictionary, please refer to the description of DictionaryBuilderHelper.calculateBucketSize

    // First build dictionary, no dictionary file exists
    var randomDataSet = generateOriginData(1000, 21)
    val meta1 = buildDict(segInfo, seg, randomDataSet, dictColSet)
    Assert.assertEquals(20, meta1.getBucketSize)
    Assert.assertEquals(1000, meta1.getDictCount)

    // apply rule #1
    randomDataSet = generateOriginData(3000, 22)
    val meta2 = buildDict(segInfo, seg, randomDataSet, dictColSet)
    Assert.assertEquals(60, meta2.getBucketSize)
    Assert.assertEquals(4000, meta2.getDictCount)

    randomDataSet = generateOriginData(3000, 23)
    val meta3 = buildDict(segInfo, seg, randomDataSet, dictColSet)
    Assert.assertEquals(60, meta3.getBucketSize)
    Assert.assertEquals(7000, meta3.getDictCount)

    // apply rule #2
    randomDataSet = generateOriginData(200, 24)
    val meta4 = buildDict(segInfo, seg, randomDataSet, dictColSet)
    Assert.assertEquals(140, meta4.getBucketSize)
    Assert.assertEquals(7200, meta4.getDictCount)

    // apply rule #3
    randomDataSet = generateHotOriginData(200, 140)
    val meta5 = buildDict(segInfo, seg, randomDataSet, dictColSet)
    Assert.assertEquals(140, meta5.getBucketSize)
    Assert.assertEquals(7400, meta5.getDictCount)

    // apply rule #3
    randomDataSet = generateOriginData(200, 25)
    val meta6 = buildDict(segInfo, seg, randomDataSet, dictColSet)
    Assert.assertEquals(280, meta6.getBucketSize)
    Assert.assertEquals(7600, meta6.getDictCount)

    randomDataSet = generateOriginData(2000, 26)
    val meta7 = buildDict(segInfo, seg, randomDataSet, dictColSet)
    Assert.assertEquals(280, meta7.getBucketSize)
    Assert.assertEquals(9600, meta7.getDictCount)
    DefaultScheduler.destroyInstance()
  }

  def buildDict(segInfo: SegmentInfo, seg: CubeSegment, randomDataSet: Dataset[Row], dictColSet: Set[ColumnDesc]): NGlobalDictMetaInfo = {
    val dictionaryBuilder = new CubeDictionaryBuilder(randomDataSet, segInfo, randomDataSet.sparkSession, dictColSet)
    val col = dictColSet.iterator().next()
    val ds = randomDataSet.select("26").distinct()
    val bucketPartitionSize = DictionaryBuilderHelper.calculateBucketSize(segInfo, col, ds)
    dictionaryBuilder.build(col, bucketPartitionSize, ds)
    val dict = new NGlobalDictionary(seg.getProject, col.tableName, col.columnName,
      seg.getConfig.getHdfsWorkingDirectory)
    dict.getMetaInfo
  }

  def generateOriginData(count: Int, length: Int): Dataset[Row] = {
    var schema = new StructType

    schema = schema.add("26", StringType)
    var set = new mutable.LinkedHashSet[Row]
    while (set.size != count) {
      val objects = new Array[String](1)
      objects(0) = RandomStringUtils.randomAlphabetic(length)
      set.+=(Row.fromSeq(objects.toSeq))
    }

    spark.createDataFrame(spark.sparkContext.parallelize(set.toSeq), schema)
  }

  def generateHotOriginData(threshold: Int, bucketSize: Int): Dataset[Row] = {
    var schema = new StructType
    schema = schema.add("26", StringType)
    var ds = generateOriginData(threshold * bucketSize * 2, 30)
    ds = ds.repartition(bucketSize, col("26"))
      .mapPartitions {
        iter =>
          val partitionID = TaskContext.get().partitionId()
          if (partitionID != 1) {
            Iterator.empty
          } else {
            iter
          }
      }(RowEncoder.apply(ds.schema))
    ds.limit(threshold)
  }

  def init() = {
    System.setProperty("kylin.metadata.distributed-lock-impl", "org.apache.kylin.engine.spark.utils.MockedDistributedLock$MockedFactory")
    val scheduler = DefaultScheduler.getInstance
    scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv), new MockJobLock)
    if (!scheduler.hasStarted) throw new RuntimeException("scheduler has not been started")
  }
}
