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

import java.text.SimpleDateFormat
import java.util.{Locale, TimeZone, UUID}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.cube.{CubeInstance, CubeManager, CubeSegment}
import org.apache.kylin.engine.spark.cross.CrossDateTimeUtils
import org.apache.kylin.engine.spark.job.KylinBuildEnv
import org.apache.kylin.engine.spark.metadata.MetadataConverter
import org.apache.kylin.engine.spark.metadata.cube.model.ForestSpanningTree
import org.apache.kylin.job.engine.JobEngineConfig
import org.apache.kylin.job.impl.threadpool.DefaultScheduler
import org.apache.kylin.job.lock.MockJobLock
import org.apache.kylin.metadata.model.SegmentRange
import org.apache.spark.{InfoHelper, SPARK_VERSION}
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.{Dataset, Row}
import org.junit.Assert

import scala.collection.JavaConversions

// scalastyle:off
class TestCreateFlatTable extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  private val PROJECT = "default"
  private val CUBE_NAME1 = "ci_left_join_cube"
  private val CUBE_NAME2 = "ci_inner_join_cube"

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT)
  dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"))

  def getTestConfig: KylinConfig = {
    val config = KylinConfig.getInstanceFromEnv
    config
  }

  test("Check the flattable filter and encode") {
    init()
    val cubeMgr: CubeManager = CubeManager.getInstance(getTestConfig)
    val cube: CubeInstance = cubeMgr.getCube(CUBE_NAME1)
    // cleanup all segments first
    cubeMgr.updateCubeDropSegments(cube, cube.getSegments())

    // resource detect mode
    val seg1 = cubeMgr.appendSegment(cube, new SegmentRange.TSRange(0L, 1356019200000L))
    val afterJoin1 = generateFlatTable(seg1, cube, false)
    checkFilterCondition(afterJoin1, seg1)
    checkEncodeCols(afterJoin1, seg1, false)

    val seg2 = cubeMgr.appendSegment(cube, new SegmentRange.TSRange(1356019200000L, 1376019200000L))
    val afterJoin2 = generateFlatTable(seg2, cube, false)
    checkFilterCondition(afterJoin2, seg2)
    checkEncodeCols(afterJoin2, seg2, false)

    // cubing mode
    val seg3 = cubeMgr.appendSegment(cube, new SegmentRange.TSRange(1376019200000L, 1396019200000L))
    val afterJoin3 = generateFlatTable(seg3, cube, true)
    checkEncodeCols(afterJoin3, seg3, true)

    val seg4 = cubeMgr.appendSegment(cube, new SegmentRange.TSRange(1396019200000L, 1416019200000L))
    val afterJoin4 = generateFlatTable(seg4, cube, true)
    checkEncodeCols(afterJoin4, seg4, true)
    //DefaultScheduler.destroyInstance()
  }

  test("Check the flattable spark jobs num correctness") {
    init()
    val helper: InfoHelper = new InfoHelper(spark)

    val cubeMgr: CubeManager = CubeManager.getInstance(getTestConfig)
    val cube: CubeInstance = cubeMgr.getCube(CUBE_NAME2)
    // cleanup all segments first
    cubeMgr.updateCubeDropSegments(cube, cube.getSegments)

    val groupId = UUID.randomUUID().toString
    spark.sparkContext.setJobGroup(groupId, "test", false)
    val seg1 = cubeMgr.appendSegment(cube, new SegmentRange.TSRange(0L, 1356019200000L))
    val afterJoin1 = generateFlatTable(seg1, cube, true)
    afterJoin1.collect()


    if (SPARK_VERSION.startsWith("2.4")) {
      val jobs = helper.getJobsByGroupId(groupId)
      if (seg1.getConfig.detectDataSkewInDictEncodingEnabled()) {
        Assert.assertEquals(jobs.length, 18)
      } else {
        Assert.assertEquals(jobs.length, 15)
      }  
    } else if (SPARK_VERSION.startsWith("3.1")) {
      // in Spark 3.x, BroadcastExchangeExec overwrites job group ID
      val jobs = helper.getJobsByGroupId(null)
      Assert.assertEquals(6, jobs.count(_.jobGroup.exists(_.endsWith(groupId))))
      Assert.assertEquals(9, jobs.count(_.description.exists(_.contains("broadcast exchange"))))
    }
    DefaultScheduler.destroyInstance()
  }

  private def checkFilterCondition(ds: Dataset[Row], seg: CubeSegment) = {
    val queryExecution = ds.queryExecution.simpleString
    var startTime = dateFormat.format(seg.getTSRange.start.v)
    var endTime = dateFormat.format(seg.getTSRange.end.v)

    //Test Filter Condition

    // dates will not be converted to string by default since spark 3.0.0.
    // see https://issues.apache.org/jira/browse/SPARK-27638 for details.
    if (SPARK_VERSION.startsWith("3.") && conf.get("spark.sql.legacy.typeCoercion.datetimeToString.enabled", "false") == "false") {
      startTime = CrossDateTimeUtils.stringToDate(startTime).get.toString
      endTime = CrossDateTimeUtils.stringToDate(endTime).get.toString
    }

    Assert.assertTrue(queryExecution.contains(startTime))
    Assert.assertTrue(queryExecution.contains(endTime))
  }

  private def checkEncodeCols(ds: Dataset[Row], segment: CubeSegment, needEncode: Boolean) = {
    val seg = MetadataConverter.getSegmentInfo(segment.getCubeInstance, segment.getUuid, segment.getName, segment.getStorageLocationIdentifier)
    val globalDictSet = seg.toBuildDictColumns
    val actualEncodeDictSize = ds.schema.count(_.name.endsWith(CubeBuilderHelper.ENCODE_SUFFIX))
    if (needEncode) {
      Assert.assertEquals(globalDictSet.size, actualEncodeDictSize)
    } else {
      Assert.assertEquals(0, actualEncodeDictSize)
    }
  }

  private def generateFlatTable(segment: CubeSegment, cube: CubeInstance, needEncode: Boolean): Dataset[Row] = {
    val seg = MetadataConverter.getSegmentInfo(segment.getCubeInstance, segment.getUuid, segment.getName, segment.getStorageLocationIdentifier)
    val spanningTree = new ForestSpanningTree(JavaConversions.asJavaCollection(seg.toBuildLayouts))
    //for test case there is no build job id
    val flatTable = new CreateFlatTable(seg, spanningTree, spark, null, spark.sparkContext.applicationId)
    val afterJoin = flatTable.generateDataset(needEncode)
    afterJoin
  }

  def init() = {
    KylinBuildEnv.getOrCreate(getTestConfig)
    System.setProperty("kylin.metadata.distributed-lock-impl", "org.apache.kylin.engine.spark.utils.MockedDistributedLock$MockedFactory")
    val scheduler = DefaultScheduler.getInstance
    scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv), new MockJobLock)
    if (!scheduler.hasStarted) throw new RuntimeException("scheduler has not been started")
  }
}
