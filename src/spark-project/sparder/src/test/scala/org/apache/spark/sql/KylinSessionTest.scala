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
package org.apache.spark.sql

import org.apache.kylin.common.KapConfig
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.SchedulingMode.{FAIR, FIFO, SchedulingMode}
import org.apache.spark.scheduler._
import org.apache.spark.sql.common.{LocalMetadata, SparderBaseFunSuite}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
import java.util.Properties

class KylinSessionTest extends SparderBaseFunSuite with LocalMetadata {

  ignore("AL-91: should reuse sharedState and SessionState if sparkContext stop") {
    overwriteSystemProp("kylin.storage.columnar.spark-conf.javax.jdo.option.ConnectionURL", "jdbc:derby:memory:db;create=true")
    val spark = SparderEnv.getSparkSession.asInstanceOf[KylinSession]

    // DB
    assertResult(1)(spark.sql("show databases").count())
    spark.sql("create database KylinSessionTest")
    assertResult(2)(spark.sql("show databases").count())

    // temp view
    import spark.implicits._
    val s = Seq(1, 2, 3).toDF("num")
    s.createOrReplaceTempView("nums")
    assert(spark.sessionState.catalog.getTempView("nums").isDefined)
    assert(SparkSession.getDefaultSession.isDefined)

    // UDF
    spark.sql("select ceil_datetime(date'2012-02-29', 'year')").collect()
      .map(row => row.toString()).mkString.equals("[2013-01-01 00:00:00.0]")
    spark.sparkContext.stop()

    // active
    assert(SparkSession.getActiveSession.isDefined)
    assert(SparkSession.getActiveSession.get eq spark)

    // default
    assert(SparkSession.getDefaultSession.isEmpty)

    val spark2 = SparderEnv.getSparkSession.asInstanceOf[KylinSession]
    assert(SparkSession.getActiveSession.isDefined)
    assert(SparkSession.getActiveSession.get eq spark2)
    assert(SparkSession.getDefaultSession.isDefined)
    assert(SparkSession.getDefaultSession.get eq spark2)

    // external catalog's reference should same
    assert(spark.sharedState.externalCatalog eq spark2.sharedState.externalCatalog)
    // DB
    assertResult(2)(spark2.sql("show databases").count())

    // temp view
    assert(spark2.sessionState.catalog.getTempView("nums").isDefined)

    // UDF
    spark2.sql("select ceil_datetime(date'2012-02-29', 'year')").collect()
      .map(row => row.toString()).mkString.equals("[2013-01-01 00:00:00.0]")
  }

  val DEFAULT_POOL_NAME = "default"
  val VIP_POOL_NAME = "vip_tasks"
  val LIGHTWEIGHT_POOL_NAME = "lightweight_tasks"
  val HEAVY_POOL_NAME = "heavy_tasks"
  val EXTREME_HEAVY_POOL_NAME = "extreme_heavy_tasks"
  val QUERY_PUSHDOWN_POOL_NAME = "query_pushdown"
  val ASYNC_POOL_NAME = "async_query_tasks"

  test("KE-36663 Test query limit switch") {
    val sparkConf = new SparkConf()
    val result1 = SparderEnv.isSparkExecutorResourceLimited(sparkConf)
    assert(result1)
    sparkConf.set("spark.dynamicAllocation.enabled", "true")
    val result2 = SparderEnv.isSparkExecutorResourceLimited(sparkConf)
    assert(!result2)
    sparkConf.set("spark.dynamicAllocation.maxExecutors", "1")
    val result3 = SparderEnv.isSparkExecutorResourceLimited(sparkConf)
    assert(result3)
  }

  test("KE-36663 Config fair scheduler file") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.executor.instances", "1")
    sparkConf.set("spark.executor.cores", "1")

    val kapConfig = KapConfig.getInstanceFromEnv
    val confDirPath = "./src/test/resources/"

    // normal fair scheduler
    KylinSession.applyFairSchedulerConfig(kapConfig, confDirPath, sparkConf)
    val path1 = confDirPath + KylinSession.NORMAL_FAIR_SCHEDULER_FILE_NAME
    sparkConf.set("spark.scheduler.allocation.file", path1)
    val sc1 = new SparkContext("local", "KylinSessionTest", sparkConf)
    val rootPool1 = new Pool("", SchedulingMode.FAIR, 0, 0)
    val scheduleBuilder1 = new FairSchedulableBuilder(rootPool1, sc1)
    scheduleBuilder1.buildPools()
    verifyPoolCreate(rootPool1, DEFAULT_POOL_NAME, 0, 1, FIFO)
    verifyPoolCreate(rootPool1, VIP_POOL_NAME, 1, 15, FAIR)
    verifyPoolCreate(rootPool1, LIGHTWEIGHT_POOL_NAME, 1, 10, FAIR)
    verifyPoolCreate(rootPool1, HEAVY_POOL_NAME, 1, 5, FAIR)
    verifyPoolCreate(rootPool1, EXTREME_HEAVY_POOL_NAME, 1, 3, FAIR)
    verifyPoolCreate(rootPool1, QUERY_PUSHDOWN_POOL_NAME, 1, 1, FAIR)
    sc1.stop()

    // query limit fair scheduler
    kapConfig.getKylinConfig.setProperty("kylin.query.query-limit-enabled", "true")
    KylinSession.applyFairSchedulerConfig(kapConfig, confDirPath, sparkConf)
    val path2 = confDirPath + KylinSession.QUERY_LIMIT_FAIR_SCHEDULER_FILE_NAME
    val queryLimitConfigFile = new File(path2)
    assert(queryLimitConfigFile.exists())
    sparkConf.set("spark.scheduler.allocation.file", path2)
    val sc2 = new SparkContext("local", "KylinSessionTest", sparkConf)
    val rootPool2 = new Pool("", SchedulingMode.FAIR, 0, 0)
    val scheduleBuilder2 = new FairSchedulableBuilder(rootPool2, sc2)
    scheduleBuilder2.buildPools()
    verifyPoolCreate(rootPool2, DEFAULT_POOL_NAME, 0, 1, FIFO)
    verifyPoolCreate(rootPool2, VIP_POOL_NAME, 2, 3, FAIR)
    verifyPoolCreate(rootPool2, LIGHTWEIGHT_POOL_NAME, 1, 2, FAIR)
    verifyPoolCreate(rootPool2, HEAVY_POOL_NAME, 0, 1, FAIR)
    verifyPoolCreate(rootPool2, EXTREME_HEAVY_POOL_NAME, 0, 1, FAIR)
    verifyPoolCreate(rootPool2, QUERY_PUSHDOWN_POOL_NAME, 0, 1, FAIR)
    verifyPoolCreate(rootPool2, ASYNC_POOL_NAME, 0, 1, FAIR)
    queryLimitConfigFile.delete()
    sc2.stop()

    // query limit when dynamicAllocation enabled
    kapConfig.getKylinConfig.setProperty("spark.dynamicAllocation.enabled", "true")
    kapConfig.getKylinConfig.setProperty("spark.dynamicAllocation.maxExecutors", "1")
    KylinSession.applyFairSchedulerConfig(kapConfig, confDirPath, sparkConf)
    val path3 = confDirPath + KylinSession.QUERY_LIMIT_FAIR_SCHEDULER_FILE_NAME
    val queryLimitConfigFile3 = new File(path3)
    assert(queryLimitConfigFile3.exists())
    sparkConf.set("spark.scheduler.allocation.file", path2)
    val sc3 = new SparkContext("local", "KylinSessionTest", sparkConf)
    val rootPool3 = new Pool("", SchedulingMode.FAIR, 0, 0)
    val scheduleBuilder3 = new FairSchedulableBuilder(rootPool3, sc3)
    scheduleBuilder3.buildPools()
    verifyPoolCreate(rootPool2, DEFAULT_POOL_NAME, 0, 1, FIFO)
    verifyPoolCreate(rootPool2, VIP_POOL_NAME, 2, 3, FAIR)
    verifyPoolCreate(rootPool2, LIGHTWEIGHT_POOL_NAME, 1, 2, FAIR)
    verifyPoolCreate(rootPool2, HEAVY_POOL_NAME, 0, 1, FAIR)
    verifyPoolCreate(rootPool2, EXTREME_HEAVY_POOL_NAME, 0, 1, FAIR)
    verifyPoolCreate(rootPool2, QUERY_PUSHDOWN_POOL_NAME, 0, 1, FAIR)
    verifyPoolCreate(rootPool2, ASYNC_POOL_NAME, 0, 1, FAIR)
    queryLimitConfigFile3.delete()
    sc3.stop()

    kapConfig.getKylinConfig.setProperty("kylin.query.query-limit-enabled", "false")
    kapConfig.getKylinConfig.setProperty("spark.dynamicAllocation.enabled", "false")
  }

  test("KE-36663 Fair scheduler creates query limit pools") {
    val path = "./src/test/resources/test-query-limit-fair-scheduler.xml"
    val conf = new SparkConf().set("spark.scheduler.allocation.file", path)
    val sc = new SparkContext("local", "KylinSessionTest", conf)

    val taskScheduler = new TaskSchedulerImpl(sc)

    val rootPool = new Pool("", SchedulingMode.FAIR, 0, 0)
    val scheduleBuilder = new FairSchedulableBuilder(rootPool, sc)
    scheduleBuilder.buildPools()

    verifyPoolCreate(rootPool, DEFAULT_POOL_NAME, 0, 1, FIFO)
    verifyPoolCreate(rootPool, VIP_POOL_NAME, 2, 3, FAIR)
    verifyPoolCreate(rootPool, LIGHTWEIGHT_POOL_NAME, 1, 2, FAIR)
    verifyPoolCreate(rootPool, HEAVY_POOL_NAME, 0, 1, FAIR)
    verifyPoolCreate(rootPool, EXTREME_HEAVY_POOL_NAME, 0, 1, FAIR)
    verifyPoolCreate(rootPool, QUERY_PUSHDOWN_POOL_NAME, 0, 1, FAIR)
    verifyPoolCreate(rootPool, ASYNC_POOL_NAME, 0, 1, FAIR)

    // each pool allocates 2 stages, stageId for pool:
    // extreme_heavy_tasks(0, 1), heavy_tasks(2, 3), lightweight_tasks(4, 5), vip_tasks(6, 7)
    val poolArray = Array(EXTREME_HEAVY_POOL_NAME, HEAVY_POOL_NAME, LIGHTWEIGHT_POOL_NAME, VIP_POOL_NAME)
    var properties: Properties = null
    var taskSetManager: TaskSetManager = null
    var stageIdInc = -1
    poolArray.foreach(p => {
      properties = new Properties()
      properties.setProperty("spark.scheduler.pool", p)
      for(addTask <- 1 to 2) {
        stageIdInc += 1
        taskSetManager = createTaskSetManager(stageId = stageIdInc, numTasks = 1, taskScheduler)
        scheduleBuilder.addTaskSetManager(taskSetManager, properties)
      }
    })

    // lightweight_tasks stage 4 gets scheduled
    scheduleTaskAndVerifyId(0, rootPool, 4)
    // then vip tasks
    scheduleTaskAndVerifyId(1, rootPool, 6)
    scheduleTaskAndVerifyId(2, rootPool, 7)
    // extreme heavy task and heavy task first, because lightweight task min share is meet
    scheduleTaskAndVerifyId(3, rootPool, 0)
    scheduleTaskAndVerifyId(4, rootPool, 2)
    // then judge the priority through weight, lightweight task is scheduled
    scheduleTaskAndVerifyId(5, rootPool, 5)
    // in this case, extreme_heavy_tasks and heavy_tasks have same weight, so stage 1 is scheduled first
    scheduleTaskAndVerifyId(6, rootPool, 1)
    scheduleTaskAndVerifyId(7, rootPool, 3)

    sc.stop()
  }

  private def createTaskSetManager(stageId: Int, numTasks: Int, taskScheduler: TaskSchedulerImpl): TaskSetManager = {
    val tasks = Array.tabulate[Task[_]](numTasks) { i =>
      new FakeTask(stageId, i, Nil)
    }
    new TaskSetManager(taskScheduler, new TaskSet(tasks, stageId, 0, 0, null,
      ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID), 0)
  }

  def verifyPoolCreate(rootPool: Pool, poolName: String, expectMinShare: Int, expectWeight: Int, expectMode: SchedulingMode): Unit = {
    val targetPool = rootPool.getSchedulableByName(poolName)
    assert(targetPool != null)
    assert(targetPool.minShare == expectMinShare)
    assert(targetPool.weight == expectWeight)
    assert(targetPool.schedulingMode == expectMode)
  }

  def scheduleTaskAndVerifyId(taskId: Int, rootPool: Pool, expectedStageId: Int): Unit = {
    val taskSetQueue = rootPool.getSortedTaskSetQueue
    val nextTaskSetToSchedule = taskSetQueue.find(t => t.runningTasks < t.numTasks)
    assert(nextTaskSetToSchedule.isDefined)
    nextTaskSetToSchedule.get.addRunningTask(taskId)
    assert(nextTaskSetToSchedule.get.stageId === expectedStageId)
  }
}
