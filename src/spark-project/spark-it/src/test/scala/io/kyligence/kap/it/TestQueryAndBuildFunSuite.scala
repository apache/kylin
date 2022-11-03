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

package io.kyligence.kap.it

import io.kyligence.kap.common.{CompareSupport, JobSupport, QuerySupport, SSSource}
import io.kyligence.kap.query.{QueryConstants, QueryFetcher}
import io.netty.util.internal.ThrowableUtil
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.TestUtils
import org.apache.kylin.engine.spark.IndexDataWarehouse
import org.apache.kylin.metadata.cube.model.NDataflowManager.NDataflowUpdater
import org.apache.kylin.metadata.cube.model.{NDataflow, NDataflowManager}
import org.apache.kylin.metadata.realization.RealizationStatusEnum
import org.apache.spark.internal.Logging
import org.apache.spark.sql.common.{LocalMetadata, SparderBaseFunSuite}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.execution.{KylinFileSourceScanExec, LayoutFileSourceScanExec}
import org.apache.spark.sql.{DataFrame, SparderEnv}

import java.io.File
import java.util.TimeZone

class TestQueryAndBuildFunSuite
  extends SparderBaseFunSuite
    with LocalMetadata
    with JobSupport
    with QuerySupport
    with CompareSupport
    with SSSource
    with AdaptiveSparkPlanHelper
    with Logging {

  override val DEFAULT_PROJECT = "default"

  case class FolderInfo(folder: String, filter: List[String] = List(), checkOrder: Boolean = false)

  val defaultTimeZone: TimeZone = TimeZone.getDefault

  val queryFolders = List(
    FolderInfo("sql", List("query105.sql", "query131.sql", "query138.sql")),
    FolderInfo("sql_cache"),
    FolderInfo("sql_casewhen", List("query08.sql", "query09.sql", "query11.sql", "query12.sql", "query13.sql")),
    FolderInfo("sql_cross_join"),
    FolderInfo("sql_datetime"),
    FolderInfo("sql_derived"),
    FolderInfo("sql_distinct_dim"),
    FolderInfo("sql_hive"),
    FolderInfo("sql_join",
      List("query_11.sql", "query_12.sql", "query_13.sql", "query_14.sql", "query_15.sql", "query_16.sql",
        "query_17.sql", "query_18.sql", "query_19.sql", "query_20.sql", "query_25.sql")),
    FolderInfo("sql_join/sql_right_join"),
    FolderInfo("sql_kap", List("query03.sql")),
    FolderInfo("sql_like", List("query25.sql", "query26.sql")),
    FolderInfo("sql_lookup"),
    FolderInfo("sql_magine", List("query13.sql")),
    FolderInfo("sql_magine_left"),
    FolderInfo("sql_subquery", List("query19.sql", "query25.sql")),
    FolderInfo("sql_orderby", List(), checkOrder = true),
    FolderInfo("sql_powerbi"),
    // FolderInfo("sql_raw"),
    // FolderInfo("sql_rawtable", List("query26.sql", "query32.sql", "query33.sql", "query34.sql", "query37.sql", "query38.sql")),
    FolderInfo("sql_tableau", List("query00.sql", "query24.sql", "query25.sql")),
    FolderInfo("sql_topn"),
    // FolderInfo("sql_union", List("query07.sql")),
    FolderInfo("sql_value"),
    FolderInfo("sql_udf", List("query02.sql")),
    FolderInfo("sql_tableau", List("query00.sql", "query24.sql", "query25.sql"))
  )

  val onlyLeft = List(
    FolderInfo("sql_computedcolumn"),
    FolderInfo("sql_computedcolumn/sql_computedcolumn_common"),
    FolderInfo("sql_computedcolumn/sql_computedcolumn_leftjoin")
  )

  val onlyInner = List(
    FolderInfo("sql_join/sql_inner_join")
  )

  val isNotDistinctFrom = List(
    FolderInfo("sql_join/sql_is_not_distinct_from")
  )

  val noneCompare = List(
    FolderInfo("sql_current_date"),
    FolderInfo("sql_distinct"),
    FolderInfo("sql_grouping", List("query07.sql", "query08.sql")),
    FolderInfo("sql_h2_uncapable"),
    FolderInfo("sql_percentile"),
    FolderInfo("sql_window")
  )

  val tempQuery = List(
    FolderInfo("temp")
  )

  val joinTypes = List(
    "left",
    "inner"
  )
  // opt memory
  conf.set("spark.shuffle.detectCorrupt", "false")
  conf.set("spark.ui.enabled", "false")

  private val DF_NAME = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96"

  case class Updater(status: RealizationStatusEnum) extends NDataflowUpdater {
    override def modify(copyForWrite: NDataflow): Unit = copyForWrite.setStatus(status)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    overwriteSystemProp("calcite.keep-in-clause", "true")
    overwriteSystemProp("kylin.dictionary.null-encoding-opt-threshold", "1")
    overwriteSystemProp("kylin.query.spark-job-trace-enabled", "false")
    val timeZoneStr = "GMT+8"
    TimeZone.setDefault(TimeZone.getTimeZone(timeZoneStr))
    logInfo(s"Current time zone set to $timeZoneStr")
    NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, DEFAULT_PROJECT)
      .updateDataflow(DF_NAME, Updater(RealizationStatusEnum.OFFLINE))
    KylinConfig.getInstanceFromEnv.setProperty("kylin.query.pushdown.runner-class-name", "")
    KylinConfig.getInstanceFromEnv.setProperty("kylin.query.pushdown-enabled", "false")
    KylinConfig.getInstanceFromEnv.setProperty("kylin.snapshot.parallel-build-enabled", "true")
    // test for snapshot cleanup
    KylinConfig.getInstanceFromEnv.setProperty("kylin.snapshot.version-ttl", "0")
    KylinConfig.getInstanceFromEnv.setProperty("kylin.snapshot.max-versions", "1")
    build()
  }

  override def afterAll(): Unit = {
    NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, DEFAULT_PROJECT)
      .updateDataflow(DF_NAME, Updater(RealizationStatusEnum.ONLINE))
    SparderEnv.cleanCompute()
    TimeZone.setDefault(defaultTimeZone)
  }

  test("buildKylinFact") {
    var result = queryFolders
      .flatMap { folder =>
        queryFolder(folder, joinTypes)
      }
      .filter(_ != null)
    if (result.nonEmpty) {
      print(result)
    }
    assert(result.isEmpty)

    result = onlyLeft
      .flatMap { folder =>
        queryFolder(folder, List("left"))
      }
      .filter(_ != null)
    if (result.nonEmpty) {
      print(result)
    }
    assert(result.isEmpty)

    result = noneCompare
      .flatMap { folder =>
        queryFolderWithoutCompare(folder)
      }
      .filter(_ != null)
    assert(result.isEmpty)

    try {
      changeCubeStatus("89af4ee2-2cdb-4b07-b39e-4c29856309aa", RealizationStatusEnum.OFFLINE)
      result = onlyInner
        .flatMap { folder =>
          queryFolder(folder, List("inner"))
        }
        .filter(_ != null)
      assert(result.isEmpty)
    } finally {
      changeCubeStatus("89af4ee2-2cdb-4b07-b39e-4c29856309aa", RealizationStatusEnum.ONLINE)
    }
  }

  // for test scenario in timestamp type , see NSegPruningTest.testSegPruningWithTimeStamp()
  test("segment pruning in date type") {
    // two segs, ranges:
    // [2010-01-01, 2013-01-01)
    // [2013-01-01, 2015-01-01)
    val no_pruning1 = "select count(*) from TEST_KYLIN_FACT"
    val no_pruning2 = "select count(*) from TEST_KYLIN_FACT where CAL_DT > DATE '2010-01-01' and CAL_DT < DATE '2015-01-01'"

    val seg_pruning1 = "select count(*) from TEST_KYLIN_FACT where CAL_DT < DATE '2013-01-01'"
    val seg_pruning2 = "select count(*) from TEST_KYLIN_FACT where CAL_DT > DATE '2013-01-01'"
    assertNumScanFile(no_pruning1, 2)
    assertNumScanFile(no_pruning2, 2)
    assertNumScanFile(seg_pruning1, 1)
    assertNumScanFile(seg_pruning2, 1)
  }


  test("ensure spark split filter strategy") {
    val sql1 = "select count(*) from TEST_KYLIN_FACT where (LSTG_SITE_ID=10 or LSTG_SITE_ID>0) and LSTG_SITE_ID<100"
    val sql2 = "select count(*) from TEST_KYLIN_FACT where LSTG_SITE_ID=10 or (LSTG_SITE_ID>0 and LSTG_SITE_ID<100)"
    assert(getFileSourceScanExec(singleQuery(sql1, DEFAULT_PROJECT)).dataFilters.size == 3)
    assert(getFileSourceScanExec(singleQuery(sql2, DEFAULT_PROJECT)).dataFilters.size == 1)
  }

  test("non-equal join with is not distinct from condition") {
    val result = isNotDistinctFrom
      .flatMap { folder =>
        queryFolder(folder, List("left"))
      }
      .filter(_ != null)
    if (result.nonEmpty) {
      print(result)
    }
    assert(result.isEmpty)
  }

  private def assertNumScanFile(sql: String, numScanFiles: Long): Unit = {
    val df = singleQuery(sql, DEFAULT_PROJECT)
    df.collect()
    val scanExec = getFileSourceScanExec(df)
    val actualNumScanFiles = scanExec.metrics("numFiles").value
    assert(actualNumScanFiles == numScanFiles)
  }

  private def getFileSourceScanExec(df: DataFrame) = {
    collectFirst(df.queryExecution.executedPlan) {
      case p: KylinFileSourceScanExec => p
      case p: LayoutFileSourceScanExec => p
    }.get
  }

  private def queryFolder(folderInfo: FolderInfo, joinType: List[String]): List[String] = {
    QueryFetcher
      .fetchQueries(QueryConstants.KYLIN_SQL_BASE_DIR + folderInfo.folder)
      .filter { tp =>
        !folderInfo.filter.contains(new File(tp._1).getName)
      }
      .flatMap {
        case (fileName: String, query: String) =>
          joinType.map { joinType =>
            runAndCompare(query, DEFAULT_PROJECT, joinType.toUpperCase, fileName, folderInfo.checkOrder)
          }
      }
      .filter(_ != null)
      .toList
  }

  private def queryFolderWithoutCompare(folderInfo: FolderInfo) = {
    QueryFetcher
      .fetchQueries(QueryConstants.KYLIN_SQL_BASE_DIR + folderInfo.folder)
      .filter { tp =>
        !folderInfo.filter.contains(new File(tp._1).getName)
      }
      .flatMap {
        case (fileName: String, query: String) =>
          joinTypes.map { joinType =>
            val afterChangeJoin = changeJoinType(query, joinType)
            try {
              singleQuery(afterChangeJoin, DEFAULT_PROJECT).collect()
              null
            } catch {
              case exception: Throwable =>
                s"$fileName \n$query \n${ThrowableUtil.stackTraceToString(exception)} "
            }
          }
      }
      .filter(_ != null)
      .toList
  }

  def build(): Unit = {
    if (TestUtils.isSkipBuild) {
      logInfo("Direct query")
      val config = KylinConfig.getInstanceFromEnv
      new IndexDataWarehouse(config, getProject, "")
        .reuseBuildData(new File("../examples/buildKylinFact"))
    } else {
      buildFourSegementAndMerge("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
      buildFourSegementAndMerge("741ca86a-1f13-46da-a59f-95fb68615e3a")

      // replace metadata with new one after build
      dumpMetadata()
      SchemaProcessor.checkSchema(spark, "89af4ee2-2cdb-4b07-b39e-4c29856309aa", DEFAULT_PROJECT)
      SchemaProcessor.checkSchema(spark, "741ca86a-1f13-46da-a59f-95fb68615e3a", DEFAULT_PROJECT)
      checkOrder(spark, "89af4ee2-2cdb-4b07-b39e-4c29856309aa", DEFAULT_PROJECT)
      checkOrder(spark, "741ca86a-1f13-46da-a59f-95fb68615e3a", DEFAULT_PROJECT)
    }

  }
}
