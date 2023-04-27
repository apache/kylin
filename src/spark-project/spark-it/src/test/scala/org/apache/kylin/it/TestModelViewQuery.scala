/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.it

import java.sql.SQLException
import java.util.TimeZone

import org.apache.kylin.common._
import org.apache.kylin.engine.spark.utils.LogEx
import org.apache.kylin.metadata.realization.NoRealizationFoundException
import org.apache.kylin.query.QueryFetcher
import org.apache.spark.sql._
import org.apache.spark.sql.common.{LocalMetadata, SparderBaseFunSuite}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

// scalastyle:off
class TestModelViewQuery
  extends SparderBaseFunSuite
    with LocalMetadata
    with JobSupport
    with QuerySupport
    with CompareSupport
    with SSSource
    with AdaptiveSparkPlanHelper
    with LogEx {

  val dumpResult = false

  override val DEFAULT_PROJECT = "tpch"

  case class FolderInfo(folder: String, filter: List[String] = List(), checkOrder: Boolean = false)

  val defaultTimeZone: TimeZone = TimeZone.getDefault

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  import scala.collection.JavaConverters._

  // opt memory
  conf.set("spark.shuffle.detectCorrupt", "false")

  override protected def getProject: String = DEFAULT_PROJECT

  override def beforeAll(): Unit = {
    super.beforeAll()
    overwriteSystemProp("calcite.keep-in-clause", "true")
    overwriteSystemProp("kylin.dictionary.null-encoding-opt-threshold", "1")
    overwriteSystemProp("kylin.web.timezone", "GMT+8")
    overwriteSystemProp("kylin.query.pushdown.runner-class-name", "")
    overwriteSystemProp("kylin.query.pushdown-enabled", "false")
    overwriteSystemProp("kylin.snapshot.parallel-build-enabled", "true")

    addModels("src/test/resources/view/", modelIds)

    build()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SparderEnv.cleanCompute()
  }

  private val modelIds = Seq(
    "fc883850-60a2-01c7-d9a1-f8782211bf87",
    "b0fab5a2-7c65-03e6-0dc8-8a396b05d833",
    "98d390ef-66ae-8acb-f57a-fe0bfb4fdf13")

  def build(): Unit = logTime("Build Time: ", debug = true) {
    modelIds.foreach { id => fullBuildCube(id) }
    // replace metadata with new one after build
    dumpMetadata()
  }

  test("test view sqls") {
    overwriteSystemProp("kylin.query.auto-model-view-enabled", "true")
    val sqls = QueryFetcher
      .fetchQueries("src/test/resources/view/sqls").toSeq
    val modelSqls = sqls.zipWithIndex.filter { case (_, idx) => idx % 2 == 0 }.map { p => p._1 }
    val sparkSqls = sqls.zipWithIndex.filter { case (_, idx) => idx % 2 == 1 }.map { p => p._1 }

    val errMsg = modelSqls.zip(sparkSqls).map {
      case ((modelSqlPath, modelSql), (sparkSqlPath, sparkSql)) =>
        runAndCompare(modelSql, getProject, "DEFAULT", sparkSqlPath,
          checkOrder = false, Some(sparkSql),
          (modelResult, _) => {
            val expectedModels = modelSql.split(';')(0).substring(21).split(",")
            expectedModels.zip(modelResult.getOlapContexts.asScala).foreach { case (modelAlias, idx) =>
              assert(idx.getModelAlias == modelAlias, s"$modelSqlPath, view model fails to match")
            }
            true
          }
        )
    }.filter(_ != null)

    if (errMsg.nonEmpty) {
      print(errMsg)
    }
    assert(errMsg.isEmpty)
  }

  test("test no real found on view model") {
    overwriteSystemProp("kylin.query.auto-model-view-enabled", "true")
    val caught = intercept[SQLException] {
      singleQuery("select\nsum(O_CUSTKEY)\nfrom TPCH.orders_join_customer", getProject)
    }
    assert(caught.getCause.isInstanceOf[NoRealizationFoundException])
  }

  def removeDataBaseInSql(originSql: String): String = {
    originSql
      .replaceAll("(?i)TPCH\\.", "") //
      .replaceAll("\"TPCH\"\\.", "") //
      .replaceAll("`TPCH`\\.", "")
  }

}
