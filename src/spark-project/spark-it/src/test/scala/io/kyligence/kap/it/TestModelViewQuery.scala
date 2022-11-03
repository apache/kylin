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
import io.kyligence.kap.query.QueryFetcher
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.StringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.persistence.{JsonSerializer, RootPersistentEntity}
import org.apache.kylin.common.util.Unsafe
import org.apache.kylin.engine.spark.utils.LogEx
import org.apache.kylin.metadata.cube.model.{IndexPlan, NDataflowManager, NIndexPlanManager}
import org.apache.kylin.metadata.model.{NDataModel, NDataModelManager}
import org.apache.kylin.metadata.realization.NoRealizationFoundException
import org.apache.spark.sql._
import org.apache.spark.sql.common.{LocalMetadata, SparderBaseFunSuite}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import java.io.{DataInputStream, File}
import java.nio.charset.Charset
import java.nio.file.Files
import java.sql.SQLException
import java.util.TimeZone
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

  case class FloderInfo(floder: String, filter: List[String] = List(), checkOrder: Boolean = false)

  val defaultTimeZone: TimeZone = TimeZone.getDefault

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  import scala.collection.JavaConverters._

  // opt memory
  conf.set("spark.shuffle.detectCorrupt", "false")

  override protected def getProject: String = DEFAULT_PROJECT

  override def beforeAll(): Unit = {
    Unsafe.setProperty("calcite.keep-in-clause", "true")
    Unsafe.setProperty("kylin.dictionary.null-encoding-opt-threshold", "1")
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"))

    super.beforeAll()
    KylinConfig.getInstanceFromEnv.setProperty("kylin.query.pushdown.runner-class-name", "")
    KylinConfig.getInstanceFromEnv.setProperty("kylin.query.pushdown-enabled", "false")
    KylinConfig.getInstanceFromEnv.setProperty("kylin.snapshot.parallel-build-enabled", "true")

    addModels()

    build()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SparderEnv.cleanCompute()
    TimeZone.setDefault(defaultTimeZone)
    Unsafe.clearProperty("calcite.keep-in-clause")
  }

  private val modelIds = Seq(
    "fc883850-60a2-01c7-d9a1-f8782211bf87", "b0fab5a2-7c65-03e6-0dc8-8a396b05d833", "98d390ef-66ae-8acb-f57a-fe0bfb4fdf13");

  private def addModels(): Unit = {
    val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv, getProject)
    val indexPlanMgr = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv, getProject)
    val dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, getProject)
    modelIds.foreach { id =>
      val model = read(classOf[NDataModel], s"model_desc/$id.json")
      model.setProject(getProject)
      modelMgr.createDataModelDesc(model, "ADMIN")
      dfMgr.createDataflow(indexPlanMgr.createIndexPlan(
        read(classOf[IndexPlan], s"index_plan/$id.json")), "ADMIN")
    }
  }

  private def read[T <: RootPersistentEntity](clz: Class[T], subPath: String): T = {
    val path = "src/test/resources/view/" + subPath
    val contents = StringUtils.join(Files.readAllLines(new File(path).toPath, Charset.defaultCharset), "\n")
    val bais = IOUtils.toInputStream(contents, Charset.defaultCharset)
    new JsonSerializer[T](clz).deserialize(new DataInputStream(bais))
  }


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
      singleQuery("select\nsum(O_CUSTKEY)\nfrom TPCH.orders_join_customer", getProject);
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
