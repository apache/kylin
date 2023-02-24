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

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.job.stage.BuildParam
import org.apache.kylin.engine.spark.job.stage.build.FlatTableAndDictBase
import org.apache.kylin.engine.spark.job.{FiltersUtil, SegmentJob}
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree.AdaptiveTreeBuilder
import org.apache.kylin.metadata.cube.model._
import org.apache.kylin.metadata.model.SegmentRange
import org.apache.spark.sql.catalyst.expressions.{And, Expression, GreaterThanOrEqual}
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.{FilterExec, SparkPlan}
import org.junit.Assert
import org.mockito.Mockito

import scala.collection.JavaConverters._
import scala.collection.mutable.Set


class TestInferFilters extends SparderBaseFunSuite with AdaptiveSparkPlanHelper with SharedSparkSession with LocalMetadata {


  private val PROJECT = "infer_filter"
  private val MODEL_NAME1 = "89af4ee2-2cdb-4b07-b39e-4c29856309ab"

  def getTestConfig: KylinConfig = {
    KylinConfig.getInstanceFromEnv
  }

  override def beforeEach(): Unit = {
    FlatTableAndDictBase.inferFiltersEnabled = true
  }

  override def afterEach(): Unit = {
    FlatTableAndDictBase.inferFiltersEnabled = false
  }

  test("infer filters from join desc") {
    getTestConfig.setProperty("kylin.engine.persist-flattable-enabled", "false")
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, PROJECT)
    val df: NDataflow = dsMgr.getDataflow(MODEL_NAME1)
    // cleanup all segments first
    val update = new NDataflowUpdate(df.getUuid)
    update.setToRemoveSegsWithArray(df.getSegments.asScala.toArray)
    dsMgr.updateDataflow(update)

    val seg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1356019200000L))
    val toBuildTree = new AdaptiveSpanningTree(getTestConfig, new AdaptiveTreeBuilder(seg, seg.getIndexPlan.getAllLayouts))
    val segmentJob = Mockito.mock(classOf[SegmentJob])
    Mockito.when(segmentJob.getSparkSession).thenReturn(spark)
    val buildParam = new BuildParam()
    new TestFlatTable(segmentJob, seg, buildParam).test(getTestConfig, toBuildTree)

    val filters = getFilterPlan(buildParam.getFlatTable.queryExecution.executedPlan)

    Assert.assertTrue(Set("EDW.TEST_CAL_DT.CAL_DT", "DEFAULT.TEST_KYLIN_FACT.CAL_DT",
      "DEFAULT.TEST_ORDER.TEST_DATE_ENC").subsetOf(FiltersUtil.getAllEqualColSets))
    Assert.assertEquals(filters.size, 3)

  }

  private def getFilterPlan(plan: SparkPlan): Set[SparkPlan] = {
    val filterPlanSet: Set[SparkPlan] = Set.empty[SparkPlan]
    foreach(plan) {
      case node: FilterExec =>
        splitConjunctivePredicates(node.condition).find { p =>
          p.isInstanceOf[GreaterThanOrEqual]
        } match {
          case Some(x) =>
            filterPlanSet.add(node)
          case None =>
        }
      case _ =>
    }
    filterPlanSet
  }

  private def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }
}
