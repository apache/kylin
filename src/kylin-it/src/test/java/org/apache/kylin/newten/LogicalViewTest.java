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

package org.apache.kylin.newten;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.util.ExecAndComp;

import org.apache.spark.sql.SparderEnv;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

public class LogicalViewTest extends NLocalWithSparkSessionTest {

  private NDataflowManager dfMgr = null;

  @Before
  public void setup() throws Exception {
    // kylin.source.ddl.logical-view.enabled=true
    overwriteSystemProp("kylin.source.ddl.logical-view.enabled", "true");
    this.createTestMetadata("src/test/resources/ut_meta/logical_view");
    dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
    NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
    scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
    if (!scheduler.hasStarted()) {
      throw new RuntimeException("scheduler has not been started");
    }
  }

  @After
  public void after() throws Exception {
    NDefaultScheduler.destroyInstance();
    cleanupTestMetadata();
  }

  @Override
  public String getProject() {
    return "logical_view";
  }

  @Test
  public void testLogicalView() throws Exception {
    String dfID = "451e127a-b684-1474-744b-c9afc14378af";
    NDataflow dataflow = dfMgr.getDataflow(dfID);
    populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
    indexDataConstructor.buildIndex(dfID, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
        Sets.newHashSet(
            dataflow.getIndexPlan().getLayoutEntity(20000000001L),
            dataflow.getIndexPlan().getLayoutEntity(1L)), true);

    List<Pair<String, String>> query = new ArrayList<>();
    String sql1 = "select t1.C_CUSTKEY from KYLIN_LOGICAL_VIEW.LOGICAL_VIEW_TABLE t1"
        + " INNER JOIN SSB.CUSTOMER t2 on t1.C_CUSTKEY = t2.C_CUSTKEY ";
    query.add(Pair.newPair("logical_view", sql1));
    ExecAndComp.execAndCompare(
        query, getProject(), ExecAndComp.CompareLevel.NONE, "inner");
  }
}
