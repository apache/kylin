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

package org.apache.kylin.auto;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.util.ExecAndComp.CompareLevel;
import org.apache.kylin.util.SuggestTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AutoBuildAndQuerySumExprWithCountDistinctExprTest extends SuggestTestBase {

    private void fullBuildAllCube(String dfName, String prj) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, prj);
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        // ready dataflow, segment, cuboid layout
        NDataflow df = dsMgr.getDataflow(dfName);
        // cleanup all segments first
        indexDataConstructor.cleanSegments(df.getUuid());
        df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        List<LayoutEntity> round1 = Lists.newArrayList(layouts);
        indexDataConstructor.buildIndex(dfName, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newLinkedHashSet(round1), true);
    }

    @Test
    public void testUnionAllForSumExprWithCountDistinctExpr() throws Exception {
        String modelId = "73e06974-e642-6b91-e7a0-5cd7f02ec4f2";
        fullBuildAllCube(modelId, getProject());
        excludedSqlPatterns.addAll(loadWhiteListPatterns());
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "TRUE");
        executeTestScenario(BuildAndCompareContext.builder() //
                .testScenario(new TestScenario(CompareLevel.SAME, "query/sql_sum_expr_with_count_distinct_expr")) //
                .isCompareLayout(false) //
                .build());
    }

    @Override
    public String getProject() {
        return "sum_expr_with_count_distinct";
    }

}
