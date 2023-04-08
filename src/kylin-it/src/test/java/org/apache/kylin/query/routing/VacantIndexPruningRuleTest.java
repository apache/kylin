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

package org.apache.kylin.query.routing;

import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegDetails;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.util.MetadataTestUtils;
import org.apache.kylin.util.OlapContextUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@MetadataInfo
class VacantIndexPruningRuleTest extends NLocalWithSparkSessionTest {

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
    }

    @AfterEach
    public void teardown() throws Exception {
        super.tearDown();
    }

    @Override
    public String getProject() {
        return "default";
    }

    @Test
    void testWithIndexMatchRulesUseVacantIndexes() throws SqlParseException {
        NDataModelManager modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelMgr.listAllModels().stream().filter(model -> !model.isBroken())
                .forEach(model -> cleanAlreadyExistingLayoutsInSegments(model.getId()));

        MetadataTestUtils.updateProjectConfig(getProject(), "kylin.query.index-match-rules",
                KylinConfigBase.USE_VACANT_INDEXES);
        try (QueryContext queryContext = QueryContext.current()) {
            String sql = "select max(LO_ORDERDATE) from ssb.lineorder";
            List<OLAPContext> olapContexts = OlapContextUtil.getOlapContexts(getProject(), sql);
            OLAPContext olapContext = olapContexts.get(0);
            StorageContext storageContext = olapContext.storageContext;
            Assertions.assertTrue(storageContext.isEmptyLayout());
            Assertions.assertTrue(queryContext.getQueryTagInfo().isVacant());
        }
    }

    @Test
    void testUnmatchedWithNullResult() throws SqlParseException {
        String modelId = "d67bf0e4-30f4-9248-2528-52daa80be91a";
        String sql = "select max(LO_ORDERPRIOTITY) from ssb.lineorder";
        List<OLAPContext> olapContexts = OlapContextUtil.getOlapContexts(getProject(), sql);
        OLAPContext olapContext = olapContexts.get(0);
        NDataflow df = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
        Map<String, String> matchedJoinGraphAliasMap = OlapContextUtil.matchJoins(df.getModel(), olapContext);
        olapContext.fixModel(df.getModel(), matchedJoinGraphAliasMap);

        Candidate candidate = new Candidate(df, olapContext, matchedJoinGraphAliasMap);
        VacantIndexPruningRule vacantIndexPruningRule = new VacantIndexPruningRule();
        vacantIndexPruningRule.apply(candidate);
        Assertions.assertNull(candidate.getCapability());
    }

    @Test
    void testUnmatchedWithRealizationIsStreaming() throws SqlParseException {
        String modelId = "d67bf0e4-30f4-9248-2528-52daa80be91a";
        String sql = "select max(LO_ORDERDATE) from ssb.lineorder";
        NDataModelManager modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelMgr.listAllModels().stream().filter(model -> !model.isBroken())
                .forEach(model -> cleanAlreadyExistingLayoutsInSegments(model.getId()));
        List<OLAPContext> olapContexts = OlapContextUtil.getOlapContexts(getProject(), sql);
        OLAPContext olapContext = olapContexts.get(0);
        NDataflow df = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
        Map<String, String> matchedJoinGraphAliasMap = OlapContextUtil.matchJoins(df.getModel(), olapContext);
        olapContext.fixModel(df.getModel(), matchedJoinGraphAliasMap);

        Candidate candidate = new Candidate(df, olapContext, matchedJoinGraphAliasMap);
        {
            df.getModel().setModelType(NDataModel.ModelType.STREAMING);
            candidate.setCapability(new CapabilityResult());
            VacantIndexPruningRule vacantIndexPruningRule = new VacantIndexPruningRule();
            vacantIndexPruningRule.apply(candidate);
            Assertions.assertFalse(candidate.getCapability().isCapable());
        }

        {
            df.getModel().setModelType(NDataModel.ModelType.HYBRID);
            candidate.setCapability(new CapabilityResult());
            VacantIndexPruningRule vacantIndexPruningRule = new VacantIndexPruningRule();
            vacantIndexPruningRule.apply(candidate);
            Assertions.assertFalse(candidate.getCapability().isCapable());
        }
    }

    @Test
    void testUnmatchedAggIndex() throws SqlParseException {
        String modelId = "d67bf0e4-30f4-9248-2528-52daa80be91a";
        String sql = "select max(LO_ORDERPRIOTITY) from ssb.lineorder";
        List<OLAPContext> olapContexts = OlapContextUtil.getOlapContexts(getProject(), sql);
        OLAPContext olapContext = olapContexts.get(0);
        NDataflow df = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
        Map<String, String> matchedJoinGraphAliasMap = OlapContextUtil.matchJoins(df.getModel(), olapContext);
        olapContext.fixModel(df.getModel(), matchedJoinGraphAliasMap);

        Candidate candidate = new Candidate(df, olapContext, matchedJoinGraphAliasMap);
        candidate.setCapability(new CapabilityResult());
        VacantIndexPruningRule vacantIndexPruningRule = new VacantIndexPruningRule();
        vacantIndexPruningRule.apply(candidate);
        Assertions.assertFalse(candidate.getCapability().isCapable());
    }

    @Test
    void testMatchedTableIndex() throws SqlParseException {
        String modelId = "d67bf0e4-30f4-9248-2528-52daa80be91a";
        String sql = "select LO_QUANTITY from ssb.lineorder";
        NDataModelManager modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelMgr.listAllModels().stream().filter(model -> !model.isBroken())
                .forEach(model -> cleanAlreadyExistingLayoutsInSegments(model.getId()));
        List<OLAPContext> olapContexts = OlapContextUtil.getOlapContexts(getProject(), sql);
        OLAPContext olapContext = olapContexts.get(0);
        NDataflow df = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
        Map<String, String> matchedJoinGraphAliasMap = OlapContextUtil.matchJoins(df.getModel(), olapContext);
        olapContext.fixModel(df.getModel(), matchedJoinGraphAliasMap);

        Candidate candidate = new Candidate(df, olapContext, matchedJoinGraphAliasMap);
        candidate.setCapability(new CapabilityResult());
        VacantIndexPruningRule vacantIndexPruningRule = new VacantIndexPruningRule();
        vacantIndexPruningRule.apply(candidate);
        Assertions.assertTrue(candidate.getCapability().isCapable());
        Assertions.assertTrue(candidate.getCapability().isVacant());
        NLayoutCandidate selectedCandidate = (NLayoutCandidate) candidate.getCapability().getSelectedCandidate();
        Assertions.assertEquals(20000000001L, selectedCandidate.getLayoutEntity().getId());
    }

    @Test
    void testMatchedAggIndex() throws SqlParseException {
        String modelId = "d67bf0e4-30f4-9248-2528-52daa80be91a";
        String sql = "select max(LO_ORDERDATE) from ssb.lineorder";
        NDataModelManager modelMgr = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelMgr.listAllModels().stream().filter(model -> !model.isBroken())
                .forEach(model -> cleanAlreadyExistingLayoutsInSegments(model.getId()));
        List<OLAPContext> olapContexts = OlapContextUtil.getOlapContexts(getProject(), sql);
        OLAPContext olapContext = olapContexts.get(0);
        NDataflow df = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
        Map<String, String> matchedJoinGraphAliasMap = OlapContextUtil.matchJoins(df.getModel(), olapContext);
        olapContext.fixModel(df.getModel(), matchedJoinGraphAliasMap);

        Candidate candidate = new Candidate(df, olapContext, matchedJoinGraphAliasMap);
        candidate.setCapability(new CapabilityResult());
        VacantIndexPruningRule vacantIndexPruningRule = new VacantIndexPruningRule();
        vacantIndexPruningRule.apply(candidate);
        Assertions.assertTrue(candidate.getCapability().isCapable());
        Assertions.assertTrue(candidate.getCapability().isVacant());
        NLayoutCandidate selectedCandidate = (NLayoutCandidate) candidate.getCapability().getSelectedCandidate();
        Assertions.assertEquals(1L, selectedCandidate.getLayoutEntity().getId());
    }

    private void cleanAlreadyExistingLayoutsInSegments(String modelId) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
            NDataflow dataflow = dfMgr.getDataflow(modelId);
            NDataSegment latestReadySegment = dataflow.getLatestReadySegment();
            if (latestReadySegment != null) {
                NDataSegDetails segDetails = latestReadySegment.getSegDetails();
                List<NDataLayout> allLayouts = segDetails.getAllLayouts();

                // update
                NDataflowUpdate dataflowUpdate = new NDataflowUpdate(modelId);
                dataflowUpdate.setToRemoveLayouts(allLayouts.toArray(new NDataLayout[0]));
                dfMgr.updateDataflow(dataflowUpdate);
            }
            return null;
        }, getProject());
    }
}
