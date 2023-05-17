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
import java.util.Set;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.cuboid.NLookupCandidate;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegDetails;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.QueryableSeg;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.util.MetadataTestUtils;
import org.apache.kylin.util.OlapContextTestUtil;
import org.junit.Assert;
import org.junit.Test;

import com.clearspring.analytics.util.Lists;

public class DataflowCapabilityCheckerTest extends NLocalWithSparkSessionTest {

    private final long baseLayoutId = IndexEntity.TABLE_INDEX_START_ID + 1;
    private final long nonBaseLayoutId = IndexEntity.TABLE_INDEX_START_ID + IndexEntity.INDEX_ID_STEP + 1;

    @Test
    public void testCapabilityResult() throws SqlParseException {
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getDataflow("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        String sql = "SELECT seller_ID FROM TEST_KYLIN_FACT LEFT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID";
        OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
        Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
        olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
        Candidate candidate = new Candidate(dataflow, olapContext, sqlAlias2ModelNameMap);
        CapabilityResult result = DataflowCapabilityChecker.check(dataflow, candidate, olapContext.getSQLDigest());
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getSelectedCandidate().getCost(), result.getCost(), 0.001);
    }

    @Test
    public void testChoosingBetweenHdfsAndSecondStorage() throws SqlParseException {
        String modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        addTableIndexes(modelId);

        {
            String sql = "select a.NAME from TEST_BANK_INCOME a inner join TEST_BANK_LOCATION b \n"
                    + " on a.COUNTRY = b.COUNTRY group by a.Name ";
            NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql, true).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(),
                    olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            Candidate candidate = new Candidate(dataflow, olapContext, sqlAlias2ModelNameMap);
            candidate.getQueryableSeg().setBatchSegments(dataflow.getQueryableSegments());
            CapabilityResult result = DataflowCapabilityChecker.check(dataflow, candidate, olapContext.getSQLDigest());
            Assert.assertEquals(40001L, ((NLayoutCandidate) result.getSelectedCandidate()).getLayoutEntity().getId());
        }

        // without normal TableIndex, thus choose ch layout
        {
            MetadataTestUtils.updateProjectConfig(getProject(), "kylin.engine.segment-online-mode", "any");
            String sql = "select a.NAME from TEST_BANK_INCOME a inner join TEST_BANK_LOCATION b \n"
                    + " on a.COUNTRY = b.COUNTRY";
            NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql, true).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(),
                    olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            Candidate candidate = new Candidate(dataflow, olapContext, sqlAlias2ModelNameMap);
            QueryableSeg queryableSeg = candidate.getQueryableSeg();
            Segments<NDataSegment> queryableSegments = dataflow.getQueryableSegments();
            queryableSeg.setBatchSegments(queryableSegments);
            Map<String, Set<Long>> chSegToLayouts = Maps.newHashMap();
            queryableSegments.forEach(seg -> {
                List<Long> layoutIds = ImmutableList.of(IndexEntity.TABLE_INDEX_START_ID + 1);
                chSegToLayouts.putIfAbsent(seg.getId(), Sets.newHashSet());
                chSegToLayouts.get(seg.getId()).addAll(layoutIds);
            });
            queryableSeg.setChSegToLayoutsMap(chSegToLayouts);
            CapabilityResult result = DataflowCapabilityChecker.check(dataflow, candidate, olapContext.getSQLDigest());
            Assert.assertEquals(20000000001L,
                    ((NLayoutCandidate) result.getSelectedCandidate()).getLayoutEntity().getId());
        }
    }

    @Test
    public void testWithHighIntegrityCandidateChoosingSecondStorage() throws SqlParseException {
        String modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        addTableIndexes(modelId);
        Set<String> missingSegIds = ImmutableSet.of("d2edf0c5-5eb2-4968-9ad5-09efbf659324");
        clearAndAddDesiredTableIndex(modelId, missingSegIds, nonBaseLayoutId);

        // ch segments aren't heterogeneous
        {
            MetadataTestUtils.updateProjectConfig(getProject(), "kylin.engine.segment-online-mode", "any");
            MetadataTestUtils.updateProjectConfig(getProject(), "kylin.query.index-match-rules", "use-vacant-indexes");
            String sql = "select a.NAME from TEST_BANK_INCOME a inner join TEST_BANK_LOCATION b \n"
                    + " on a.COUNTRY = b.COUNTRY";
            NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql, true).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(),
                    olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            Candidate candidate = new Candidate(dataflow, olapContext, sqlAlias2ModelNameMap);
            QueryableSeg queryableSeg = candidate.getQueryableSeg();
            Segments<NDataSegment> queryableSegments = dataflow.getQueryableSegments();
            queryableSeg.setBatchSegments(queryableSegments);
            Map<String, Set<Long>> chSegToLayouts = Maps.newHashMap();
            queryableSegments.forEach(seg -> {
                List<Long> layoutIds = ImmutableList.of(baseLayoutId);
                chSegToLayouts.putIfAbsent(seg.getId(), Sets.newHashSet());
                chSegToLayouts.get(seg.getId()).addAll(layoutIds);
            });
            queryableSeg.setChSegToLayoutsMap(chSegToLayouts);
            CapabilityResult result = DataflowCapabilityChecker.check(dataflow, candidate, olapContext.getSQLDigest());
            Assert.assertEquals(20000000001L,
                    ((NLayoutCandidate) result.getSelectedCandidate()).getLayoutEntity().getId());
        }

        // ch segments are heterogeneous
        {
            MetadataTestUtils.updateProjectConfig(getProject(), "kylin.engine.segment-online-mode", "any");
            MetadataTestUtils.updateProjectConfig(getProject(), "kylin.query.index-match-rules", "use-vacant-indexes");
            String sql = "select a.NAME from TEST_BANK_INCOME a inner join TEST_BANK_LOCATION b \n"
                    + " on a.COUNTRY = b.COUNTRY";
            NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql, true).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(),
                    olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            Candidate candidate = new Candidate(dataflow, olapContext, sqlAlias2ModelNameMap);
            QueryableSeg queryableSeg = candidate.getQueryableSeg();
            Segments<NDataSegment> queryableSegments = dataflow.getQueryableSegments();
            queryableSeg.setBatchSegments(queryableSegments);
            Map<String, Set<Long>> chSegToLayouts = Maps.newHashMap();
            Set<String> missingChSegIds = Sets.newHashSet();
            missingChSegIds.add("d2edf0c5-5eb2-4968-9ad5-09efbf659324");
            missingChSegIds.add("73570f31-05a5-448f-973c-44209830dd01");
            queryableSegments.forEach(seg -> {
                if (missingChSegIds.contains(seg.getId())) {
                    return;
                }
                List<Long> layoutIds = ImmutableList.of(baseLayoutId);
                chSegToLayouts.putIfAbsent(seg.getId(), Sets.newHashSet());
                chSegToLayouts.get(seg.getId()).addAll(layoutIds);
            });
            queryableSeg.setChSegToLayoutsMap(chSegToLayouts);
            CapabilityResult result = DataflowCapabilityChecker.check(dataflow, candidate, olapContext.getSQLDigest());
            Assert.assertEquals(20000010001L,
                    ((NLayoutCandidate) result.getSelectedCandidate()).getLayoutEntity().getId());
        }
    }

    @Test
    public void testWithHighIntegrityCandidateChoosingBaseLayoutNonSecondStorage() throws SqlParseException {
        String modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        Set<String> missingSegIds = ImmutableSet.of("d2edf0c5-5eb2-4968-9ad5-09efbf659324");
        addTableIndexes(modelId);
        clearAndAddDesiredTableIndex(modelId, missingSegIds, baseLayoutId);

        MetadataTestUtils.updateProjectConfig(getProject(), "kylin.engine.segment-online-mode", "any");
        MetadataTestUtils.updateProjectConfig(getProject(), "kylin.query.index-match-rules", "use-vacant-indexes");
        String sql = "select a.NAME from TEST_BANK_INCOME a inner join TEST_BANK_LOCATION b \n"
                + " on a.COUNTRY = b.COUNTRY";
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
        OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql, true).get(0);
        Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
        olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
        Candidate candidate = new Candidate(dataflow, olapContext, sqlAlias2ModelNameMap);
        QueryableSeg queryableSeg = candidate.getQueryableSeg();
        Segments<NDataSegment> queryableSegments = dataflow.getQueryableSegments();
        queryableSeg.setBatchSegments(queryableSegments);
        Map<String, Set<Long>> chSegToLayouts = Maps.newHashMap();
        Set<String> missingChSegIds = Sets.newHashSet();
        missingChSegIds.add("d2edf0c5-5eb2-4968-9ad5-09efbf659324");
        missingChSegIds.add("73570f31-05a5-448f-973c-44209830dd01");
        queryableSegments.forEach(seg -> {
            if (missingChSegIds.contains(seg.getId())) {
                return;
            }
            List<Long> layoutIds = ImmutableList.of(baseLayoutId);
            chSegToLayouts.putIfAbsent(seg.getId(), Sets.newHashSet());
            chSegToLayouts.get(seg.getId()).addAll(layoutIds);
        });
        queryableSeg.setChSegToLayoutsMap(chSegToLayouts);
        CapabilityResult result = DataflowCapabilityChecker.check(dataflow, candidate, olapContext.getSQLDigest());
        Assert.assertTrue(result.isPartialResult());
        Assert.assertTrue(result.isCapable());
        NLayoutCandidate selectedCandidate = (NLayoutCandidate) result.getSelectedCandidate();
        Assert.assertEquals(259200000, selectedCandidate.getRange()); // from ch layout is 172800000
        Assert.assertEquals(baseLayoutId, selectedCandidate.getLayoutEntity().getId());
    }

    @Test
    public void testWithHighIntegrityCandidateChoosingBaseLayoutSecondStorage() throws SqlParseException {
        String modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        Set<String> missingSegIds = ImmutableSet.of("d2edf0c5-5eb2-4968-9ad5-09efbf659324",
                "73570f31-05a5-448f-973c-44209830dd01");
        addTableIndexes(modelId);
        clearAndAddDesiredTableIndex(modelId, missingSegIds, baseLayoutId);

        MetadataTestUtils.updateProjectConfig(getProject(), "kylin.engine.segment-online-mode", "any");
        MetadataTestUtils.updateProjectConfig(getProject(), "kylin.query.index-match-rules", "use-vacant-indexes");
        String sql = "select a.NAME from TEST_BANK_INCOME a inner join TEST_BANK_LOCATION b \n"
                + " on a.COUNTRY = b.COUNTRY";
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
        OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql, true).get(0);
        Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
        olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
        Candidate candidate = new Candidate(dataflow, olapContext, sqlAlias2ModelNameMap);
        QueryableSeg queryableSeg = candidate.getQueryableSeg();
        Segments<NDataSegment> queryableSegments = dataflow.getQueryableSegments();
        queryableSeg.setBatchSegments(queryableSegments);
        Map<String, Set<Long>> chSegToLayouts = Maps.newHashMap();
        Set<String> withoutChSegIdSet = Sets.newHashSet();
        withoutChSegIdSet.add("d2edf0c5-5eb2-4968-9ad5-09efbf659324");
        queryableSegments.forEach(seg -> {
            if (withoutChSegIdSet.contains(seg.getId())) {
                return;
            }
            List<Long> layoutIds = ImmutableList.of(baseLayoutId);
            chSegToLayouts.putIfAbsent(seg.getId(), Sets.newHashSet());
            chSegToLayouts.get(seg.getId()).addAll(layoutIds);
        });
        queryableSeg.setChSegToLayoutsMap(chSegToLayouts);
        CapabilityResult result = DataflowCapabilityChecker.check(dataflow, candidate, olapContext.getSQLDigest());
        Assert.assertTrue(result.isPartialResult());
        Assert.assertTrue(result.isCapable());
        NLayoutCandidate selectedCandidate = (NLayoutCandidate) result.getSelectedCandidate();
        Assert.assertEquals(259200000, selectedCandidate.getRange()); // from normal layout is 172800000
        Assert.assertEquals(baseLayoutId, selectedCandidate.getLayoutEntity().getId());
    }

    @Test
    public void testWithHighIntegrityCandidateChoosingSecondStorageWithLargeSegEnd() throws SqlParseException {
        String modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        Set<String> missingSegIds = ImmutableSet.of("d2edf0c5-5eb2-4968-9ad5-09efbf659324",
                "ff839b0b-2c23-4420-b332-0df70e36c343");
        addTableIndexes(modelId);
        clearAndAddDesiredTableIndex(modelId, missingSegIds, baseLayoutId);

        MetadataTestUtils.updateProjectConfig(getProject(), "kylin.engine.segment-online-mode", "any");
        MetadataTestUtils.updateProjectConfig(getProject(), "kylin.query.index-match-rules", "use-vacant-indexes");
        String sql = "select a.NAME from TEST_BANK_INCOME a inner join TEST_BANK_LOCATION b \n"
                + " on a.COUNTRY = b.COUNTRY";
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
        OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql, true).get(0);
        Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
        olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
        Candidate candidate = new Candidate(dataflow, olapContext, sqlAlias2ModelNameMap);
        QueryableSeg queryableSeg = candidate.getQueryableSeg();
        Segments<NDataSegment> queryableSegments = dataflow.getQueryableSegments();
        queryableSeg.setBatchSegments(queryableSegments);
        Map<String, Set<Long>> chSegToLayouts = Maps.newHashMap();
        Set<String> withoutChSegIdSet = ImmutableSet.of("d2edf0c5-5eb2-4968-9ad5-09efbf659324",
                "73570f31-05a5-448f-973c-44209830dd01");
        queryableSegments.forEach(seg -> {
            if (withoutChSegIdSet.contains(seg.getId())) {
                return;
            }
            List<Long> layoutIds = ImmutableList.of(baseLayoutId);
            chSegToLayouts.putIfAbsent(seg.getId(), Sets.newHashSet());
            chSegToLayouts.get(seg.getId()).addAll(layoutIds);
        });
        queryableSeg.setChSegToLayoutsMap(chSegToLayouts);
        CapabilityResult result = DataflowCapabilityChecker.check(dataflow, candidate, olapContext.getSQLDigest());
        Assert.assertTrue(result.isPartialResult());
        Assert.assertTrue(result.isCapable());
        NLayoutCandidate selectedCandidate = (NLayoutCandidate) result.getSelectedCandidate();
        Assert.assertEquals(172800000, selectedCandidate.getRange()); // from ch layout is 172800000
        Assert.assertEquals(1604505600000L, selectedCandidate.getMaxSegEnd());
        Assert.assertEquals(baseLayoutId, selectedCandidate.getLayoutEntity().getId());
    }

    @Test
    public void testWithHighIntegrityCandidateChoosingNonSecondStorageWithLargeSegEnd() throws SqlParseException {
        String modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        Set<String> missingSegIds = ImmutableSet.of("0db919f3-1359-496c-aab5-b6f3951adc0e",
                "d2edf0c5-5eb2-4968-9ad5-09efbf659324");
        addTableIndexes(modelId);
        clearAndAddDesiredTableIndex(modelId, missingSegIds, baseLayoutId);

        MetadataTestUtils.updateProjectConfig(getProject(), "kylin.engine.segment-online-mode", "any");
        MetadataTestUtils.updateProjectConfig(getProject(), "kylin.query.index-match-rules", "use-vacant-indexes");
        String sql = "select a.NAME from TEST_BANK_INCOME a inner join TEST_BANK_LOCATION b \n"
                + " on a.COUNTRY = b.COUNTRY";
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(modelId);
        OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql, true).get(0);
        Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
        olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
        Candidate candidate = new Candidate(dataflow, olapContext, sqlAlias2ModelNameMap);
        QueryableSeg queryableSeg = candidate.getQueryableSeg();
        Segments<NDataSegment> queryableSegments = dataflow.getQueryableSegments();
        queryableSeg.setBatchSegments(queryableSegments);
        Map<String, Set<Long>> chSegToLayouts = Maps.newHashMap();
        Set<String> missingChSegIds = ImmutableSet.of("d2edf0c5-5eb2-4968-9ad5-09efbf659324",
                "ff839b0b-2c23-4420-b332-0df70e36c343");
        queryableSegments.forEach(seg -> {
            if (missingChSegIds.contains(seg.getId())) {
                return;
            }
            List<Long> layoutIds = ImmutableList.of(baseLayoutId);
            chSegToLayouts.putIfAbsent(seg.getId(), Sets.newHashSet());
            chSegToLayouts.get(seg.getId()).addAll(layoutIds);
        });
        queryableSeg.setChSegToLayoutsMap(chSegToLayouts);
        CapabilityResult result = DataflowCapabilityChecker.check(dataflow, candidate, olapContext.getSQLDigest());
        Assert.assertTrue(result.isPartialResult());
        Assert.assertTrue(result.isCapable());
        NLayoutCandidate selectedCandidate = (NLayoutCandidate) result.getSelectedCandidate();
        Assert.assertEquals(172800000L, selectedCandidate.getRange()); // from normal layout is 172800000
        Assert.assertEquals(1604505600000L, selectedCandidate.getMaxSegEnd());
        Assert.assertEquals(baseLayoutId, selectedCandidate.getLayoutEntity().getId());
    }

    private void addTableIndexes(String modelId) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(getTestConfig(), getProject());
            indexMgr.updateIndexPlan(modelId, copyForWrite -> {
                IndexEntity index = new IndexEntity();
                index.setMeasures(Lists.newArrayList());
                index.setDimensions(ImmutableList.of(0, 1, 2, 3));
                index.setId(IndexEntity.TABLE_INDEX_START_ID);
                LayoutEntity layout = new LayoutEntity();
                layout.setId(index.getId() + 1);
                layout.setIndex(index);
                layout.setColOrder(ImmutableList.of(0, 1, 2, 3));
                layout.setBase(true);
                layout.setUpdateTime(System.currentTimeMillis());
                index.setLayouts(ImmutableList.of(layout));
                index.setIndexPlan(copyForWrite);
                copyForWrite.getIndexes().add(index);

                IndexEntity otherIndex = new IndexEntity();
                otherIndex.setMeasures(Lists.newArrayList());
                otherIndex.setDimensions(ImmutableList.of(0, 1, 2));
                otherIndex.setId(IndexEntity.TABLE_INDEX_START_ID + IndexEntity.INDEX_ID_STEP);
                LayoutEntity otherLayout = new LayoutEntity();
                otherLayout.setId(otherIndex.getId() + 1);
                otherLayout.setIndex(otherIndex);
                otherLayout.setColOrder(ImmutableList.of(0, 1, 2));
                otherLayout.setBase(true);
                otherLayout.setUpdateTime(System.currentTimeMillis());
                otherIndex.setLayouts(ImmutableList.of(otherLayout));
                otherIndex.setIndexPlan(copyForWrite);
                copyForWrite.getIndexes().add(otherIndex);
            });
            return null;
        }, getProject());
    }

    private void clearAndAddDesiredTableIndex(String modelId, Set<String> missingSegIds, long desiredLayoutId) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
            NDataflow df = dfMgr.getDataflow(modelId);
            List<NDataLayout> toAddLayouts = Lists.newArrayList();
            List<NDataLayout> toRemoveLayouts = Lists.newArrayList();
            for (NDataSegment segment : df.getSegments()) {
                NDataSegDetails segDetails = segment.getSegDetails();
                toRemoveLayouts.addAll(segDetails.getAllLayouts());
                if (!missingSegIds.contains(segment.getId())) {
                    NDataLayout layout = NDataLayout.newDataLayout(segDetails, desiredLayoutId);
                    layout.setRows(100);
                    toAddLayouts.add(layout);
                }
            }

            // update
            NDataflowUpdate dataflowUpdate = new NDataflowUpdate(modelId);
            dataflowUpdate.setToRemoveLayouts(toRemoveLayouts.toArray(new NDataLayout[0]));
            dataflowUpdate.setToAddOrUpdateLayouts(toAddLayouts.toArray(new NDataLayout[0]));
            dfMgr.updateDataflow(dataflowUpdate);
            return null;
        }, getProject());
    }

    @Test
    public void testLookupMatch() throws SqlParseException {
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        // prepare table desc snapshot path
        NTableMetadataManager.getInstance(dataflow.getConfig(), dataflow.getProject()).getTableDesc("EDW.TEST_SITES")
                .setLastSnapshotPath("default/table_snapshot/EDW.TEST_SITES/c1e8096e-4e7f-4387-b7c3-5147c1ce38d6");

        // case 1. raw-query answered by Lookup
        {
            String sql = "select SITE_ID from EDW.TEST_SITES";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(),
                    olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            Candidate candidate = new Candidate(dataflow, olapContext, sqlAlias2ModelNameMap);
            CapabilityResult result = DataflowCapabilityChecker.check(dataflow, candidate, olapContext.getSQLDigest());
            Assert.assertNotNull(result);
            Assert.assertTrue(result.getSelectedCandidate() instanceof NLookupCandidate);
            Assert.assertFalse(olapContext.getSQLDigest().allColumns.isEmpty());
            Assert.assertEquals(1, olapContext.getSQLDigest().allColumns.size());
        }

        // case 2. aggregate-query answered by lookup
        {
            String sql = "select sum(SITE_ID) from EDW.TEST_SITES";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(),
                    olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            Candidate candidate = new Candidate(dataflow, olapContext, sqlAlias2ModelNameMap);
            CapabilityResult result = DataflowCapabilityChecker.check(dataflow, candidate, olapContext.getSQLDigest());
            Assert.assertNotNull(result);
            Assert.assertTrue(result.getSelectedCandidate() instanceof NLookupCandidate);
            Assert.assertFalse(olapContext.getSQLDigest().allColumns.isEmpty());
            Assert.assertEquals(1, olapContext.getSQLDigest().allColumns.size());
        }

        {
            // case 3. cannot answer when there are no ready segment
            String sql = "select sum(SITE_ID) from EDW.TEST_SITES";
            OLAPContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            removeAllSegments(dataflow);
            dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                    .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            Candidate candidate = new Candidate(dataflow, olapContext, Maps.newHashMap());
            CapabilityResult result = DataflowCapabilityChecker.check(dataflow, candidate, olapContext.getSQLDigest());
            Assert.assertFalse(result.isCapable());
        }
    }

    private void removeAllSegments(NDataflow dataflow) {
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[0]));
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).updateDataflow(dataflowUpdate);
    }
}
