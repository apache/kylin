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

package org.apache.kylin.rest.service;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableBiMap;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.metadata.recommendation.candidate.LayoutMetric;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.ref.LayoutRef;
import org.apache.kylin.metadata.recommendation.ref.ModelColumnRef;
import org.apache.kylin.metadata.recommendation.ref.OptRecV2;
import org.apache.kylin.metadata.recommendation.ref.RecommendationRef;
import org.apache.kylin.query.util.ComputedColumnRewriter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.springframework.jdbc.core.JdbcTemplate;

import com.fasterxml.jackson.databind.JsonNode;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OptRecV2TestBase extends NLocalFileMetadataTestCase {

    @Getter
    private final String basePath;
    private final String modelPathPattern;
    private final String indexPathPattern;
    private final String recDirectory;
    private final String recPathPattern;

    protected NDataModelManager modelManager;
    protected NIndexPlanManager indexPlanManager;
    protected JdbcRawRecStore jdbcRawRecStore;
    private JdbcTemplate jdbcTemplate;
    protected NDataModel ndataModel;
    NDataflowManager dataflowManager;

    private final String[] modelUUIDs;

    public OptRecV2TestBase(String basePath, String[] modelUUIDs) {
        this.basePath = basePath;
        modelPathPattern = basePath + "/model_desc/%s.json";
        indexPathPattern = basePath + "/index_plan/%s.json";
        recDirectory = basePath + "/rec_items/";
        recPathPattern = basePath + "/rec_items/%s.json";
        this.modelUUIDs = modelUUIDs;
    }

    protected String getProject() {
        return "ssb";
    }

    protected String getDefaultUUID() {
        return modelUUIDs[0];
    }

    @Before
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        this.createTestMetadata();
        jdbcTemplate = JdbcUtil.getJdbcTemplate(getTestConfig());
        jdbcTemplate.batchUpdate("SHUTDOWN;");
        jdbcRawRecStore = new JdbcRawRecStore(getTestConfig());

        modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());

        List<RawRecItem> recItems = loadAllRecItems(recDirectory);
        recItems.forEach(recItem -> recItem.setState(RawRecItem.RawRecState.INITIAL));
        recItems.sort(Comparator.comparingInt(RawRecItem::getId));
        jdbcRawRecStore.saveWithoutCheck(recItems, false);

        ComputedColumnUtil.setEXTRACTOR(ComputedColumnRewriter::extractCcRexNode);
    }

    @After
    public void tearDown() throws Exception {
        if (jdbcTemplate != null) {
            jdbcTemplate.batchUpdate("SHUTDOWN;");
        }
        cleanupTestMetadata();
    }

    protected void prepareEnv(List<Integer> recommendItemIds) throws IOException {
        recommendRecItems(recommendItemIds);
        prepareModelAndIndex();
    }

    private void recommendRecItems(List<Integer> recommendItemIds) {
        List<RawRecItem> recommendedLayoutItems = Lists.newArrayList();
        for (int id : recommendItemIds) {
            recommendedLayoutItems.add(jdbcRawRecStore.queryById(id));
        }
        List<RawRecItem> addLayoutRecs = recommendedLayoutItems.stream()
                .filter(item -> item.getType() == RawRecItem.RawRecType.ADDITIONAL_LAYOUT).collect(Collectors.toList());
        List<RawRecItem> removeLayoutRecs = recommendedLayoutItems.stream()
                .filter(item -> item.getType() == RawRecItem.RawRecType.REMOVAL_LAYOUT).collect(Collectors.toList());

        changeLayoutRecItemState(addLayoutRecs, RawRecItem.RawRecState.RECOMMENDED);
        changeLayoutRecItemState(removeLayoutRecs, RawRecItem.RawRecState.INITIAL);
        jdbcRawRecStore.batchAddOrUpdate(recommendedLayoutItems);

        Map<Integer, RawRecItem> map = Maps.newHashMap();
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        rawRecItems.forEach(rawRecItem -> map.put(rawRecItem.getId(), rawRecItem));
        for (Integer id : recommendItemIds) {
            log.trace("set RawRecItem({}) to recommended", id);
            Assert.assertTrue(map.containsKey(id));
            RawRecItem recItem = map.get(id);
            if (recItem.getType() == RawRecItem.RawRecType.ADDITIONAL_LAYOUT) {
                Assert.assertEquals(RawRecItem.RawRecState.RECOMMENDED, recItem.getState());
            } else {
                Assert.assertEquals(RawRecItem.RawRecState.INITIAL, recItem.getState());
            }
        }
    }

    protected void changeRecTopN(int topN) {
        FavoriteRule recRule = FavoriteRule.getDefaultRuleIfNull(null, FavoriteRule.REC_SELECT_RULE_NAME);
        FavoriteRule.Condition abstractCondition = (FavoriteRule.Condition) recRule.getConds().get(0);
        abstractCondition.setRightThreshold(String.valueOf(topN));
        FavoriteRuleManager favoriteRuleManager = FavoriteRuleManager.getInstance(getProject());
        favoriteRuleManager.createRule(recRule);
    }

    protected void prepareModelAndIndex() throws IOException {
        UnitOfWork.doInTransactionWithRetry(() -> {
            for (String id : modelUUIDs) {
                NDataModel dataModel = JsonUtil.readValue(new File(String.format(Locale.ROOT, modelPathPattern, id)),
                        NDataModelWrapper.class);
                dataModel.setProject(getProject());
                IndexPlan indexPlan = JsonUtil.readValue(new File(String.format(Locale.ROOT, indexPathPattern, id)),
                        IndexPlan.class);
                indexPlan.setProject(getProject());
                KylinConfig conf = getTestConfig();
                String project = getProject();
                NDataModelManager.getInstance(conf, project).createDataModelDesc(dataModel, dataModel.getOwner());
                NIndexPlanManager.getInstance(conf, project).createIndexPlan(indexPlan);
                NDataflowManager dfManager = NDataflowManager.getInstance(conf, project);
                dfManager.createDataflow(indexPlan, dataModel.getOwner());
                dfManager.updateDataflowStatus(id, RealizationStatusEnum.ONLINE);
            }
            return true;
        }, getProject());
        ndataModel = modelManager.getDataModelDesc(getDefaultUUID());
    }

    protected NDataModel getModel() {
        return modelManager.getDataModelDesc(getDefaultUUID());
    }

    protected IndexPlan getIndexPlan() {
        return indexPlanManager.getIndexPlan(getDefaultUUID());
    }

    private void changeLayoutRecItemState(List<RawRecItem> allRecItems, RawRecItem.RawRecState state) {
        allRecItems.forEach(recItem -> recItem.setState(state));
    }

    private List<RawRecItem> loadAllRecItems(String dirPath) throws IOException {
        List<RawRecItem> allRecItems = Lists.newArrayList();
        File directory = new File(dirPath);
        for (File file : Objects.requireNonNull(directory.listFiles())) {
            String uuid = file.getName().substring(0, file.getName().lastIndexOf('.'));
            String recItemContent = FileUtils
                    .readFileToString(new File(String.format(Locale.ROOT, recPathPattern, uuid)));
            allRecItems.addAll(parseRecItems(recItemContent));
        }
        return allRecItems;
    }

    private List<RawRecItem> parseRecItems(String recItemContent) throws IOException {
        List<RawRecItem> recItems = Lists.newArrayList();
        JsonNode jsonNode = JsonUtil.readValueAsTree(recItemContent);
        final Iterator<JsonNode> elements = jsonNode.elements();
        while (elements.hasNext()) {
            JsonNode recItemNode = elements.next();
            RawRecItem item = parseRawRecItem(recItemNode);
            recItems.add(item);
        }
        return recItems;
    }

    private RawRecItem parseRawRecItem(JsonNode recItemNode) throws IOException {
        RawRecItem item = new RawRecItem();
        item.setId(recItemNode.get("id").asInt());
        item.setProject(recItemNode.get("project").asText());
        item.setModelID(recItemNode.get("model_id").asText());
        item.setUniqueFlag(recItemNode.get("unique_flag").asText());
        item.setSemanticVersion(recItemNode.get("semantic_version").asInt());
        byte type = (byte) recItemNode.get("type").asInt();
        item.setType(RawRecItem.toRecType(type));
        item.setRecEntity(RawRecItem.toRecItem(recItemNode.get("rec_entity").asText(), type));
        item.setDependIDs(RawRecItem.toDependIds(recItemNode.get("depend_ids").asText()));
        // item.setLayoutMetric(null)
        item.setCost(recItemNode.get("cost").asDouble());
        item.setTotalLatencyOfLastDay(recItemNode.get("total_latency_of_last_day").asDouble());
        item.setHitCount(recItemNode.get("hit_count").asInt());
        item.setTotalTime(recItemNode.get("total_time").asDouble());
        item.setMaxTime(recItemNode.get("max_time").asDouble());
        item.setMinTime(recItemNode.get("min_time").asDouble());
        item.setState(RawRecItem.toRecState((byte) recItemNode.get("state").asInt()));
        item.setUpdateTime(recItemNode.get("update_time").asLong());
        item.setCreateTime(recItemNode.get("create_time").asLong());
        item.setRecSource(recItemNode.get("reserved_field_1").asText());
        item.setLayoutMetric(JsonUtil.readValue(recItemNode.get("layout_metric").asText(), LayoutMetric.class));
        return item;
    }

    protected void checkAllDependency(Dependency expectedDep, OptRecV2 actualDep) {
        checkDimRef(expectedDep.dimDep, actualDep.getDimensionRefs());
        checkMeasureRef(expectedDep.measureDep, actualDep.getMeasureRefs());
        checkLayoutRef(expectedDep.layoutDep, actualDep.getAdditionalLayoutRefs());
        checkCCRef(expectedDep.ccDep, actualDep.getCcRefs());
        checkColumnRef(expectedDep.colSize, actualDep.getColumnRefs());
        for (RecommendationRef ref : actualDep.getCcRefs().values()) {
            Assert.assertEquals(expectedDep.existed, ref.isExisted());
            Assert.assertEquals(expectedDep.cross, ref.isCrossModel());
        }
    }

    private void checkMeasureRef(Map<Integer, List<Integer>> expectedMeasureDep,
            Map<Integer, RecommendationRef> measureRefs) {
        measureRefs.forEach((refId, ref) -> {
            Assert.assertEquals(expectedMeasureDep.size(), measureRefs.size());
            Assert.assertTrue(expectedMeasureDep.containsKey(ref.getId()));
            if (ref.getId() > 0) {

                NDataModel.Measure measure = ndataModel.getAllMeasures().stream().filter(m -> ref.getId() == m.getId())
                        .findFirst().get();
                Assert.assertEquals(measure.getName(), ref.getName());
                Assert.assertTrue(ref.isExisted());
                Assert.assertFalse(ref.isBroken());
                Assert.assertTrue(ref.getDependencies().isEmpty());
            } else {
                Assert.assertFalse(ref.isExisted());
                Assert.assertFalse(ref.isBroken());
                List<Integer> depedencyId = expectedMeasureDep.get(refId);
                Assert.assertEquals(depedencyId.size(), ref.getDependencies().size());

                for (int n = 0; n < depedencyId.size(); n++) {
                    Assert.assertEquals(ref.getDependencies().get(n).getId(), depedencyId.get(n).intValue());
                    if (depedencyId.get(n) > 0) {
                        Assert.assertTrue(ref.getDependencies().get(n) instanceof ModelColumnRef);
                    }
                }

            }
        });
    }

    private void checkLayoutRef(Map<Integer, List<Integer>> expectedLayoutDep, Map<Integer, LayoutRef> layoutRefs) {
        Assert.assertEquals(expectedLayoutDep.size(), layoutRefs.size());
        layoutRefs.forEach((refId, ref) -> {

            Assert.assertTrue(expectedLayoutDep.containsKey(ref.getId()));
            List<Integer> execptedDependencies = expectedLayoutDep.get(ref.getId());
            Assert.assertFalse(ref.isExisted());
            Assert.assertFalse(ref.isBroken());
            List<RecommendationRef> dependencies = ref.getDependencies();
            Assert.assertEquals(execptedDependencies.size(), dependencies.size());
            for (int n = 0; n < dependencies.size(); n++) {
                Assert.assertEquals(dependencies.get(n).getId(), execptedDependencies.get(n).intValue());
            }
        });
    }

    private void checkDimRef(Map<Integer, Integer> expectedDimDep, Map<Integer, RecommendationRef> dimensionRefs) {
        Assert.assertEquals(expectedDimDep.size(), dimensionRefs.size());
        dimensionRefs.forEach((refId, ref) -> {
            Integer modelDimId = expectedDimDep.get(ref.getId());
            Assert.assertNotNull(modelDimId);
            NDataModel.NamedColumn modelCol = ndataModel.getAllNamedColumns().stream()
                    .filter(dimCol -> dimCol.getId() == modelDimId).findFirst().get();
            Assert.assertEquals(modelCol.getAliasDotColumn(), ref.getName());
            Assert.assertFalse(ref.isExisted());
            Assert.assertFalse(ref.isBroken());
            Assert.assertEquals(1, ref.getDependencies().size());
            RecommendationRef recommendationRef = ref.getDependencies().get(0);
            Assert.assertTrue(recommendationRef instanceof ModelColumnRef);
            Assert.assertEquals(modelDimId.intValue(), recommendationRef.getId());
        });
    }

    private void checkColumnRef(int expectedSize, Map<Integer, RecommendationRef> columnRefs) {
        Assert.assertEquals(expectedSize, columnRefs.size());
        columnRefs.forEach((refId, ref) -> {
            Assert.assertTrue(ref.getEntity() instanceof NDataModel.NamedColumn);
            Assert.assertTrue(ref.getId() >= 0);
            Assert.assertTrue(ref.isExisted());
            Assert.assertFalse(ref.isBroken());
            Assert.assertTrue(ref.getDependencies().isEmpty());
        });
    }

    private void checkCCRef(Map<Integer, List<Integer>> expectedCCDep, Map<Integer, RecommendationRef> ccRef) {
        ccRef.forEach((refId, ref) -> {
            Assert.assertEquals(expectedCCDep.size(), ccRef.size());
            Assert.assertTrue(expectedCCDep.containsKey(ref.getId()));
            if (ref.getId() < 0) {
                List<Integer> depedencyId = expectedCCDep.get(ref.getId());

                Assert.assertEquals(depedencyId.size(), ref.getDependencies().size());

                for (int n = 0; n < depedencyId.size(); n++) {
                    Assert.assertEquals(ref.getDependencies().get(n).getId(), depedencyId.get(n).intValue());
                    if (depedencyId.get(n) > 0) {
                        Assert.assertTrue(ref.getDependencies().get(n) instanceof ModelColumnRef);
                    }
                }
            }
        });

    }

    protected ImmutableMap<Integer, String> extractIdToName(
            ImmutableBiMap<Integer, NDataModel.Measure> effectiveMeasures) {
        ImmutableMap.Builder<Integer, String> builder = ImmutableMap.builder();
        effectiveMeasures.forEach((id, measure) -> builder.put(id, measure.getName()));
        return builder.build();
    }

    protected static class Dependency {
        Map<Integer, Integer> dimDep;
        Map<Integer, List<Integer>> measureDep;
        Map<Integer, List<Integer>> layoutDep;
        Map<Integer, List<Integer>> ccDep;
        private int colSize;
        private boolean existed;
        private boolean cross;

        public static class Builder {
            OptRecV2TestBase.Dependency dependency = new OptRecV2TestBase.Dependency();

            public Builder addDimDep(ImmutableMap<Integer, Integer> dimDep) {
                dependency.dimDep = dimDep;
                return this;
            }

            public Builder addLayDep(ImmutableMap<Integer, List<Integer>> layoutDep) {
                dependency.layoutDep = layoutDep;
                return this;
            }

            public Builder addCCDep(ImmutableMap<Integer, List<Integer>> ccDep) {
                dependency.ccDep = ccDep;
                return this;
            }

            public Builder addMeasureDep(ImmutableMap<Integer, List<Integer>> measureDep) {
                dependency.measureDep = measureDep;
                return this;
            }

            public OptRecV2TestBase.Dependency builder() {
                return dependency;
            }

            public Builder addColSize(int colSize) {
                dependency.colSize = colSize;
                return this;
            }

            public Builder setCCProperties(boolean existed, boolean cross) {
                dependency.existed = existed;
                dependency.cross = cross;
                return this;
            }
        }
    }
}
