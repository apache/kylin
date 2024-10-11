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

package org.apache.kylin.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.springframework.jdbc.core.JdbcTemplate;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class SuggestTestBase extends NLocalWithSparkSessionTest {

    protected static final String IT_SQL_DIR = "../kylin-it/src/test/resources/";

    protected KylinConfig kylinConfig;
    private JdbcTemplate jdbcTemplate;
    protected Set<String> excludedSqlPatterns = Sets.newHashSet();

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        overwriteSystemProp("kylin.web.timezone", "GMT+8");
        jdbcTemplate = JdbcUtil.getJdbcTemplate(getTestConfig());
        new JdbcRawRecStore(getTestConfig());
        kylinConfig = getTestConfig();
    }

    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        if (jdbcTemplate != null) {
            jdbcTemplate.batchUpdate("SHUTDOWN;");
        }
        super.cleanupTestMetadata();
        ResourceStore.clearCache();
        excludedSqlPatterns = Sets.newHashSet();

        FileUtils.deleteQuietly(new File("../kylin-it/metastore_db"));
    }

    public Set<String> loadWhiteListPatterns() throws IOException {
        log.info("override loadWhiteListSqlPatterns in NAutoBuildAndQueryTest");

        Set<String> result = Sets.newHashSet();
        final String folder = getFolder("query/unchecked_layout_list");
        File[] files = new File(folder).listFiles();
        if (files == null || files.length == 0) {
            return result;
        }

        String[] fileContentArr = new String(getFileBytes(files[0]), StandardCharsets.UTF_8)
                .split(System.lineSeparator());
        final List<String> fileNames = Arrays.stream(fileContentArr)
                .filter(name -> !name.startsWith("-") && !name.isEmpty()) //
                .collect(Collectors.toList());
        final List<Pair<String, String>> queries = Lists.newArrayList();
        for (String name : fileNames) {
            File tmp = new File(SuggestTestBase.IT_SQL_DIR + "/" + name);
            final String sql = new String(getFileBytes(tmp), StandardCharsets.UTF_8);
            queries.add(new Pair<>(tmp.getCanonicalPath(), sql));
        }

        queries.forEach(pair -> {
            String sql = pair.getSecond(); // origin sql
            result.addAll(changeJoinType(sql));

            // add limit
            if (!sql.toLowerCase(Locale.ROOT).contains("limit ")) {
                result.addAll(changeJoinType(sql + " limit 5"));
            }
        });

        return result;
    }

    protected void dropAllModels() {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            NDataflowManager dfMgr = NDataflowManager.getInstance(config, getProject());
            NDataModelManager modelManager = NDataModelManager.getInstance(config, getProject());
            NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(config, getProject());
            List<NDataflow> dfList = dfMgr.listAllDataflows();
            for (NDataflow df : dfList) {
                modelManager.dropModel(df.getId());
                dfMgr.dropDataflow(df.getId());
                indexMgr.dropIndexPlan(df.getId());
            }
            return null;
        }, getProject());
    }

    @Override
    public String getProject() {
        return "newten";
    }

    protected String getFolder(String subFolder) {
        return IT_SQL_DIR + File.separator + subFolder;
    }

    protected void dumpMetadata() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        val metadataUrlPrefix = config.getMetadataUrlPrefix();
        val metadataUrl = metadataUrlPrefix + "/metadata";
        FileUtils.deleteQuietly(new File(metadataUrl));
        val resourceStore = ResourceStore.getKylinMetaStore(config);
        val outputConfig = KylinConfig.createKylinConfig(config);
        outputConfig.setMetadataUrl(metadataUrlPrefix);
        MetadataStore.createMetadataStore(outputConfig).dump(resourceStore);
    }

    public enum JoinType {

        /**
         * Left outer join.
         */
        LEFT,

        /**
         * Inner join
         */
        INNER,

        /**
         * original state
         */
        DEFAULT
    }

    @Getter
    @Setter
    public class TestScenario {

        Set<Pair<String, Long>> removeLayouts;
        String folderName;
        private ExecAndComp.CompareLevel compareLevel;
        JoinType joinType;
        private int fromIndex;
        private int toIndex;
        private boolean isLimit;
        private Set<String> exclusionList;
        private boolean isDynamicSql = false;
        private ConcurrentHashMap<String, Set<String>> failedQueries = new ConcurrentHashMap<>();

        // value when execute
        List<Pair<String, String>> queries;
        Set<String> ignoredPathComparingResult = Sets.newHashSet();

        public TestScenario(String folderName) {
            this(ExecAndComp.CompareLevel.SAME, folderName);
        }

        public TestScenario(ExecAndComp.CompareLevel compareLevel, String folder) {
            this(compareLevel, JoinType.DEFAULT, folder);
        }

        public TestScenario(ExecAndComp.CompareLevel compareLevel, JoinType joinType, String folder) {
            this(compareLevel, joinType, false, folder, 0, 0, null);
        }

        public TestScenario(ExecAndComp.CompareLevel compareLevel, String folder, int fromIndex, int toIndex) {
            this(compareLevel, JoinType.DEFAULT, false, folder, fromIndex, toIndex, null);
        }

        public TestScenario(ExecAndComp.CompareLevel compareLevel, String folder, JoinType joinType, int fromIndex,
                int toIndex) {
            this(compareLevel, joinType, false, folder, fromIndex, toIndex, null);
        }

        public TestScenario(ExecAndComp.CompareLevel compareLevel, boolean isLimit, String folder) {
            this(compareLevel, JoinType.DEFAULT, isLimit, folder, 0, 0, null);
        }

        public TestScenario(ExecAndComp.CompareLevel compareLevel, JoinType joinType, boolean isLimit,
                String folderName, int fromIndex, int toIndex, Set<String> exclusionList) {
            this.compareLevel = compareLevel;
            this.folderName = folderName;
            this.joinType = joinType;
            this.isLimit = isLimit;
            this.fromIndex = fromIndex;
            this.toIndex = toIndex;
            this.exclusionList = exclusionList;
            this.removeLayouts = Sets.newHashSet();
        }

        public void execute() throws Exception {
            executeTestScenario(BuildAndCompareContext.builder().testScenarios(Lists.newArrayList(this)).build());
        }

        public void execute(int storageType) throws Exception {
            executeTestScenario(BuildAndCompareContext.builder().storageType(storageType)
                    .testScenarios(Lists.newArrayList(this)).build());
        }

    } // end TestScenario

    protected void executeTestScenarioWithStorageType(Integer storageType,
            SuggestTestBase.TestScenario... testScenarios) throws Exception {
        executeTestScenario(BuildAndCompareContext.builder().storageType(storageType)
                .testScenarios(Lists.newArrayList(testScenarios)).build());
    }

    protected void executeTestScenario(Integer storageType, Integer expectModelNum, TestScenario... testScenarios)
            throws Exception {
        executeTestScenario(BuildAndCompareContext.builder().storageType(storageType).expectModelNum(expectModelNum)
                .testScenarios(Lists.newArrayList(testScenarios)).build());
    }

    protected void executeTestScenario(BuildAndCompareContext context) throws Exception {
        long startTime = System.currentTimeMillis();
        AbstractContext smartCtx = proposeWithSmartMaster(getProject(), context.getTestScenarios());
        exchangeStorageType(getProject(), context.getStorageType());
        updateAccelerateInfoMap(smartCtx);
        final Map<String, ExecAndCompExt.CompareEntity> compareMap = Maps.newConcurrentMap();
        compareMap.putAll(collectCompareEntity(smartCtx));
        context.setCompareMap(compareMap);
        log.debug("smart proposal cost {} ms", System.currentTimeMillis() - startTime);
        if (context.getExpectModelNum() != null) {
            Assert.assertEquals(context.getExpectModelNum().intValue(), smartCtx.getProposedModels().size());
        }

        buildAndCompare(context);

        startTime = System.currentTimeMillis();
        // 4. compare layout propose result and query cube result
        RecAndQueryCompareUtil.computeCompareRank(kylinConfig, getProject(), compareMap);
        // 5. check layout
        if (context.isCompareLayout()) {
            assertOrPrintCmpResult(
                    compareMap.entrySet().stream()
                            .filter(entry -> RecAndQueryCompareUtil.AccelerationMatchedLevel.FAILED_QUERY != entry
                                    .getValue().getLevel())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            log.debug("compare realization cost {} s", System.currentTimeMillis() - startTime);
        }

        // 6. summary info
        val rankInfoMap = RecAndQueryCompareUtil.summarizeRankInfo(compareMap);
        StringBuilder sb = new StringBuilder();
        sb.append("All used queries: ").append(compareMap.size()).append('\n');
        rankInfoMap.forEach((key, value) -> sb.append(key).append(": ").append(value).append("\n"));
        log.debug(sb.toString());
    }

    private void updateAccelerateInfoMap(AbstractContext context) {
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        val targetIndexPlanMap = context.getModelContexts().stream()
                .map(AbstractContext.ModelContext::getTargetIndexPlan)
                .collect(Collectors.toMap(RootPersistentEntity::getId, i -> i));
        val targetModelMap = context.getModelContexts().stream().map(AbstractContext.ModelContext::getTargetModel)
                .collect(Collectors.toMap(RootPersistentEntity::getId, i -> i));
        context.getAccelerateInfoMap().forEach((sql, info) -> {
            Set<AccelerateInfo.QueryLayoutRelation> relatedLayouts = info.getRelatedLayouts();
            relatedLayouts.forEach(r -> {
                val modelId = r.getModelId();
                val indexPlan = indexManager.getIndexPlan(modelId);
                val indexInSmart = targetIndexPlanMap.get(modelId);
                val modelInSmart = targetModelMap.get(modelId);
                r.setLayoutId(indexPlan
                        .getAllLayouts().stream().filter(l -> isMatch(l, indexInSmart.getLayoutEntity(r.getLayoutId()),
                                indexPlan.getModel(), modelInSmart))
                        .map(LayoutEntity::getId).findFirst().orElse(r.getLayoutId()));
            });
        });
    }

    private boolean isMatch(LayoutEntity real, LayoutEntity virtual, NDataModel realModel, NDataModel virtualModel) {
        val copy = JsonUtil.deepCopyQuietly(virtual, LayoutEntity.class);
        copy.setColOrder(translate(copy.getColOrder(), realModel, virtualModel));
        copy.setShardByColumns(translate(copy.getShardByColumns(), realModel, virtualModel));
        return real.equals(copy);
    }

    private List<Integer> translate(List<Integer> cols, NDataModel realModel, NDataModel virtualModel) {
        val realColsMap = realModel.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist)
                .collect(Collectors.toMap(NDataModel.NamedColumn::getAliasDotColumn, NDataModel.NamedColumn::getId));
        val realMeasureMap = realModel.getAllMeasures().stream().filter(m -> !m.isTomb())
                .collect(Collectors.toMap(MeasureDesc::getName, NDataModel.Measure::getId));
        val virtualColsMap = virtualModel.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist)
                .collect(Collectors.toMap(NDataModel.NamedColumn::getId, NDataModel.NamedColumn::getAliasDotColumn));
        val virtualMeasureMap = virtualModel.getAllMeasures().stream().filter(m -> !m.isTomb())
                .collect(Collectors.toMap(NDataModel.Measure::getId, MeasureDesc::getName));
        return cols.stream().map(i -> {
            if (i < NDataModel.MEASURE_ID_BASE) {
                return realColsMap.get(virtualColsMap.get(i));
            } else {
                return realMeasureMap.get(virtualMeasureMap.get(i));
            }
        }).collect(Collectors.toList());
    }

    public void buildAllModels(KylinConfig kylinConfig, String project) throws InterruptedException {
        buildAllModels(kylinConfig, project, new BuildAndCompareContext());
    }

    public void buildAllModels(KylinConfig kylinConfig, String project, BuildAndCompareContext context)
            throws InterruptedException {
        kylinConfig.clearManagers();
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);

        val extension = context.getExtension();
        List<IndexDataConstructor.BuildInfo> buildInfos = Lists.newArrayList();
        for (IRealization realization : projectManager.listAllRealizations(project)) {
            NDataflow df = (NDataflow) realization;
            if (extension != null) {
                extension.accept(df);
            }
            Segments<NDataSegment> readySegments = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
            NDataSegment oneSeg;
            Set<LayoutEntity> layouts;
            boolean isAppend = false;
            if (readySegments.isEmpty()) {
                oneSeg = UnitOfWork
                        .doInTransactionWithRetry(
                                () -> NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                                        .appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite()),
                                project);
                layouts = Sets.newHashSet(df.getIndexPlan().getAllLayouts());
                isAppend = true;
                readySegments.add(oneSeg);
            } else {
                oneSeg = readySegments.getFirstSegment();
                layouts = df.getIndexPlan().getAllLayouts().stream()
                        .filter(c -> !oneSeg.getLayoutsMap().containsKey(c.getId())).collect(Collectors.toSet());
            }
            if (!layouts.isEmpty()) {
                buildInfos.add(new IndexDataConstructor.BuildInfo(df.getId(), oneSeg, layouts, isAppend, null));
            }
        }
        indexDataConstructor.buildSegments(buildInfos);
    }

    protected Map<String, ExecAndCompExt.CompareEntity> collectCompareEntity(AbstractContext context) {
        Map<String, ExecAndCompExt.CompareEntity> map = Maps.newHashMap();
        final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        accelerateInfoMap.forEach((sql, accelerateInfo) -> {
            map.putIfAbsent(sql, new ExecAndCompExt.CompareEntity());
            final ExecAndCompExt.CompareEntity entity = map.get(sql);
            entity.setAccelerateInfo(accelerateInfo);
            entity.setAccelerateLayouts(RecAndQueryCompareUtil.writeQueryLayoutRelationAsString(kylinConfig,
                    getProject(), accelerateInfo.getRelatedLayouts()));
            entity.setSql(sql);
        });
        return map;
    }

    protected List<String> collectQueries(List<TestScenario> tests) throws IOException {
        List<String> allQueries = Lists.newArrayList();
        for (TestScenario test : tests) {
            List<Pair<String, String>> queries = fetchQueries(test.folderName, test.getFromIndex(), test.getToIndex());
            normalizeSql(test.joinType, queries);
            test.queries = test.getExclusionList() == null ? queries
                    : ExecAndComp.doFilter(queries, test.getExclusionList());
            allQueries.addAll(test.queries.stream().map(Pair::getSecond).collect(Collectors.toList()));
        }

        return allQueries;
    }

    protected List<Pair<String, String>> fetchQueries(String subFolder, int fromIndex, int toIndex) throws IOException {
        List<Pair<String, String>> queries;
        String folder = getFolder(subFolder);
        if (fromIndex == toIndex) {
            queries = ExecAndComp.fetchQueries(folder);
        } else {
            if (fromIndex > toIndex) {
                int tmp = fromIndex;
                fromIndex = toIndex;
                toIndex = tmp;
            }
            queries = ExecAndComp.fetchPartialQueries(folder, fromIndex, toIndex);
        }
        return queries;
    }

    private void normalizeSql(JoinType joinType, List<Pair<String, String>> queries) {
        queries.forEach(pair -> {
            String tmp = ExecAndComp.changeJoinType(pair.getSecond(), joinType.name());
            pair.setSecond(tmp);
        });
    }

    private void assertOrPrintCmpResult(Map<String, ExecAndCompExt.CompareEntity> compareMap) {
        // print details
        compareMap.forEach((key, value) -> {
            if (RecAndQueryCompareUtil.AccelerationMatchedLevel.FAILED_QUERY == value.getLevel()) {
                return;
            }
            log.debug("** start comparing the SQL: {} **", value.getFilePath());
            if (!excludedSqlPatterns.contains(key) && !value.ignoredCompareLevel()) {
                Assert.assertEquals(
                        "something unexpected happened when comparing result of sql: " + value.getFilePath(),
                        value.getAccelerateLayouts(), value.getQueryUsedLayouts());
            } else {
                log.info("{}\n", value);
            }
        });
    }

    protected Set<String> changeJoinType(String sql) {
        Set<String> patterns = Sets.newHashSet();
        for (JoinType joinType : JoinType.values()) {
            final String rst = ExecAndComp.changeJoinType(sql, joinType.name());
            patterns.add(rst);
        }

        return patterns;
    }

    protected byte[] getFileBytes(File whiteListFile) throws IOException {
        final long fileLength = whiteListFile.length();
        byte[] fileContent = new byte[(int) fileLength];
        try (FileInputStream inputStream = new FileInputStream(whiteListFile)) {
            final int read = inputStream.read(fileContent);
            Preconditions.checkState(read != -1);
        }
        return fileContent;
    }

    protected AbstractContext proposeWithSmartMaster(String project, List<TestScenario> testScenarios)
            throws IOException {
        List<String> sqlList = collectQueries(testScenarios);
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(sqlList));
        String[] sqls = sqlList.toArray(new String[0]);
        return AccelerationUtil.runWithSmartContext(getTestConfig(), project, sqls, true);
    }

    protected void buildAndCompare(TestScenario... testScenarios) throws Exception {
        buildAndCompare(BuildAndCompareContext.builder()//
                .testScenarios(Lists.newArrayList(testScenarios)) //
                .build());
    }

    public final void buildAndCompare(BuildAndCompareContext params) throws Exception {
        try {
            // 2. execute cube building
            long startTime = System.currentTimeMillis();
            buildAllModels(kylinConfig, getProject(), params);
            log.info("build models cost {} ms", System.currentTimeMillis() - startTime);

            // 3. validate results between SparkSQL and cube
            populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
            startTime = System.currentTimeMillis();
            val compareMap = params.getCompareMap();
            params.getTestScenarios().forEach(testScenario -> {
                populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
                if (testScenario.isLimit()) {
                    ExecAndCompExt.execLimitAndValidateNew(testScenario, getProject(), JoinType.DEFAULT.name(),
                            compareMap);
                } else if (testScenario.isDynamicSql()) {
                    ExecAndCompExt.execAndCompareDynamic(testScenario, getProject(), testScenario.getCompareLevel(),
                            testScenario.joinType.name(), compareMap);
                } else {
                    ExecAndCompExt.execAndCompare(testScenario, getProject(), testScenario.getCompareLevel(),
                            testScenario.joinType.name(), compareMap, null);
                }
            });
            Map<String, Set<String>> allFailedQueries = new HashMap<>();
            int allQuerySize = 0;
            for (TestScenario testScenario : params.getTestScenarios()) {
                ConcurrentHashMap<String, Set<String>> failedQueries = testScenario.getFailedQueries();
                allQuerySize += testScenario.getQueries().size();
                allFailedQueries.putAll(failedQueries);
            }
            if (!allFailedQueries.isEmpty()) {
                long failedCnt = allFailedQueries.values().stream().mapToLong(Set::size).sum();
                throw new IllegalStateException(String.format(Locale.ROOT,
                        "Total query count: %d, failed count: %d, failed queries as follows:%n%s", allQuerySize,
                        failedCnt, StringUtils.join(allFailedQueries, "\n")));
            }
            log.info("compare result cost {} ms", System.currentTimeMillis() - startTime);
        } finally {
            FileUtils.deleteQuietly(new File("../kylin-it/metastore_db"));
        }

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class BuildAndCompareContext {

        Integer expectModelNum;

        Map<String, ExecAndCompExt.CompareEntity> compareMap;

        @Singular
        List<TestScenario> testScenarios;

        Consumer<NDataflow> extension;

        boolean isCompareLayout = true;

        int storageType = 1;
    }
}
