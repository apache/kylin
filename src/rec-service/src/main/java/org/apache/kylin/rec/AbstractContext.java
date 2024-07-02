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

package org.apache.kylin.rec;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.model.AntiFlatChecker;
import org.apache.kylin.metadata.model.ColExcludedChecker;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.entity.CCRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.DimensionRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.LayoutRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.MeasureRecItemV2;
import org.apache.kylin.query.IQueryTransformer;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.query.util.SqlNodeExtractor;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.common.SmartConfig;
import org.apache.kylin.rec.exception.PendingException;
import org.apache.kylin.rec.index.IndexSuggester;
import org.apache.kylin.rec.model.ModelTree;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public abstract class AbstractContext {

    private final SmartConfig smartConfig;
    private final String project;
    private final String[] sqlArray;
    private final ChainedProposer proposers;
    private final ExtraMetaInfo extraMeta = new ExtraMetaInfo();

    private final List<NDataModel> relatedModels = Lists.newArrayList();
    private final Set<String> relatedTables = Sets.newHashSet();

    @Setter
    private boolean isRestoredProposeContext;

    @Setter
    protected boolean canCreateNewModel;

    @Setter
    private List<ModelContext> modelContexts = Lists.newArrayList();
    @Setter
    private Map<String, AccelerateInfo> accelerateInfoMap = Maps.newHashMap();
    @Getter(lazy = true)
    private final Map<String, RawRecItem> existingNonLayoutRecItemMap = Maps.newHashMap();
    private final Map<String, Collection<OlapContext>> modelViewOlapContextMap = Maps.newHashMap();

    @Setter
    private boolean skipEvaluateCC;
    protected boolean partialMatch;
    protected boolean partialMatchNonEqui;
    @Setter
    protected String modelName;

    protected AbstractContext(KylinConfig kylinConfig, String project, String[] sqlArray) {
        this.smartConfig = SmartConfig.wrap(kylinConfig);
        this.project = project;
        this.sqlArray = sqlArray;
        this.proposers = createProposers();
        this.partialMatch = false;
        this.partialMatchNonEqui = false;
        filterSqlRelatedModelsAndTables();
    }

    protected AbstractContext(KylinConfig kylinConfig, String project, String[] sqlArray, String modelName) {
        this(kylinConfig, project, sqlArray);
        this.modelName = modelName;
    }

    public ModelContext createModelContext(ModelTree modelTree) {
        return new ModelContext(this, modelTree);
    }

    public abstract IndexPlan getOriginIndexPlan(String modelId);

    public abstract List<NDataModel> getOriginModels();

    public abstract void changeModelMainType(NDataModel model);

    public abstract ChainedProposer createProposers();

    public abstract void saveMetadata();

    public abstract String getIdentifier();

    public KapConfig getKapConfig() {
        return getSmartConfig().getKapConfig();
    }

    private void filterSqlRelatedModelsAndTables() {
        Set<NDataModel> models = Sets.newHashSet();
        Set<String> tableIdentities = Sets.newHashSet();
        Map<String, Set<NDataModel>> tableToModelsMap = Maps.newHashMap();
        Map<String, NDataModel> modelViewToModelMap = Maps.newHashMap();
        getAllModels().forEach(model -> {
            if (model.isBroken()) {
                return;
            }
            modelViewToModelMap.put((model.getProject() + "." + model.getAlias()).toUpperCase(Locale.ROOT), model);
            for (TableRef tableRef : model.getAllTables()) {
                tableToModelsMap.putIfAbsent(tableRef.getTableIdentity(), Sets.newHashSet());
                tableToModelsMap.get(tableRef.getTableIdentity()).add(model);
            }
        });

        Map<String, Set<String>> allTableMap = getProjectTableMap();
        if (!smartConfig.skipUselessMetadata() || isRestoredProposeContext) {
            tableToModelsMap.forEach((k, modelSet) -> getRelatedModels().addAll(modelSet));
            allTableMap.forEach((k, tableSet) -> getRelatedTables().addAll(tableSet));
            return;
        }

        // related tables from sql + related tables from baseModels
        Preconditions.checkNotNull(sqlArray);
        for (String sql : sqlArray) {
            List<SqlIdentifier> sqlIdentifiers = extractSqlIdentifier(sql);
            Set<String> sqlRelatedTableIdentities = extractTable(sqlIdentifiers, allTableMap);
            Set<NDataModel> sqlRelatedViewModels = extractViewModel(sqlIdentifiers, modelViewToModelMap);
            models.addAll(sqlRelatedViewModels);
            tableIdentities.addAll(extractViewModelTable(sqlRelatedViewModels));
            tableIdentities.addAll(sqlRelatedTableIdentities);
            sqlRelatedTableIdentities.forEach(tableIdentity -> {
                Set<NDataModel> rModels = tableToModelsMap.getOrDefault(tableIdentity, Sets.newHashSet());
                rModels.forEach(model -> {
                    Set<TableRef> allTables = model.getAllTables();
                    allTables.forEach(tableRef -> tableIdentities.add(tableRef.getTableIdentity()));
                });
                models.addAll(rModels);
            });
        }
        getRelatedModels().addAll(models);
        getRelatedTables().addAll(tableIdentities);
    }

    private Map<String, Set<String>> getProjectTableMap() {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(smartConfig.getKylinConfig(), project);
        List<TableDesc> tableList = tableMgr.listAllTables();
        Map<String, Set<String>> tableNameMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        tableList.forEach(table -> {
            tableNameMap.putIfAbsent(table.getName(), Sets.newHashSet());
            tableNameMap.putIfAbsent(table.getIdentity(), Sets.newHashSet());
            tableNameMap.get(table.getName()).add(table.getIdentity());
            tableNameMap.get(table.getIdentity()).add(table.getIdentity());
        });
        return tableNameMap;
    }

    private Set<String> extractTable(List<SqlIdentifier> sqlIdentifiers, Map<String, Set<String>> tableNameMap) {
        return sqlIdentifiers.stream().map(id -> tableNameMap.getOrDefault(id.toString(), Sets.newHashSet()))
                .flatMap(Collection::stream).collect(Collectors.toSet());
    }

    private Set<NDataModel> extractViewModel(List<SqlIdentifier> sqlIdentifiers,
            Map<String, NDataModel> modelViewToModelMap) {
        return sqlIdentifiers.stream().filter(id -> modelViewToModelMap.containsKey(id.toString()))
                .map(id -> modelViewToModelMap.get(id.toString())).collect(Collectors.toSet());
    }

    private Set<String> extractViewModelTable(Set<NDataModel> sqlRelatedViewModels) {
        return sqlRelatedViewModels.stream()
                .map(model -> model.getAllTables().stream().map(TableRef::getTableIdentity).collect(Collectors.toSet()))
                .flatMap(Collection::stream).collect(Collectors.toSet());
    }

    private List<SqlIdentifier> extractSqlIdentifier(String sql) {
        try {
            String normalizedSql = normalizeForTableDetecting(project, sql);
            return SqlNodeExtractor.getAllSqlIdentifier(normalizedSql);
        } catch (SqlParseException e) {
            log.info("extract error, sql is: {}. Error message is: {}", sql, e.getMessage());
            AccelerateInfo accelerateInfo = new AccelerateInfo();
            accelerateInfo.setFailedCause(e);
            accelerateInfoMap.put(sql, accelerateInfo);
            return Lists.newArrayList();
        }
    }

    public static String normalizeForTableDetecting(String project, String sql) {
        KylinConfig kylinConfig = NProjectManager.getProjectConfig(project);
        String convertedSql = QueryUtil.appendLimitOffset(project, sql, 0, 0);
        String defaultSchema = "DEFAULT";
        try {
            QueryExec queryExec = new QueryExec(project, kylinConfig);
            defaultSchema = queryExec.getDefaultSchemaName();
        } catch (Exception e) {
            log.error("Get project default schema failed: {}", e.getMessage());
        }

        String[] detectorTransformers = kylinConfig.getTableDetectorTransformers();
        List<IQueryTransformer> transformerList = QueryUtil.fetchTransformers(false, detectorTransformers);
        if (log.isDebugEnabled()) {
            log.debug("All used transformers of detecting used tables are: {}", transformerList.stream()
                    .map(clz -> clz.getClass().getCanonicalName()).collect(Collectors.joining(",")));
        }
        for (IQueryTransformer t : transformerList) {
            convertedSql = t.transform(convertedSql, project, defaultSchema);
        }
        return convertedSql;
    }

    protected List<NDataModel> getAllModels() {
        return NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).listAllModels();
    }

    public void recordException(ModelContext modelCtx, Exception e) {
        modelCtx.getModelTree().getOlapContexts().forEach(olapCtx -> {
            String sql = olapCtx.getSql();
            final AccelerateInfo accelerateInfo = accelerateInfoMap.get(sql);
            Preconditions.checkNotNull(accelerateInfo);
            accelerateInfo.setFailedCause(e);
        });
    }

    public boolean skipCollectRecommendations() {
        return !(this instanceof ModelReuseContext);
    }

    public void handleExceptionAfterModelSelect() {
        // default do nothing
    }

    public List<NDataModel> getProposedModels() {
        if (CollectionUtils.isEmpty(modelContexts)) {
            return Lists.newArrayList();
        }

        List<NDataModel> models = Lists.newArrayList();
        for (ModelContext modelContext : modelContexts) {
            NDataModel model = modelContext.getTargetModel();
            if (model == null) {
                continue;
            }

            models.add(modelContext.getTargetModel());
        }

        return models;
    }

    public NDataModel getProposedModel(String modelId) {
        return getProposedModels().stream().filter(m -> Objects.equals(m.getId(), modelId)).findAny().orElse(null);
    }

    @Getter
    public static class ModelContext {
        @Setter
        private ModelTree modelTree; // query

        @Setter
        private NDataModel targetModel; // output model
        @Setter
        private NDataModel originModel; // used when update existing models

        @Setter
        private IndexPlan targetIndexPlan;
        @Setter
        private IndexPlan originIndexPlan;

        @Setter
        private Map<String, CCRecItemV2> ccRecItemMap = Maps.newHashMap();
        @Setter
        private Map<String, DimensionRecItemV2> dimensionRecItemMap = Maps.newHashMap();
        @Setter
        private Map<String, MeasureRecItemV2> measureRecItemMap = Maps.newHashMap();
        @Setter
        private Map<String, LayoutRecItemV2> indexRexItemMap = Maps.newHashMap();

        @Setter
        private boolean snapshotSelected;

        private final AbstractContext proposeContext;
        private final Map<String, ComputedColumnDesc> usedCC = Maps.newHashMap();
        @Setter
        private boolean needUpdateCC = false;
        @Getter(lazy = true)
        private final Map<String, String> uniqueContentToFlag = loadUniqueContentToFlag();
        @Setter
        private AntiFlatChecker antiFlatChecker;
        @Setter
        private ColExcludedChecker excludedChecker;

        private Map<String, String> loadUniqueContentToFlag() {
            Map<String, String> result = Maps.newHashMap();
            if (!(getProposeContext() instanceof AbstractSemiContext) || getTargetModel() == null) {
                return result;
            }

            String modelId = getTargetModel().getUuid();
            getProposeContext().getExistingNonLayoutRecItemMap().forEach((uniqueFlag, item) -> {
                if (item.getModelID().equalsIgnoreCase(modelId)) {
                    result.put(item.getRecEntity().getUniqueContent(), uniqueFlag);
                }
            });
            return result;
        }

        public ModelContext(AbstractContext proposeContext, ModelTree modelTree) {
            this.proposeContext = proposeContext;
            this.modelTree = modelTree;
        }

        public boolean isTargetModelMissing() {
            return targetModel == null;
        }

        public boolean isProposedIndexesEmpty() {
            // we can not modify rule_based_indexes
            return targetIndexPlan == null || CollectionUtils.isEmpty(targetIndexPlan.getIndexes());
        }

        public boolean skipSavingMetadata() {
            return isTargetModelMissing() || isProposedIndexesEmpty() || snapshotSelected;
        }

        /**
         * Only for Semi-Auto
         */
        public void gatherLayoutRecItem(LayoutEntity layout) {
            if (getProposeContext().skipCollectRecommendations()) {
                return;
            }
            LayoutRecItemV2 item = new LayoutRecItemV2();
            item.setLayout(layout);
            item.setCreateTime(System.currentTimeMillis());
            item.setAgg(layout.getId() < IndexEntity.TABLE_INDEX_START_ID);
            item.setUuid(RandomUtil.randomUUIDStr());
            getIndexRexItemMap().put(layout.genUniqueContent(), item);
        }

        public Map<String, TblColRef> collectFkDimensionMap(OlapContext olap) {
            Map<String, TblColRef> fKAsDimensionMap = Maps.newHashMap();
            Set<String> allAntiFlattenLookups = antiFlatChecker.getAntiFlattenLookups();
            Set<TableRef> pkTableRefs = olap.getAllColumns().stream() //
                    .filter(tblColRef -> excludedChecker.isExcludedCol(tblColRef)) //
                    .map(TblColRef::getTableRef) //
                    .collect(Collectors.toSet());
            boolean hasDerivedCols = excludedChecker.anyExcludedColMatch(olap.getAllColumns());
            olap.getJoins().forEach(join -> {
                for (int i = 0; i < join.getForeignKeyColumns().length; i++) {
                    TblColRef fk = join.getForeignKeyColumns()[i];
                    TblColRef pk = join.getPrimaryKeyColumns()[i];
                    if (!allAntiFlattenLookups.contains(pk.getTableWithSchema()) && !hasDerivedCols) {
                        continue;
                    }
                    if (allAntiFlattenLookups.contains(fk.getTableWithSchema())) {
                        throw new PendingException(IndexSuggester.FK_ON_ANTI_FLATTEN_LOOKUP + fk.getIdentity());
                    } else if (excludedChecker.isExcludedCol(fk)) {
                        throw new PendingException(IndexSuggester.FK_OF_EXCLUDED_COLUMNS + fk.getIdentity());
                    } else if (!proposeContext.getSmartConfig().includeAllFks()) {
                        if (pkTableRefs.contains(pk.getTableRef())) {
                            fKAsDimensionMap.put(fk.getCanonicalName(), fk);
                        }
                    } else {
                        fKAsDimensionMap.putIfAbsent(fk.getCanonicalName(), fk);
                    }
                }
            });
            log.debug("Collect some foreign keys as dimensions when proposing. {}", fKAsDimensionMap.keySet());
            return fKAsDimensionMap;
        }
    }

    @Getter
    @Setter
    public static class ExtraMetaInfo {

        private String modelOptRule;
        private Set<String> allModels = Sets.newHashSet();
        private Set<String> onlineModelIds = Sets.newHashSet();
    }
}
