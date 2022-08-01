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

package org.apache.kylin.metadata.recommendation.candidate;

import static org.apache.kylin.metadata.recommendation.candidate.RawRecItem.CostMethod.getCostMethod;
import static org.mybatis.dynamic.sql.SqlBuilder.count;
import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.isGreaterThanOrEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.isIn;
import static org.mybatis.dynamic.sql.SqlBuilder.isNotEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.isNotIn;
import static org.mybatis.dynamic.sql.SqlBuilder.max;
import static org.mybatis.dynamic.sql.SqlBuilder.min;
import static org.mybatis.dynamic.sql.SqlBuilder.select;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.schema.ImportModelContext;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem.CostMethod;
import org.apache.kylin.metadata.recommendation.util.RawRecStoreUtil;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.SqlBuilder;
import org.mybatis.dynamic.sql.delete.DeleteModel;
import org.mybatis.dynamic.sql.delete.render.DeleteStatementProvider;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.update.render.UpdateStatementProvider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.Getter;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcRawRecStore {

    public static final String RECOMMENDATION_CANDIDATE = "_rec_candidate";
    private static final int NON_EXIST_MODEL_SEMANTIC_VERSION = Integer.MIN_VALUE;
    private static final int LAG_SEMANTIC_VERSION = 1;

    private final RawRecItemTable table;
    @VisibleForTesting
    @Getter
    private final SqlSessionFactory sqlSessionFactory;

    public JdbcRawRecStore(KylinConfig config) throws Exception {
        this(config, genRawRecTableName(config));
    }

    public JdbcRawRecStore(KylinConfig config, String tableName) throws Exception {
        StorageURL url = config.getQueryHistoryUrl();
        Properties props = JdbcUtil.datasourceParameters(url);
        DataSource dataSource = JdbcDataSource.getDataSource(props);
        table = new RawRecItemTable(tableName);
        sqlSessionFactory = RawRecStoreUtil.getSqlSessionFactory(dataSource, table.tableNameAtRuntime());
    }

    private static String genRawRecTableName(KylinConfig config) {
        StorageURL url = config.getQueryHistoryUrl();
        String tablePrefix = config.isUTEnv() ? "test_opt" : url.getIdentifier();
        return tablePrefix + RECOMMENDATION_CANDIDATE;
    }

    public void save(RawRecItem recItem) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            InsertStatementProvider<RawRecItem> insertStatement = getInsertProvider(recItem, false);
            int rows = mapper.insert(insertStatement);
            session.commit();
            if (rows > 0) {
                log.debug("Insert one raw recommendation({}) into database.", recItem.getUniqueFlag());
            } else {
                log.debug("No raw recommendation has been inserted into database.");
            }
        }
    }

    public void save(List<RawRecItem> recItems, boolean reserveId) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            List<InsertStatementProvider<RawRecItem>> providers = Lists.newArrayList();
            recItems.forEach(recItem -> providers.add(getInsertProvider(recItem, reserveId)));
            providers.forEach(mapper::insert);
            session.commit();
            log.info("Insert {} raw recommendations into database takes {} ms", recItems.size(),
                    System.currentTimeMillis() - startTime);
        }
    }

    public RawRecItem queryById(int id) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = getSelectByIdStatementProvider(id);
            return mapper.selectOne(statementProvider);
        }
    }

    public RawRecItem queryByUniqueFlag(String project, String modelId, String uniqueFlag, Integer semanticVersion) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = getSelectUniqueFlagIdStatementProvider(project, modelId,
                    uniqueFlag, semanticVersion);
            return mapper.selectOne(statementProvider);
        }
    }

    public List<RawRecItem> queryAll() {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .build().render(RenderingStrategies.MYBATIS3);
            return mapper.selectMany(statementProvider);
        }
    }

    private List<RawRecItem> list(String project, String model, Integer semanticVersion, int limit) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            var statement = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.modelID, isEqualTo(model));
            if (semanticVersion != null) {
                statement = statement.and(table.semanticVersion, isEqualTo(semanticVersion)); //
            }
            List<RawRecItem> rawRecItems = mapper.selectMany(statement.limit(limit) //
                    .build().render(RenderingStrategies.MYBATIS3));
            log.info("List all raw recommendations of model({}/{}, semanticVersion: {}) takes {} ms.", //
                    project, model, semanticVersion, System.currentTimeMillis() - startTime);
            return rawRecItems;
        }
    }

    public List<RawRecItem> listAll(String project, String model, int semanticVersion, int limit) {
        return list(project, model, semanticVersion, limit);
    }

    public List<RawRecItem> listAll(String project, String model, int limit) {
        return list(project, model, null, limit);
    }

    public List<RawRecItem> queryAdditionalLayoutRecItems(String project, String model) {
        int semanticVersion = getSemanticVersion(project, model);
        if (semanticVersion == NON_EXIST_MODEL_SEMANTIC_VERSION) {
            log.debug("queryAdditionalLayoutRecItems - model({}/{}) does not exist.", project, model);
            return Lists.newArrayList();
        }
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.semanticVersion, isEqualTo(semanticVersion)) //
                    .and(table.modelID, isEqualTo(model)) //
                    .and(table.type, isEqualTo(RawRecItem.RawRecType.ADDITIONAL_LAYOUT)) //
                    .and(table.state, isEqualTo(RawRecItem.RawRecState.RECOMMENDED)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            List<RawRecItem> rawRecItems = mapper.selectMany(statementProvider);
            log.info("Query raw recommendations can add indexes to model({}/{}, semanticVersion: {}) takes {} ms.", //
                    project, model, semanticVersion, System.currentTimeMillis() - startTime);
            return rawRecItems;
        }
    }

    public List<RawRecItem> queryFrom(BasicColumn[] cols, int startId, int limit) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(cols) //
                    .from(table).where(table.id, isGreaterThanOrEqualTo(startId))//
                    .orderBy(table.id) //
                    .limit(limit).build().render(RenderingStrategies.MYBATIS3);
            return mapper.selectMany(statementProvider);
        }
    }

    public List<RawRecItem> chooseTopNCandidates(String project, String model, double minCost, int topN, int offset,
            RawRecItem.RawRecState state) {
        int semanticVersion = getSemanticVersion(project, model);
        if (semanticVersion == NON_EXIST_MODEL_SEMANTIC_VERSION) {
            log.debug("chooseTopNCandidates - model({}/{}) does not exist.", project, model);
            return Lists.newArrayList();
        }
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            var queryBuilder = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.semanticVersion, isEqualTo(semanticVersion)) //
                    .and(table.modelID, isEqualTo(model)) //
                    .and(table.type, isEqualTo(RawRecItem.RawRecType.ADDITIONAL_LAYOUT)) //
                    .and(table.state, isEqualTo(state)); //
            if (minCost > 0) {
                queryBuilder = queryBuilder.and(table.cost, isGreaterThanOrEqualTo(minCost));
            }

            val statementProvider = queryBuilder.and(table.recSource, isNotEqualTo(RawRecItem.IMPORTED)) //
                    .orderBy(table.cost.descending(), table.hitCount.descending(), table.id.descending()) //
                    .limit(topN).offset(offset) //
                    .build().render(RenderingStrategies.MYBATIS3);
            List<RawRecItem> rawRecItems = mapper.selectMany(statementProvider);
            log.info("Query topN({}) recommendations for adding to model({}/{}, semanticVersion: {}) takes {} ms.", //
                    topN, project, model, semanticVersion, System.currentTimeMillis() - startTime);
            return rawRecItems;
        }
    }

    public List<RawRecItem> chooseTopNCandidates(String project, String model, int topN, int offset,
            RawRecItem.RawRecState state) {
        return chooseTopNCandidates(project, model, -1, topN, offset, state);
    }

    public List<RawRecItem> queryImportedRawRecItems(String project, String model, RawRecItem.RawRecState state) {
        int semanticVersion = getSemanticVersion(project, model);
        if (semanticVersion == NON_EXIST_MODEL_SEMANTIC_VERSION) {
            log.debug("queryImportedRawRecItems - model({}/{}) does not exist.", project, model);
            return Lists.newArrayList();
        }
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.semanticVersion, isEqualTo(semanticVersion)) //
                    .and(table.modelID, isEqualTo(model)) //
                    .and(table.type, isEqualTo(RawRecItem.RawRecType.ADDITIONAL_LAYOUT)) //
                    .and(table.state, isEqualTo(state)) //
                    .and(table.recSource, isEqualTo(RawRecItem.IMPORTED)) //
                    .orderBy(table.id.descending()) //
                    .build().render(RenderingStrategies.MYBATIS3);
            List<RawRecItem> rawRecItems = mapper.selectMany(statementProvider);
            log.info("Query recommendations "
                    + "generated from imported sql for adding to model({}/{}, semanticVersion: {}) takes {} ms.", //
                    project, model, semanticVersion, System.currentTimeMillis() - startTime);
            return rawRecItems;
        }
    }

    public List<RawRecItem> queryNonAppliedLayoutRecItems(String project, String model, boolean isAdditionalRec) {
        int semanticVersion = getSemanticVersion(project, model);
        if (semanticVersion == NON_EXIST_MODEL_SEMANTIC_VERSION) {
            log.debug("queryNonAppliedLayoutRecItems - model({}/{}) does not exist.", project, model);
            return Lists.newArrayList();
        }
        RawRecItem.RawRecType type = isAdditionalRec //
                ? RawRecItem.RawRecType.ADDITIONAL_LAYOUT //
                : RawRecItem.RawRecType.REMOVAL_LAYOUT;
        long start = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.semanticVersion, isEqualTo(semanticVersion)) //
                    .and(table.modelID, isEqualTo(model)) //
                    .and(table.type, isEqualTo(type)) //
                    .and(table.state, isNotIn(RawRecItem.RawRecState.APPLIED, RawRecItem.RawRecState.BROKEN)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            List<RawRecItem> recItems = mapper.selectMany(statementProvider);
            log.info("Query raw recommendations of model({}/{}, semanticVersion: {}, type: {}) takes {} ms", //
                    project, model, semanticVersion, type.name(), System.currentTimeMillis() - start);
            return recItems;
        }
    }

    public List<RawRecItem> queryNonLayoutRecItems(String project, String model) {
        int semanticVersion = getSemanticVersion(project, model);
        if (semanticVersion == NON_EXIST_MODEL_SEMANTIC_VERSION) {
            log.debug("queryNonLayoutRecItems - model({}/{}) does not exist.", project, model);
            return Lists.newArrayList();
        }
        long start = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.semanticVersion, isEqualTo(semanticVersion)) //
                    .and(table.modelID, isEqualTo(model)) //
                    .and(table.state, isNotEqualTo(RawRecItem.RawRecState.BROKEN)) //
                    .and(table.type,
                            isNotIn(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, RawRecItem.RawRecType.REMOVAL_LAYOUT)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            List<RawRecItem> recItems = mapper.selectMany(statementProvider);
            log.info("Query non-index raw recommendations of model({}/{}, semanticVersion: {}) takes {} ms", //
                    project, model, semanticVersion, System.currentTimeMillis() - start);
            return recItems;
        }
    }

    public List<RawRecItem> queryNonLayoutRecItems(String project) {
        long start = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.project, isEqualTo(project)) //
                    .and(table.state, isNotEqualTo(RawRecItem.RawRecState.BROKEN)) //
                    .and(table.type,
                            isNotIn(RawRecItem.RawRecType.ADDITIONAL_LAYOUT, RawRecItem.RawRecType.REMOVAL_LAYOUT)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            List<RawRecItem> recItems = mapper.selectMany(statementProvider);
            log.info("Query non-index raw recommendations of project({}) takes {} ms", //
                    project, System.currentTimeMillis() - start);
            return recItems;
        }
    }

    private int getSemanticVersion(String project, String model) {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel dataModel = modelManager.getDataModelDesc(model);
        if (dataModel == null || dataModel.isBroken()) {
            return NON_EXIST_MODEL_SEMANTIC_VERSION;
        }
        return dataModel.getSemanticVersion();
    }

    public void updateState(List<Integer> idList, RawRecItem.RawRecState state) {
        long start = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            List<UpdateStatementProvider> providers = Lists.newArrayList();
            long updateTime = System.currentTimeMillis();
            idList.forEach(id -> providers.add(changeRecStateProvider(id, state, updateTime)));
            providers.forEach(mapper::update);
            session.commit();
            log.info("Update {} raw recommendation(s) to state({}) takes {} ms", idList.size(), state.name(),
                    System.currentTimeMillis() - start);
        }
    }

    public void update(RawRecItem recItem) {
        // no need to update type and create_time
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            UpdateStatementProvider updateStatement = getUpdateProvider(recItem);
            mapper.update(updateStatement);
            session.commit();
            log.debug("Update one raw recommendation({})", recItem.getUniqueFlag());
        }
    }

    public void update(List<RawRecItem> recItems) {
        if (recItems == null || recItems.isEmpty()) {
            log.info("No raw recommendations need to update.");
            return;
        }

        long start = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            List<UpdateStatementProvider> providers = Lists.newArrayList();
            recItems.forEach(item -> providers.add(getUpdateProvider(item)));
            providers.forEach(mapper::update);
            session.commit();
            log.info("Update {} raw recommendation(s) takes {} ms", recItems.size(),
                    System.currentTimeMillis() - start);
        }
    }

    public void batchAddOrUpdate(List<RawRecItem> recItems) {
        if (recItems == null || recItems.isEmpty()) {
            log.info("No raw recommendations need to add or update.");
            return;
        }

        List<RawRecItem> recItemsToAdd = Lists.newArrayList();
        List<RawRecItem> recItemsToUpdate = Lists.newArrayList();
        recItems.forEach(recItem -> {
            if (recItem.getId() == 0) {
                recItemsToAdd.add(recItem);
            } else {
                recItemsToUpdate.add(recItem);
            }
        });
        if (!recItemsToAdd.isEmpty()) {
            save(recItemsToAdd, false);
        }
        if (!recItemsToUpdate.isEmpty()) {
            update(recItemsToUpdate);
        }
    }

    public void importRecommendations(String project, String modelId, List<RawRecItem> rawRecItems) {
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            val manager = RawRecManager.getInstance(project);
            val baseId = manager.getMaxId() + 100;

            Map<Integer, Integer> idChangedMap = new HashMap<>();

            val index = new AtomicInteger(0);

            rawRecItems = rawRecItems.stream().map(rawRecItem -> {
                rawRecItem.setProject(project);
                rawRecItem.setModelID(modelId);

                String uniqueFlag = rawRecItem.getUniqueFlag();
                RawRecItem originalRawRecItem = manager.getRawRecItemByUniqueFlag(rawRecItem.getProject(),
                        rawRecItem.getModelID(), uniqueFlag, rawRecItem.getSemanticVersion());
                int newId;
                if (originalRawRecItem != null) {
                    newId = originalRawRecItem.getId();
                } else {
                    newId = index.incrementAndGet() + baseId;
                }
                idChangedMap.put(-rawRecItem.getId(), -newId);

                rawRecItem.setId(newId);

                return rawRecItem;
            }).collect(Collectors.toList());

            ImportModelContext.reorderRecommendations(rawRecItems, idChangedMap);

            val mapper = session.getMapper(RawRecItemMapper.class);
            List<UpdateStatementProvider> updaters = Lists.newArrayList();
            List<InsertStatementProvider<RawRecItem>> inserts = Lists.newArrayList();
            rawRecItems.forEach(item -> {
                if (queryByUniqueFlag(item.getProject(), item.getModelID(), item.getUniqueFlag(),
                        item.getSemanticVersion()) != null) {
                    updaters.add(getUpdateProvider(item));
                } else {
                    inserts.add(getInsertProvider(item, true));
                }
            });
            updaters.forEach(mapper::update);
            inserts.forEach(mapper::insert);
            session.commit();
        }
    }

    private void delete(String project) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            DeleteModel deleteStatement;
            if (project != null) {
                deleteStatement = SqlBuilder.deleteFrom(table).where(table.project, isEqualTo(project)).build();
            } else {
                deleteStatement = SqlBuilder.deleteFrom(table).build();
            }
            int rows = mapper.delete(deleteStatement.render(RenderingStrategies.MYBATIS3));
            session.commit();
            log.info("Delete {} row(s) raw recommendation takes {} ms.", rows, System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            if (project != null) {
                log.error("Fail to delete raw recommendations for deleted project({}).", project, e);
            } else {
                log.error("Fail to delete raw recommendations.", e);
            }

        }
    }

    public void deleteByProject(String project) {
        delete(project);
    }

    public void deleteAll() {
        delete(null);
    }

    public void cleanForDeletedProject(List<String> projectList) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(table)//
                    .where(table.project, isNotIn(projectList)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int rows = mapper.delete(deleteStatement);
            session.commit();
            log.info("Delete {} row(s) residual raw recommendation takes {} ms", rows,
                    System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            log.error("Fail to clean raw recommendations for deleted projects({})", projectList.toString(), e);
        }
    }

    public void deleteById(List<Integer> ids) {
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(table)//
                    .where(table.id, SqlBuilder.isIn(ids)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int deleteSize = mapper.delete(deleteStatement);
            session.commit();
            log.info("Delete {} row(s) raw recommendation takes {} ms", deleteSize,
                    System.currentTimeMillis() - startTime);
        }
    }

    public void discardRecItemsOfBrokenModel(String model) {
        long startTime = System.currentTimeMillis();
        RawRecItem.RawRecType[] rawRecTypes = { RawRecItem.RawRecType.ADDITIONAL_LAYOUT,
                RawRecItem.RawRecType.REMOVAL_LAYOUT };
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            UpdateStatementProvider updateProvider = SqlBuilder.update(table)//
                    .set(table.updateTime).equalTo(startTime)//
                    .set(table.state).equalTo(RawRecItem.RawRecState.DISCARD)//
                    .where(table.modelID, isEqualTo(model)) //
                    .and(table.type, isIn(rawRecTypes)) //
                    .build().render(RenderingStrategies.MYBATIS3);
            int rows = mapper.update(updateProvider);
            session.commit();
            log.info("Discard {} row(s) raw recommendation of broken model takes {} ms", rows,
                    System.currentTimeMillis() - startTime);
        }
    }

    public int getRecItemCountByProject(String project, RawRecItem.RawRecType type) {
        SelectStatementProvider statementProvider = select(count(table.id)) //
                .from(table) //
                .where(table.project, isEqualTo(project)) //
                .and(table.type, isEqualTo(type)) //
                .build().render(RenderingStrategies.MYBATIS3);
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            return mapper.selectAsInt(statementProvider);
        }
    }

    public Set<String> updateAllCost(String project) {
        final int batchToUpdate = 1000;
        long currentTime = System.currentTimeMillis();
        int effectiveDays = Integer.parseInt(FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getValue(FavoriteRule.EFFECTIVE_DAYS));
        CostMethod costMethod = getCostMethod(project);
        Set<String> updateModels = Sets.newHashSet();
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            // if no records, no need to update cost
            if (mapper.selectAsInt(getContStarProvider()) == 0) {
                return Sets.newHashSet();
            }

            int totalUpdated = 0;
            for (int i = 0;; i++) {
                List<RawRecItem> rawRecItems = mapper.selectMany(getSelectLayoutProvider(project, batchToUpdate,
                        batchToUpdate * i, RawRecItem.RawRecState.INITIAL, RawRecItem.RawRecState.RECOMMENDED));
                int size = rawRecItems.size();
                updateCost(effectiveDays, costMethod, currentTime, session, mapper, rawRecItems);
                rawRecItems.forEach(item -> updateModels.add(item.getModelID()));
                totalUpdated += size;
                if (size < batchToUpdate) {
                    break;
                }
            }
            log.info("Update the cost of all {} raw recommendation takes {} ms", totalUpdated,
                    System.currentTimeMillis() - currentTime);
            return updateModels;
        }
    }

    public List<RawRecItem> list(Collection<Integer> rawRecIds) {
        if (rawRecIds.isEmpty()) {
            return Collections.emptyList();
        }
        long startTime = System.currentTimeMillis();
        try (SqlSession session = sqlSessionFactory.openSession()) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            var statement = select(getSelectFields(table)) //
                    .from(table) //
                    .where(table.id, isIn(rawRecIds));

            List<RawRecItem> rawRecItems = mapper.selectMany(statement.build().render(RenderingStrategies.MYBATIS3));
            log.info("List all raw recommendations id in {}) takes {} ms.", //
                    rawRecIds.stream().map(String::valueOf).collect(Collectors.joining(", ")),
                    System.currentTimeMillis() - startTime);
            return rawRecItems;
        }
    }

    private void updateCost(int effectiveDays, CostMethod costMethod, long currentTime, SqlSession session,
            RawRecItemMapper mapper, List<RawRecItem> oneBatch) {
        if (oneBatch.isEmpty()) {
            return;
        }

        oneBatch.forEach(recItem -> {
            if (RawRecItem.IMPORTED.equalsIgnoreCase(recItem.getRecSource())) {
                recItem.setUpdateTime(currentTime);
                return;
            }
            recItem.updateCost(costMethod, currentTime, effectiveDays);
            recItem.setUpdateTime(currentTime);
        });
        List<UpdateStatementProvider> providers = Lists.newArrayList();
        oneBatch.forEach(item -> providers.add(getUpdateProvider(item)));
        providers.forEach(mapper::update);
        session.commit();
    }

    private SelectStatementProvider getSelectLayoutProvider(String project, int limit, int offset,
            RawRecItem.RawRecState... states) {
        return select(getSelectFields(table)) //
                .from(table).where(table.project, isEqualTo(project)) //
                .and(table.type, isEqualTo(RawRecItem.RawRecType.ADDITIONAL_LAYOUT)) //
                .and(table.state, isIn(states)) //
                .limit(limit).offset(offset) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    SelectStatementProvider getMinIdProvider() {
        return select(min(table.id)).from(table).build().render(RenderingStrategies.MYBATIS3);
    }

    SelectStatementProvider getMaxIdProvider() {
        return select(max(table.id)).from(table).build().render(RenderingStrategies.MYBATIS3);
    }

    SelectStatementProvider getContStarProvider() {
        return select(count(table.id)).from(table).build().render(RenderingStrategies.MYBATIS3);
    }

    public SelectStatementProvider getSelectByIdStatementProvider(int id) {
        return select(getSelectFields(table)) //
                .from(table) //
                .where(table.id, isEqualTo(id)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    SelectStatementProvider getSelectUniqueFlagIdStatementProvider(String project, String modelId, String uniqueFlag,
            Integer semanticVersion) {
        return select(getSelectFields(table)) //
                .from(table) //
                .where(table.uniqueFlag, isEqualTo(uniqueFlag)) //
                .and(table.project, isEqualTo(project)) //
                .and(table.modelID, isEqualTo(modelId)) //
                .and(table.semanticVersion, isEqualTo(semanticVersion)).build().render(RenderingStrategies.MYBATIS3);
    }

    public InsertStatementProvider<RawRecItem> getInsertProvider(RawRecItem recItem, boolean reserveId) {
        var provider = SqlBuilder.insert(recItem).into(table);
        if (reserveId) {
            provider = provider.map(table.id).toProperty("id");
        }
        return provider.map(table.project).toProperty("project") //
                .map(table.modelID).toProperty("modelID") //
                .map(table.uniqueFlag).toProperty("uniqueFlag") //
                .map(table.semanticVersion).toProperty("semanticVersion") //
                .map(table.type).toProperty("type") //
                .map(table.recEntity).toProperty("recEntity") //
                .map(table.state).toProperty("state") //
                .map(table.createTime).toProperty("createTime") //
                .map(table.updateTime).toProperty("updateTime") //
                .map(table.dependIDs).toPropertyWhenPresent("dependIDs", recItem::getDependIDs) //
                // only for layout raw recommendation
                .map(table.layoutMetric).toPropertyWhenPresent("layoutMetric", recItem::getLayoutMetric) //
                .map(table.cost).toPropertyWhenPresent("cost", recItem::getCost) //
                .map(table.totalLatencyOfLastDay)
                .toPropertyWhenPresent("totalLatencyOfLastDay", recItem::getTotalLatencyOfLastDay) //
                .map(table.hitCount).toPropertyWhenPresent("hitCount", recItem::getHitCount) //
                .map(table.totalTime).toPropertyWhenPresent("totalTime", recItem::getMaxTime) //
                .map(table.maxTime).toPropertyWhenPresent("maxTime", recItem::getMinTime) //
                .map(table.minTime).toPropertyWhenPresent("minTime", recItem::getMinTime) //
                .map(table.queryHistoryInfo).toPropertyWhenPresent("queryHistoryInfo", recItem::getQueryHistoryInfo) //
                .map(table.recSource).toPropertyWhenPresent("recSource", recItem::getRecSource).build()
                .render(RenderingStrategies.MYBATIS3);
    }

    UpdateStatementProvider changeRecStateProvider(int id, RawRecItem.RawRecState state, long updateTime) {
        return SqlBuilder.update(table) //
                .set(table.state).equalTo(state) //
                .set(table.updateTime).equalTo(updateTime) //
                .where(table.id, isEqualTo(id)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    UpdateStatementProvider getUpdateProvider(RawRecItem recItem) {
        return SqlBuilder.update(table)//
                .set(table.uniqueFlag).equalTo(recItem::getUniqueFlag) //
                .set(table.semanticVersion).equalTo(recItem::getSemanticVersion) //
                .set(table.state).equalTo(recItem::getState) //
                .set(table.updateTime).equalTo(recItem::getUpdateTime) //
                .set(table.recEntity).equalTo(recItem::getRecEntity) //
                .set(table.dependIDs).equalToWhenPresent(recItem::getDependIDs) //
                // only for layout raw recommendation
                .set(table.layoutMetric).equalToWhenPresent(recItem::getLayoutMetric) //
                .set(table.cost).equalToWhenPresent(recItem::getCost) //
                .set(table.totalLatencyOfLastDay).equalToWhenPresent(recItem::getTotalLatencyOfLastDay) //
                .set(table.hitCount).equalToWhenPresent(recItem::getHitCount) //
                .set(table.totalTime).equalToWhenPresent(recItem::getTotalTime) //
                .set(table.maxTime).equalToWhenPresent(recItem::getMaxTime) //
                .set(table.minTime).equalToWhenPresent(recItem::getMinTime) //
                .set(table.queryHistoryInfo).equalToWhenPresent(recItem::getQueryHistoryInfo) //
                .set(table.recSource).equalTo(recItem::getRecSource) //
                .where(table.id, isEqualTo(recItem::getId)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    private BasicColumn[] getSelectFields(RawRecItemTable recItemTable) {
        return BasicColumn.columnList(//
                recItemTable.id, //
                recItemTable.project, //
                recItemTable.modelID, //
                recItemTable.uniqueFlag, //
                recItemTable.semanticVersion, //
                recItemTable.type, //
                recItemTable.recEntity, //
                recItemTable.dependIDs, //
                recItemTable.state, //
                recItemTable.createTime, //
                recItemTable.updateTime, //

                // only for layout
                recItemTable.layoutMetric, //
                recItemTable.cost, //
                recItemTable.totalLatencyOfLastDay, //
                recItemTable.hitCount, //
                recItemTable.totalTime, //
                recItemTable.maxTime, //
                recItemTable.minTime, //
                recItemTable.queryHistoryInfo, //
                recItemTable.recSource);
    }

    public int getMaxId() {
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            if (mapper.selectAsInt(getContStarProvider()) == 0) {
                return 0;
            }
            return mapper.selectAsInt(getMaxIdProvider());
        }
    }

    public int getMinId() {
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            RawRecItemMapper mapper = session.getMapper(RawRecItemMapper.class);
            if (mapper.selectAsInt(getContStarProvider()) == 0) {
                return 0;
            }
            return mapper.selectAsInt(getMinIdProvider());
        }
    }

    public void deleteOutdated() {
        int batch = 1000;
        int maxId = getMaxId();
        int currentId = getMinId() - 1;
        List<Integer> outDatedItems = Lists.newArrayList();
        val cols = BasicColumn.columnList(//
                table.id, table.project, table.modelID, table.semanticVersion);

        while (currentId < maxId) {
            // queryFrom just select id, project, modelId, semanticVersion
            List<RawRecItem> items = queryFrom(cols, currentId, batch);
            if (CollectionUtils.isEmpty(items)) {
                break;
            }
            for (RawRecItem item : items) {
                currentId = Math.max(currentId, item.getId());
                if (outDated(item)) {
                    outDatedItems.add(item.getId());
                    if (outDatedItems.size() == batch) {
                        deleteById(outDatedItems);
                        outDatedItems = Lists.newArrayList();
                    }
                }
            }
        }
        if (!outDatedItems.isEmpty()) {
            deleteById(outDatedItems);
        }
    }

    private boolean outDated(RawRecItem item) {
        val project = item.getProject();
        val modelId = item.getModelID();
        if (NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project) == null) {
            return true;
        }
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataModel = modelManager.getDataModelDesc(modelId);
        if (dataModel == null) {
            return true;
        }
        if (dataModel.isBroken()) {
            return false;
        }

        return item.getSemanticVersion() < dataModel.getSemanticVersion() - LAG_SEMANTIC_VERSION;
    }
}
