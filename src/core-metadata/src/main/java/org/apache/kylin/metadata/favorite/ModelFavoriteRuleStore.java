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

package org.apache.kylin.metadata.favorite;

import static org.apache.kylin.common.exception.CommonErrorCode.FAILED_UPDATE_METADATA;
import static org.mybatis.dynamic.sql.SqlBuilder.deleteFrom;
import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.select;

import java.util.List;
import java.util.Locale;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.util.MetadataStoreUtil;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.SqlBuilder;
import org.mybatis.dynamic.sql.delete.render.DeleteStatementProvider;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.update.render.UpdateStatementProvider;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import lombok.Getter;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelFavoriteRuleStore {
    public static final String MODEL_FAVORITE_RULE = "_model_favorite_rule";

    private final ModelFavoriteRuleTable table;
    @Getter
    private final SqlSessionTemplate sqlSessionTemplate;
    @Getter
    private final DataSourceTransactionManager transactionManager;

    // todo ModelFavoriteRule backup&restore tool required
    public ModelFavoriteRuleStore(KylinConfig config) throws Exception {
        this(config, genTableName(config));
    }

    private ModelFavoriteRuleStore(KylinConfig config, String tableName) throws Exception {
        StorageURL url = config.getCoreMetadataDBUrl();
        Properties props = JdbcUtil.datasourceParameters(url);
        DataSource dataSource = JdbcDataSource.getDataSource(props);
        table = new ModelFavoriteRuleTable(tableName);
        transactionManager = JdbcDataSource.getTransactionManager(dataSource);
        sqlSessionTemplate = new SqlSessionTemplate(MetadataStoreUtil.getSqlSessionFactory(dataSource,
                table.tableNameAtRuntime(), MetadataStoreUtil.TableType.MODEL_FAVORITE_RULE));
    }

    private static String genTableName(KylinConfig config) {
        StorageURL url = config.getCoreMetadataDBUrl();
        String tablePrefix = config.isUTEnv() ? "test_opt" : url.getIdentifier();
        return tablePrefix + MODEL_FAVORITE_RULE;
    }

    public void save(FavoriteRule rule) {
        ModelFavoriteRuleMapper mapper = sqlSessionTemplate.getMapper(ModelFavoriteRuleMapper.class);
        InsertStatementProvider<FavoriteRule> insertStatement = getInsertProvider(rule);
        int rows = mapper.insert(insertStatement);
        if (rows > 0) {
            log.debug("Insert one favorite rule for model ({}) into database.", rule.getModel());
        } else {
            throw new KylinException(FAILED_UPDATE_METADATA,
                    String.format(Locale.ROOT, "Failed to insert favorite rule for model (%s)", rule.getModel()));
        }
    }

    public void update(FavoriteRule rule) {
        ModelFavoriteRuleMapper mapper = sqlSessionTemplate.getMapper(ModelFavoriteRuleMapper.class);
        UpdateStatementProvider updateStatement = getUpdateProvider(rule);
        int rows = mapper.update(updateStatement);
        if (rows > 0) {
            log.debug("Update one favorite rule for model ({})", rule.getModel());
        } else {
            throw new KylinException(FAILED_UPDATE_METADATA,
                    String.format(Locale.ROOT, "Failed to update favorite rule for model (%s)", rule.getModel()));
        }
    }

    public List<FavoriteRule> queryByProject(String project) {
        ModelFavoriteRuleMapper mapper = sqlSessionTemplate.getMapper(ModelFavoriteRuleMapper.class);
        SelectStatementProvider statementProvider = getSelectByProjectStatementProvider(project);
        return mapper.selectMany(statementProvider);
    }

    public List<FavoriteRule> queryByModel(String model) {
        ModelFavoriteRuleMapper mapper = sqlSessionTemplate.getMapper(ModelFavoriteRuleMapper.class);
        SelectStatementProvider statementProvider = getSelectByModelStatementProvider(model);
        return mapper.selectMany(statementProvider);
    }

    public FavoriteRule queryByName(String model, String name) {
        ModelFavoriteRuleMapper mapper = sqlSessionTemplate.getMapper(ModelFavoriteRuleMapper.class);
        SelectStatementProvider statementProvider = getSelectByNameStatementProvider(model, name);
        return mapper.selectOne(statementProvider);
    }

    public List<FavoriteRule> queryAll() {
        ModelFavoriteRuleMapper mapper = sqlSessionTemplate.getMapper(ModelFavoriteRuleMapper.class);
        SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                .from(table) //
                .build().render(RenderingStrategies.MYBATIS3);
        return mapper.selectMany(statementProvider);
    }

    public void deleteByModel(String model) {
        ModelFavoriteRuleMapper mapper = sqlSessionTemplate.getMapper(ModelFavoriteRuleMapper.class);
        DeleteStatementProvider deleteStatement = deleteFrom(table)//
                .where(table.model, isEqualTo(model)) //
                .build().render(RenderingStrategies.MYBATIS3);
        mapper.delete(deleteStatement);
        log.info("Delete favorite rule for model ({})", model);
    }

    public void deleteByProject(String project) {
        ModelFavoriteRuleMapper mapper = sqlSessionTemplate.getMapper(ModelFavoriteRuleMapper.class);
        DeleteStatementProvider deleteStatement = deleteFrom(table)//
                .where(table.project, isEqualTo(project)) //
                .build().render(RenderingStrategies.MYBATIS3);
        mapper.delete(deleteStatement);
        log.info("Delete model favorite rule for project ({})", project);
    }

    public void deleteByName(String model, String name) {
        ModelFavoriteRuleMapper mapper = sqlSessionTemplate.getMapper(ModelFavoriteRuleMapper.class);
        DeleteStatementProvider deleteStatement = deleteFrom(table)//
                .where(table.model, isEqualTo(model)) //
                .and(table.name, isEqualTo(name)) //
                .build().render(RenderingStrategies.MYBATIS3);
        mapper.delete(deleteStatement);
        log.info("Delete favorite rule for model ({})", model);
    }

    InsertStatementProvider<FavoriteRule> getInsertProvider(FavoriteRule rule) {
        var provider = SqlBuilder.insert(rule).into(table);
        return provider.map(table.model).toProperty("model") //
                .map(table.project).toProperty("project") //
                .map(table.conds).toProperty("conds") //
                .map(table.name).toProperty("name") //
                .map(table.enabled).toProperty("enabled") //
                .map(table.updateTime).toProperty("updateTime") //
                .map(table.createTime).toProperty("createTime") //
                .map(table.mvcc).toProperty("mvcc") //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    UpdateStatementProvider getUpdateProvider(FavoriteRule rule) {

        return SqlBuilder.update(table) //
                .set(table.conds).equalTo(rule::getConds) //
                .set(table.updateTime).equalTo(rule::getUpdateTime) //
                .set(table.enabled).equalTo(rule::isEnabled) //
                .set(table.mvcc).equalTo(rule.getMvcc() + 1) //
                .where(table.id, isEqualTo(rule::getId)) //
                .and(table.mvcc, isEqualTo(rule::getMvcc)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    SelectStatementProvider getSelectByModelStatementProvider(String model) {
        return select(getSelectFields(table)) //
                .from(table) //
                .where(table.model, isEqualTo(model)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    SelectStatementProvider getSelectByProjectStatementProvider(String project) {
        return select(getSelectFields(table)) //
                .from(table) //
                .where(table.project, isEqualTo(project)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    SelectStatementProvider getSelectByNameStatementProvider(String model, String name) {
        return select(getSelectFields(table)) //
                .from(table) //
                .where(table.model, isEqualTo(model)) //
                .and(table.name, isEqualTo(name)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    private BasicColumn[] getSelectFields(ModelFavoriteRuleTable ruleTable) {
        return BasicColumn.columnList(//
                ruleTable.id, //
                ruleTable.project, //
                ruleTable.model, //
                ruleTable.conds, //
                ruleTable.name, //
                ruleTable.enabled, //
                ruleTable.updateTime, //
                ruleTable.createTime, //
                ruleTable.mvcc);
    }
}
