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
package org.apache.kylin.common.persistence.metadata;

import static org.apache.kylin.common.exception.CommonErrorCode.FAILED_UPDATE_METADATA;
import static org.apache.kylin.common.persistence.metadata.MetadataMapperFactory.convertConditionsToDSLCompleter;
import static org.apache.kylin.common.persistence.metadata.MetadataMapperFactory.resetMapperTableNameIfNeed;
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.withTransaction;
import static org.apache.kylin.common.persistence.metadata.mapper.BasicSqlTable.CONTENT_FIELD;
import static org.apache.kylin.common.persistence.metadata.mapper.BasicSqlTable.META_KEY_PROPERTIES_NAME;
import static org.apache.kylin.common.persistence.metadata.mapper.BasicSqlTable.MVCC_FIELD;
import static org.apache.kylin.common.persistence.metadata.mapper.BasicSqlTable.UUID_FIELD;
import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.RawResourceCompressProxy;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.VersionedRawResource;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.BindableColumn;
import org.mybatis.dynamic.sql.SqlColumn;
import org.mybatis.dynamic.sql.util.mybatis3.MyBatis3Utils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.security.util.InMemoryResource;

import lombok.Getter;
import lombok.val;
import lombok.var;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcMetadataStore extends MetadataStore {

    @VisibleForTesting
    @Getter
    private final SqlSessionFactory sqlSessionFactory;

    @VisibleForTesting
    @Getter
    protected final DataSourceTransactionManager transactionManager;
    @Getter
    private final JdbcTemplate jdbcTemplate;
    private final boolean isUT;

    @Delegate
    private final JdbcTransactionHelper helper;

    public JdbcMetadataStore(KylinConfig config) throws Exception {
        super(config);
        val url = config.getMetadataUrl();
        val props = JdbcUtil.datasourceParameters(url);
        val dataSource = JdbcDataSource.getDataSource(props);
        transactionManager = JdbcDataSource.getTransactionManager(dataSource);
        jdbcTemplate = new JdbcTemplate(dataSource);
        String table = url.getIdentifier();
        isUT = config.isUTEnv();
        auditLogStore = new JdbcAuditLogStore(config, jdbcTemplate, transactionManager,
                table + JdbcAuditLogStore.AUDIT_LOG_SUFFIX);
        sqlSessionFactory = MetadataMapperFactory.getSqlSessionFactory(dataSource);
        createIfNotExist(table);
        if (isUT) {
            resetMapperTableNameIfNeed(url, sqlSessionFactory);
        }
        helper = new JdbcTransactionHelper(transactionManager);
    }

    @Override
    public <T extends RawResource> List<T> get(MetadataType type, RawResourceFilter filter, boolean needLock,
            boolean needContent) {
        return get(type, filter, needLock, needContent, null);
    }

    /**
     * Get metadata by RawResourceFilter
     * @param type The metadata type
     * @param filter The Condition includes the operator and left and right variables used for filtering
     * @param needLock Whether to lock the metadata in record lock mode
     * @param needContent Whether to get the content field of the metadata
     * @param provideSelections The fields to be selected. If no provided, should be null
     * @return The list of RawResource
     */
    @VisibleForTesting
    protected <T extends RawResource> List<T> get(MetadataType type, RawResourceFilter filter, boolean needLock,
            boolean needContent, List<String> provideSelections) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            val mapper = MetadataMapperFactory.<T> createFor(type, session);
            BasicColumn[] selectList = mapper.getSelectList();
            if (provideSelections != null && !provideSelections.isEmpty()) {
                Map<String, BasicColumn> selectColumnMap = mapper.getSelectColumnMap();
                if (needContent && !provideSelections.contains(CONTENT_FIELD)) {
                    provideSelections.add(CONTENT_FIELD);
                }
                selectList = provideSelections.stream().map(selectColumnMap::get).filter(Objects::nonNull)
                        .toArray(BasicColumn[]::new);
            }

            if (!needContent) {
                List<BasicColumn> resultList = new ArrayList<>();
                for (val column : selectList) {
                    if (CONTENT_FIELD.equals(((SqlColumn<Object>) column).name())) {
                        continue;
                    }
                    resultList.add(column);
                }
                selectList = resultList.toArray(new BasicColumn[0]);
            }

            val dsl = convertConditionsToDSLCompleter(filter, mapper.getSelectColumnMap());
            return needLock
                    ? MyBatis3Utils.selectList(mapper::selectManyWithRecordLock, selectList, mapper.getSqlTable(), dsl)
                    : MyBatis3Utils.selectList(mapper::selectMany, selectList, mapper.getSqlTable(), dsl);
        }
    }

    /**
     * Common method for save/update/delete metadata
     * @param type The metadata type
     * @param rawResource The metadata
     * @return The number of affected rows
     */
    @Override
    public int save(MetadataType type, final RawResource rawResource) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            val mapper = MetadataMapperFactory.createFor(type, session);
            int affectedRow;
            long mvcc = rawResource.getMvcc();
            BindableColumn<String> metaKeyCol = (BindableColumn<String>) mapper.getSqlColumn(META_KEY_PROPERTIES_NAME);
            BindableColumn<Long> mvccCol = (BindableColumn<Long>) mapper.getSqlColumn(MVCC_FIELD);
            Optional<RawResource> record;
            if (rawResource.getContent() != null) {
                RawResource proxy = RawResourceCompressProxy.createProxy(rawResource);
                record = mapper
                        .selectOneWithColumnsAndRecordLock(
                                c -> c.where(metaKeyCol, isEqualTo(proxy::getMetaKey)).and(mvccCol,
                                        isEqualTo(proxy.getMvcc() - 1)),
                                new BasicColumn[] { mapper.getSqlColumn(UUID_FIELD) });
                if (record.isPresent()) {
                    affectedRow = mapper.updateByPrimaryKeyAndMvcc(proxy);
                } else {
                    assert isUT || mvcc == 0;
                    affectedRow = mapper.insertOne(proxy);
                }
            } else {
                record = mapper.selectOneWithColumnsAndRecordLock(
                        c -> c.where(metaKeyCol, isEqualTo(rawResource::getMetaKey)),
                        new BasicColumn[] { mapper.getSqlColumn(UUID_FIELD) });
                if (!record.isPresent()) {
                    return -1;
                }
                affectedRow = mapper.delete(c -> c.where(metaKeyCol, isEqualTo(rawResource::getMetaKey)));
            }

            if (affectedRow == 0) {
                throw new KylinException(FAILED_UPDATE_METADATA, String.format(Locale.ROOT,
                        "Failed to update or insert meta key: %s, mvcc: %s", rawResource.getMetaKey(), mvcc));
            }
            return affectedRow;
        }
    }

    @Override
    public NavigableSet<String> listAll() {
        throw new NotImplementedException("JdbcMetadataStore doesn't support listAll.");
    }

    @Override
    public void dump(ResourceStore store) {
        withTransaction(transactionManager, () -> {
            val resources = store.listResourcesRecursively(MetadataType.ALL.name());
            if (resources == null || resources.isEmpty()) {
                log.info("there is no resources in rootPath ({}),please check the rootPath.", MetadataType.ALL.name());
                return null;
            }
            for (String resPath : resources) {
                val raw = store.getResource(resPath);
                save(raw.getMetaType(), raw);
            }
            return null;
        });
    }

    @Override
    public void uploadFromFile(File folder) {
        withTransaction(transactionManager, () -> {
            super.uploadFromFile(folder);
            return null;
        });
    }

    /**
     * @throws Exception
     */
    @VisibleForTesting
    public void createIfNotExist(String table) throws Exception {
        String fileName = "metadata-jdbc-default.properties";
        if (((BasicDataSource) jdbcTemplate.getDataSource()).getDriverClassName().equals("org.postgresql.Driver")) {
            fileName = "metadata-jdbc-postgresql.properties";
        } else if (((BasicDataSource) jdbcTemplate.getDataSource()).getDriverClassName()
                .equals("com.mysql.jdbc.Driver")) {
            fileName = "metadata-jdbc-mysql.properties";
        } else if (((BasicDataSource) jdbcTemplate.getDataSource()).getDriverClassName().equals("org.h2.Driver")) {
            fileName = "metadata-jdbc-h2.properties";
        }
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        Properties properties = new Properties();
        properties.load(is);
        var sql = properties.getProperty("create.metadata.store.table");
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        sql = sql.replace("%s_", table + "_");
        populator.addScript(new InMemoryResource(sql));
        populator.setContinueOnError(false);
        DatabasePopulatorUtils.execute(populator, jdbcTemplate.getDataSource());
        log.info("Succeed to create table. Prefix is: {}", table + "_");
    }

    @Override
    public MemoryMetaData reloadAll() {
        val data = MemoryMetaData.createEmpty();
        return withTransaction(transactionManager, () -> {
            log.info("Start jdbc reloadAll");
            data.getData().keySet().parallelStream().forEach(type -> {
                // _REC is only appeared in the file based metadata store, skip it
                if (type != MetadataType.TMP_REC) {
                    // The needLock parameter set to false, because we use the mysql snapshot read
                    val rawResourceMap = data.getData().get(type);
                    get(type, new RawResourceFilter(), false, true)
                            .forEach(res -> rawResourceMap.put(res.getMetaKey(), new VersionedRawResource(res)));
                }
            });

            long offset = getAuditLogStore().getMaxId();
            log.info("end reloadAll offset is {}", offset);
            data.setOffset(offset);
            return data;
        });
    }
}
