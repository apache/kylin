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

import static org.apache.kylin.common.persistence.metadata.JdbcMetadataStore.SELECT_TERM;
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.isIndexExists;
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.isTableExists;
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.withTransaction;

import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.AuditLog;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.event.ResourceDeleteEvent;
import org.apache.kylin.common.persistence.metadata.jdbc.AuditLogRowMapper;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.persistence.transaction.AbstractAuditLogReplayWorker;
import org.apache.kylin.common.persistence.transaction.AuditLogReplayWorker;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.CompressionUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.transaction.TransactionDefinition;

import com.google.common.base.Joiner;

import io.kyligence.kap.guava20.shaded.common.annotations.VisibleForTesting;
import io.kyligence.kap.guava20.shaded.common.base.Strings;
import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import lombok.Getter;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcAuditLogStore implements AuditLogStore {

    static final String AUDIT_LOG_SUFFIX = "_audit_log";

    static final String AUDIT_LOG_TABLE_ID = "id";
    static final String AUDIT_LOG_TABLE_KEY = "meta_key";
    static final String AUDIT_LOG_TABLE_CONTENT = "meta_content";
    static final String AUDIT_LOG_TABLE_TS = "meta_ts";
    static final String AUDIT_LOG_TABLE_MVCC = "meta_mvcc";
    static final String AUDIT_LOG_TABLE_UNIT = "unit_id";
    static final String AUDIT_LOG_TABLE_OPERATOR = "operator";
    static final String AUDIT_LOG_TABLE_INSTANCE = "instance";
    static final String CREATE_TABLE = "create.auditlog.store.table";
    static final String META_KEY_META_MVCC_INDEX = "%s_meta_key_meta_mvcc_index";
    static final String META_KEY_META_MVCC_INDEX_KEY = "create.auditlog.store.tableindex.meta_key_meta_mvcc_index";

    static final String INSERT_SQL = "insert into %s ("
            + Joiner.on(",").join(AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT, AUDIT_LOG_TABLE_TS,
                    AUDIT_LOG_TABLE_MVCC, AUDIT_LOG_TABLE_UNIT, AUDIT_LOG_TABLE_OPERATOR, AUDIT_LOG_TABLE_INSTANCE)
            + ") values (?, ?, ?, ?, ?, ?, ?)";
    static final String SELECT_BY_RANGE_SQL = SELECT_TERM
            + Joiner.on(",").join(AUDIT_LOG_TABLE_ID, AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT, AUDIT_LOG_TABLE_TS,
                    AUDIT_LOG_TABLE_MVCC, AUDIT_LOG_TABLE_UNIT, AUDIT_LOG_TABLE_OPERATOR, AUDIT_LOG_TABLE_INSTANCE)
            + " from %s where id > %d and id <= %d order by id";
    static final String SELECT_BY_ID_SQL = SELECT_TERM
            + Joiner.on(",").join(AUDIT_LOG_TABLE_ID, AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT, AUDIT_LOG_TABLE_TS,
                    AUDIT_LOG_TABLE_MVCC, AUDIT_LOG_TABLE_UNIT, AUDIT_LOG_TABLE_OPERATOR, AUDIT_LOG_TABLE_INSTANCE)
            + " from %s where id in(%s) order by id";

    static final String SELECT_BY_PROJECT_RANGE_SQL = SELECT_TERM
            + Joiner.on(",").join(AUDIT_LOG_TABLE_ID, AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT, AUDIT_LOG_TABLE_TS,
                    AUDIT_LOG_TABLE_MVCC, AUDIT_LOG_TABLE_UNIT, AUDIT_LOG_TABLE_OPERATOR, AUDIT_LOG_TABLE_INSTANCE)
            + " from %s where meta_key like '/%s/%%' and id > %d and id <= %d order by id";

    static final String SELECT_MAX_ID_BY_PROJECT_SQL = "select max(id) from %s where id > %d and meta_key like '/%s/%%'";
    static final String SELECT_MAX_ID_SQL = "select max(id) from %s";
    static final String SELECT_MIN_ID_SQL = "select min(id) from %s";
    static final String SELECT_COUNT_ID_RANGE = "select count(id) from %s where id > %d and id <= %d";
    static final String DELETE_ID_LESSTHAN_SQL = "delete from %s where id < ?";
    static final String SELECT_TS_RANGE = SELECT_TERM
            + Joiner.on(",").join(AUDIT_LOG_TABLE_ID, AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT, AUDIT_LOG_TABLE_TS,
                    AUDIT_LOG_TABLE_MVCC, AUDIT_LOG_TABLE_UNIT, AUDIT_LOG_TABLE_OPERATOR, AUDIT_LOG_TABLE_INSTANCE)
            + " from %s where id < %d and meta_ts between %d and %d order by id desc limit %d";

    static final String SELECT_BY_META_KET_AND_MVCC = SELECT_TERM
            + Joiner.on(",").join(AUDIT_LOG_TABLE_ID, AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT, AUDIT_LOG_TABLE_TS,
                    AUDIT_LOG_TABLE_MVCC, AUDIT_LOG_TABLE_UNIT, AUDIT_LOG_TABLE_OPERATOR, AUDIT_LOG_TABLE_INSTANCE)
            + " from %s where meta_key = '%s' and meta_mvcc = %s";

    private final KylinConfig config;
    @Getter
    private final JdbcTemplate jdbcTemplate;
    @Getter
    private final String table;

    protected final AbstractAuditLogReplayWorker replayWorker;

    private String instance;
    @Getter
    private final DataSourceTransactionManager transactionManager;

    public JdbcAuditLogStore(KylinConfig config) throws Exception {
        this(config, -1);
    }

    public JdbcAuditLogStore(KylinConfig config, int timeout) throws Exception {
        this.config = config;
        val url = config.getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = JdbcDataSource.getDataSource(props);
        transactionManager = new DataSourceTransactionManager(dataSource);
        jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.setQueryTimeout(timeout);
        table = url.getIdentifier() + AUDIT_LOG_SUFFIX;

        instance = AddressUtil.getLocalInstance();
        createIfNotExist();
        replayWorker = new AuditLogReplayWorker(config, this);
    }

    public JdbcAuditLogStore(KylinConfig config, JdbcTemplate jdbcTemplate,
            DataSourceTransactionManager transactionManager, String table) throws Exception {
        this.config = config;
        this.jdbcTemplate = jdbcTemplate;
        this.transactionManager = transactionManager;
        this.table = table;
        instance = AddressUtil.getLocalInstance();

        createIfNotExist();
        replayWorker = new AuditLogReplayWorker(config, this);

    }

    public void save(UnitMessages unitMessages) {
        val unitId = unitMessages.getUnitId();
        val operator = Optional.ofNullable(SecurityContextHolder.getContext().getAuthentication())
                .map(Principal::getName).orElse(null);

        JdbcUtil.Callback<Object> beforeCommit = null;
        if (config.isUnitOfWorkSimulationEnabled() && UnitOfWork.isAlreadyInTransaction()
                && UnitOfWork.get().getSleepMills() > 0) {
            beforeCommit = () -> {
                long sleepMills = UnitOfWork.get().getSleepMills();
                log.debug("audit log sleep {} ", sleepMills);
                try {
                    TimeUnit.MILLISECONDS.sleep(sleepMills);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                return null;
            };
        }

        withTransaction(transactionManager,
                () -> jdbcTemplate.batchUpdate(String.format(Locale.ROOT, INSERT_SQL, table),
                        unitMessages.getMessages().stream().map(e -> {
                            if (e instanceof ResourceCreateOrUpdateEvent) {
                                ResourceCreateOrUpdateEvent createEvent = (ResourceCreateOrUpdateEvent) e;
                                try {
                                    return new Object[] { createEvent.getResPath(),
                                            CompressionUtils
                                                    .compress(createEvent.getCreatedOrUpdated().getByteSource().read()),
                                            createEvent.getCreatedOrUpdated().getTimestamp(),
                                            createEvent.getCreatedOrUpdated().getMvcc(), unitId, operator, instance };
                                } catch (IOException ignore) {
                                    return null;
                                }
                            } else if (e instanceof ResourceDeleteEvent) {
                                ResourceDeleteEvent deleteEvent = (ResourceDeleteEvent) e;
                                return new Object[] { deleteEvent.getResPath(), null, System.currentTimeMillis(), null,
                                        unitId, operator, instance };
                            }
                            return null;
                        }).filter(Objects::nonNull).collect(Collectors.toList())),
                TransactionDefinition.ISOLATION_REPEATABLE_READ, beforeCommit, TransactionDefinition.TIMEOUT_DEFAULT);
    }

    public void batchInsert(List<AuditLog> auditLogs) {
        withTransaction(transactionManager, () -> jdbcTemplate
                .batchUpdate(String.format(Locale.ROOT, INSERT_SQL, table), auditLogs.stream().map(x -> {
                    try {
                        val bs = Objects.isNull(x.getByteSource()) ? null : x.getByteSource().read();
                        return new Object[] { x.getResPath(), CompressionUtils.compress(bs), x.getTimestamp(),
                                x.getMvcc(), x.getUnitId(), x.getOperator(), x.getInstance() };
                    } catch (IOException e) {
                        return null;
                    }
                }).filter(Objects::nonNull).collect(Collectors.toList())));
    }

    public List<AuditLog> fetch(long currentId, long size) {
        log.trace("fetch log from {} < id <= {}", currentId, currentId + size);
        return jdbcTemplate.query(String.format(Locale.ROOT, SELECT_BY_RANGE_SQL, table, currentId, currentId + size),
                new AuditLogRowMapper());
    }

    public List<AuditLog> fetch(List<Long> auditIdList) {
        if (CollectionUtils.isEmpty(auditIdList)) {
            return Lists.newArrayList();
        }
        log.trace("fetch log from {} =< id <= {},{}", auditIdList.get(0), auditIdList.get(auditIdList.size() - 1),
                auditIdList.size());
        val sqlIn = auditIdList.stream().map(String::valueOf).collect(Collectors.joining(","));
        return jdbcTemplate.query(String.format(Locale.ROOT, SELECT_BY_ID_SQL, table, sqlIn), new AuditLogRowMapper());
    }

    public List<AuditLog> fetch(String project, long currentId, long size) {
        log.trace("fetch log from {} < id <= {}", currentId, currentId + size);
        return jdbcTemplate.query(
                String.format(Locale.ROOT, SELECT_BY_PROJECT_RANGE_SQL, table, project, currentId, currentId + size),
                new AuditLogRowMapper());
    }

    public List<AuditLog> fetchRange(long fromId, long start, long end, int limit) {
        log.trace("Fetch log from {} meta_ts between {} and {}, fromId: {}.", table, start, end, fromId);
        return jdbcTemplate.query(String.format(Locale.ROOT, SELECT_TS_RANGE, table, fromId, start, end, limit),
                new AuditLogRowMapper());
    }

    @Override
    public long getMaxId() {
        return Optional
                .ofNullable(
                        jdbcTemplate.queryForObject(String.format(Locale.ROOT, SELECT_MAX_ID_SQL, table), Long.class))
                .orElse(0L);
    }

    public long getMaxIdByProject(String project, long from) {
        return Optional
                .ofNullable(jdbcTemplate.queryForObject(
                        String.format(Locale.ROOT, SELECT_MAX_ID_BY_PROJECT_SQL, table, from, project), Long.class))
                .orElse(0L);
    }

    @Override
    public long getMinId() {
        return Optional
                .ofNullable(
                        jdbcTemplate.queryForObject(String.format(Locale.ROOT, SELECT_MIN_ID_SQL, table), Long.class))
                .orElse(0L);
    }

    public long count(long startId, long endId) {
        return jdbcTemplate.queryForObject(String.format(Locale.ROOT, SELECT_COUNT_ID_RANGE, table, startId, endId),
                Long.class);
    }

    @Override
    public void restore(long currentId) {
        if (config.isJobNode() && !config.isUTEnv()) {
            log.info("current maxId is {}", currentId);
            replayWorker.startSchedule(currentId, false);
            return;
        }
        // query node need wait update to latest due to restore from backup
        replayWorker.startSchedule(currentId, true);
    }

    @Override
    public void catchupWithTimeout() throws Exception {
        val store = ResourceStore.getKylinMetaStore(config);
        replayWorker.catchupFrom(store.getOffset());
        replayWorker.waitForCatchup(getMaxId(), config.getCatchUpTimeout());
    }

    public void catchupWithMaxTimeout() throws Exception {
        val store = ResourceStore.getKylinMetaStore(config);
        replayWorker.catchupFrom(store.getOffset());
        replayWorker.waitForCatchup(getMaxId(), config.getCatchUpMaxTimeout());
    }

    @Override
    public void catchup() {
        val store = ResourceStore.getKylinMetaStore(config);
        replayWorker.catchupFrom(store.getOffset());
    }

    @Override
    public void setInstance(String instance) {
        this.instance = instance;
    }

    @Override
    public AuditLog get(String resPath, long mvcc) {
        return withTransaction(transactionManager, () -> {
            val result = jdbcTemplate.query(
                    String.format(Locale.ROOT, SELECT_BY_META_KET_AND_MVCC, table, resPath, mvcc),
                    new AuditLogRowMapper());
            if (!result.isEmpty()) {
                return result.get(0);
            }
            return null;
        });
    }

    @Override
    public void pause() {
        replayWorker.close(true);
    }

    @Override
    public void reInit() {
        val store = ResourceStore.getKylinMetaStore(config);
        replayWorker.reStartSchedule(store.getOffset());
    }

    @Override
    public void rotate() {
        withTransaction(transactionManager, () -> {
            val retainMaxSize = config.getMetadataAuditLogMaxSize();
            val currentMaxId = getMaxId();
            val deletableMaxId = currentMaxId - retainMaxSize + 1;
            log.info("try to delete audit_logs which id < {}", deletableMaxId);
            log.info("retainMaxSize: {}, currentMaxId: {}", retainMaxSize, currentMaxId);
            val startTime = System.currentTimeMillis();
            val update = jdbcTemplate.update(String.format(Locale.ROOT, DELETE_ID_LESSTHAN_SQL, table), deletableMaxId);
            log.info("delete audit_logs count: {}, cost: {}ms", update, System.currentTimeMillis() - startTime);
            return null;
        });
    }

    private Properties loadMedataProperties() throws IOException {
        String fileName = "metadata-jdbc-default.properties";
        if (((BasicDataSource) jdbcTemplate.getDataSource()).getDriverClassName().equals("org.postgresql.Driver")) {
            fileName = "metadata-jdbc-postgresql.properties";
        } else if (((BasicDataSource) jdbcTemplate.getDataSource()).getDriverClassName()
                .equals("com.mysql.jdbc.Driver")) {
            fileName = "metadata-jdbc-mysql.properties";
        }
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        Properties properties = new Properties();
        properties.load(is);
        return properties;
    }

    void createTableIfNotExist() throws SQLException, IOException {
        if (isTableExists(jdbcTemplate.getDataSource().getConnection(), table)) {
            return;
        }
        Properties properties = loadMedataProperties();
        var sql = properties.getProperty(CREATE_TABLE);

        jdbcTemplate.execute(String.format(Locale.ROOT, sql, table, AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT,
                AUDIT_LOG_TABLE_TS, AUDIT_LOG_TABLE_MVCC));
        log.info("Succeed to create table: {}", table);
    }

    void createIndexIfNotExist() {
        try {
            String indexName = String.format(Locale.ROOT, META_KEY_META_MVCC_INDEX, table);
            if (isIndexExists(jdbcTemplate.getDataSource().getConnection(), table, indexName)) {
                return;
            }
            Properties properties = loadMedataProperties();
            var sql = properties.getProperty(META_KEY_META_MVCC_INDEX_KEY);

            if (Strings.isNullOrEmpty(sql)) {
                return;
            }
            jdbcTemplate.execute(String.format(Locale.ROOT, sql, indexName, table));
            log.info("Succeed to create table {} index: {}", table, indexName);
        } catch (Exception e) {
            log.warn("Failed create index on table {}", table, e);
        }
    }

    void createIfNotExist() throws Exception {
        createTableIfNotExist();
        createIndexIfNotExist();
    }

    @Override
    public void close() throws IOException {
        replayWorker.close(false);
    }

    @VisibleForTesting
    public void forceClose() {
        replayWorker.close(true);
    }

    public long getLogOffset() {
        return replayWorker.getLogOffset();
    }

}
