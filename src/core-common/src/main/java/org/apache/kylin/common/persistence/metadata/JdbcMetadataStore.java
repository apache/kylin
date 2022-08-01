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
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.isTableExists;
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.withTransaction;
import static org.apache.kylin.common.persistence.transaction.UnitOfWork.GLOBAL_UNIT;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.Locale;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.VersionedRawResource;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.persistence.metadata.jdbc.RawResourceRowMapper;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.Getter;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcMetadataStore extends MetadataStore {

    private static final RowMapper<RawResource> RAW_RESOURCE_ROW_MAPPER = new RawResourceRowMapper();

    static final String META_TABLE_KEY = "META_TABLE_KEY";
    static final String META_TABLE_CONTENT = "META_TABLE_CONTENT";
    static final String META_TABLE_TS = "META_TABLE_TS";
    static final String META_TABLE_MVCC = "META_TABLE_MVCC";

    private static final String SELECT_TERM = "select ";

    private static final String SELECT_ALL_KEY_SQL = SELECT_TERM + META_TABLE_KEY + " from %s";
    private static final String SELECT_BY_RANGE_SQL = SELECT_TERM
            + Joiner.on(",").join(META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC)
            + " from %s where " + META_TABLE_KEY + " > '%s' and " + META_TABLE_KEY + " < '%s'";
    private static final String SELECT_BY_KEY_MVCC_SQL = SELECT_TERM
            + Joiner.on(",").join(META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC)
            + " from %s where " + META_TABLE_KEY + "='%s' and " + META_TABLE_MVCC + "=%d";
    private static final String SELECT_BY_KEY_SQL = SELECT_TERM
            + Joiner.on(",").join(META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC)
            + " from %s where " + META_TABLE_KEY + "='%s'";

    private static final String INSERT_SQL = "insert into %s ("
            + Joiner.on(",").join(META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC)
            + ") values (?, ?, ?, ?)";
    private static final String UPDATE_SQL = "update %s set " + META_TABLE_CONTENT + "=?, " + META_TABLE_MVCC + "=?, "
            + META_TABLE_TS + "=? where " + META_TABLE_KEY + "=? and " + META_TABLE_MVCC + "=?";
    private static final String DELETE_SQL = "delete from %s where " + META_TABLE_KEY + "=?";

    private static final String UPDATE_KEY_SQL = "update %s set " + META_TABLE_KEY + "='%s', " + META_TABLE_MVCC + "="
            + META_TABLE_MVCC + 1 + ", " + META_TABLE_TS + "=? where " + META_TABLE_KEY + "=? ";

    @Getter
    private final DataSourceTransactionManager transactionManager;
    @Getter
    private final JdbcTemplate jdbcTemplate;
    private final String table;

    public JdbcMetadataStore(KylinConfig config) throws Exception {
        super(config);
        val url = config.getMetadataUrl();
        val props = JdbcUtil.datasourceParameters(url);
        val dataSource = JdbcDataSource.getDataSource(props);
        transactionManager = new DataSourceTransactionManager(dataSource);
        jdbcTemplate = new JdbcTemplate(dataSource);
        table = url.getIdentifier();
        if (config.isMetadataAuditLogEnabled()) {
            auditLogStore = new JdbcAuditLogStore(config, jdbcTemplate, transactionManager,
                    table + JdbcAuditLogStore.AUDIT_LOG_SUFFIX);
        }
        epochStore = EpochStore.getEpochStore(config);
        createIfNotExist();
    }

    @Override
    protected void save(String path, @Nullable ByteSource bs, long ts, long mvcc, String unitPath, long epochId)
            throws Exception {
        withTransaction(transactionManager, new JdbcUtil.Callback<Object>() {
            @Override
            public Object handle() {
                checkEpochModified(unitPath, epochId);
                int affectedRow;
                if (bs != null) {
                    val result = jdbcTemplate.query(
                            String.format(Locale.ROOT, SELECT_BY_KEY_MVCC_SQL, table, path, mvcc - 1),
                            RAW_RESOURCE_ROW_MAPPER);
                    if (CollectionUtils.isEmpty(result)) {
                        affectedRow = insert(String.format(Locale.ROOT, INSERT_SQL, table), path, bs, ts, mvcc);
                    } else {
                        affectedRow = update(String.format(Locale.ROOT, UPDATE_SQL, table), bs, mvcc, ts, path,
                                mvcc - 1);
                    }
                } else {
                    val result = jdbcTemplate.query(String.format(Locale.ROOT, SELECT_BY_KEY_SQL, table, path),
                            RAW_RESOURCE_ROW_MAPPER);
                    if (CollectionUtils.isEmpty(result)) {
                        return null;
                    }
                    affectedRow = jdbcTemplate.update(String.format(Locale.ROOT, DELETE_SQL, table), path);
                }
                if (affectedRow == 0) {
                    throw new KylinException(FAILED_UPDATE_METADATA,
                            String.format(Locale.ROOT, "Failed to update or insert path: %s, mvcc: %s", path, mvcc));
                }
                return null;
            }

            @Override
            public void onError() {
                try {
                    log.warn("write {} {} {} failed", path, mvcc,
                            bs == null ? null : new String(bs.read(), Charset.defaultCharset()));
                } catch (IOException ignore) {
                }
            }
        });
    }

    public void checkEpochModified(String unitPath, long oriEpochId) {
        if (StringUtils.isNotEmpty(unitPath)) {
            if (unitPath.startsWith("_") && !unitPath.equalsIgnoreCase(GLOBAL_UNIT)) {
                return;
            }

            if (oriEpochId < 0) {
                return;
            }

            Epoch epoch = epochStore.getEpoch(unitPath);
            if (epoch == null) {
                throw new IllegalStateException("Epoch of key " + unitPath + " has been modified");
            }
            long epochId = epoch.getEpochId();
            if (oriEpochId != epochId)
                throw new IllegalStateException(String.format(Locale.ROOT,
                        "EpochId for path %s dose not match, origin epoch id is %s, but epoch id in db is %s.",
                        unitPath, oriEpochId, epochId));
        }
    }

    @Override
    public void move(String srcPath, String destPath) throws Exception {
        withTransaction(transactionManager, new JdbcUtil.Callback<Object>() {
            @Override
            public Object handle() {
                jdbcTemplate.update(String.format(Locale.ROOT, UPDATE_KEY_SQL, table, destPath),
                        System.currentTimeMillis(), srcPath);
                return null;
            }

            @Override
            public void onError() {
                log.warn("move {} to {} failed", srcPath, destPath);
            }
        });
    }

    private int insert(String sql, String path, ByteSource bs, long ts, long mvcc) {
        return jdbcTemplate.update(sql, ps -> {
            ps.setString(1, path);
            try {
                ps.setBytes(2, CompressionUtils.compress(bs.read()));
            } catch (IOException e) {
                log.error("exception: ", e);
                throw new SQLException(e);
            }
            ps.setLong(3, ts);
            ps.setLong(4, mvcc);
        });
    }

    private int update(String sql, ByteSource bs, long ts, long mvcc, String path, long oldMvcc) {
        return jdbcTemplate.update(sql, ps -> {
            try {
                ps.setBytes(1, CompressionUtils.compress(bs.read()));
            } catch (IOException e) {
                log.error("exception: ", e);
                throw new SQLException(e);
            }
            ps.setLong(2, ts);
            ps.setLong(3, mvcc);
            ps.setString(4, path);
            ps.setLong(5, oldMvcc);
        });
    }

    @Override
    public NavigableSet<String> list(String rootPath) {
        val allPaths = withTransaction(transactionManager,
                () -> jdbcTemplate.queryForList(String.format(Locale.ROOT, SELECT_ALL_KEY_SQL, table), String.class));
        return Sets.newTreeSet(allPaths);
    }

    @Override
    public RawResource load(String path) throws IOException {
        return withTransaction(transactionManager, () -> jdbcTemplate
                .queryForObject(String.format(Locale.ROOT, SELECT_BY_KEY_SQL, table, path), RAW_RESOURCE_ROW_MAPPER));
    }

    @Override
    public void batchUpdate(UnitMessages unitMessages, boolean skipAuditLog, String unitPath, long epochId)
            throws Exception {
        if (CollectionUtils.isEmpty(unitMessages.getMessages())) {
            return;
        }
        withTransaction(transactionManager, () -> {
            super.batchUpdate(unitMessages, skipAuditLog, unitPath, epochId);
            return null;
        });
    }

    @Override
    public void dump(ResourceStore store) throws Exception {
        withTransaction(transactionManager, () -> {
            super.dump(store);
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

    private void restoreProject(MemoryMetaData data, String project) {
        val rowMapper = new RawResourceRowMapper();
        var prevKey = "/" + project + "/";
        val endKey = "/" + project + "/~";
        val resources = jdbcTemplate.query(String.format(Locale.ROOT, SELECT_BY_RANGE_SQL, table, prevKey, endKey),
                rowMapper);

        for (RawResource resource : resources) {
            data.put(resource.getResPath(), new VersionedRawResource(resource));
        }
    }

    private void createIfNotExist() throws Exception {
        if (isTableExists(jdbcTemplate.getDataSource().getConnection(), table)) {
            return;
        }
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
        var sql = properties.getProperty("create.metadata.store.table");
        jdbcTemplate.execute(String.format(Locale.ROOT, sql, table, META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS,
                META_TABLE_MVCC));
        log.info("Succeed to create table: {}", table);
    }

    @Override
    public MemoryMetaData reloadAll() {
        MemoryMetaData data = MemoryMetaData.createEmpty();
        return withTransaction(transactionManager, () -> {
            log.debug("start reloadAll");
            //for lock meta store table
            jdbcTemplate.queryForObject(String.format(Locale.ROOT, "select max(%s) from %s", META_TABLE_KEY, table),
                    String.class);

            restoreProject(data, "_global");
            val projects = listProjects(data);
            projects.forEach(project -> restoreProject(data, project));

            loadUUIDQuietly(data);
            long offset = getAuditLogStore().getMaxId();

            log.debug("end reloadAll offset is {}", offset);
            data.setOffset(offset);
            return data;
        }, TransactionDefinition.ISOLATION_SERIALIZABLE);

    }

    private void loadUUIDQuietly(MemoryMetaData data) {
        try {
            val uuidRaw = jdbcTemplate.queryForObject(
                    String.format(Locale.ROOT, SELECT_BY_KEY_SQL, table, ResourceStore.METASTORE_UUID_TAG),
                    RAW_RESOURCE_ROW_MAPPER);
            data.put(uuidRaw.getResPath(), new VersionedRawResource(uuidRaw));
        } catch (PersistException | EmptyResultDataAccessException e) {
            if (e instanceof EmptyResultDataAccessException || e.getCause() instanceof EmptyResultDataAccessException) {
                log.info("Cannot find /UUID in metastore");
            } else {
                throw e;
            }
        }

    }

    private Set<String> listProjects(MemoryMetaData data) {
        Set<String> projects = Sets.newTreeSet();

        data.getData().keySet().forEach(path -> {
            if (path.startsWith("/_global/project/")) {
                val words = path.split("/");
                val project = words[words.length - 1].replace(".json", "");
                projects.add(project);
            }
        });

        return projects;
    }
}
