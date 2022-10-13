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
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.isTableExists;
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.withTransaction;
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.withTransactionTimeout;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.exception.KylinException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.google.common.base.Joiner;

import lombok.Getter;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcEpochStore extends EpochStore {

    public static final String CREATE_EPOCH_TABLE = "create.epoch.store.table";
    public static final String EPOCH_TABLE_NAME = "_epoch";

    static final String EPOCH_ID = "epoch_id";
    static final String EPOCH_TARGET = "epoch_target";
    static final String CURRENT_EPOCH_OWNER = "current_epoch_owner";
    static final String LAST_EPOCH_RENEW_TIME = "last_epoch_renew_time";
    static final String SERVER_MODE = "server_mode";
    static final String MAINTENANCE_MODE_REASON = "maintenance_mode_reason";
    static final String MVCC = "mvcc";

    static final String INSERT_SQL = "insert into %s (" + Joiner.on(",").join(EPOCH_ID, EPOCH_TARGET,
            CURRENT_EPOCH_OWNER, LAST_EPOCH_RENEW_TIME, SERVER_MODE, MAINTENANCE_MODE_REASON, MVCC)
            + ") values (?, ?, ?, ?, ?, ?, ?)";

    static final String SELECT_SQL = "select " + Joiner.on(",").join(EPOCH_ID, EPOCH_TARGET, CURRENT_EPOCH_OWNER,
            LAST_EPOCH_RENEW_TIME, SERVER_MODE, MAINTENANCE_MODE_REASON, MVCC) + " from %s";

    static final String UPDATE_SQL = "update %s set " + EPOCH_ID + " =?, " + CURRENT_EPOCH_OWNER + " =?, "
            + LAST_EPOCH_RENEW_TIME + " =?, " + SERVER_MODE + " =?, " + MAINTENANCE_MODE_REASON + " =?, " + MVCC
            + " =? where " + EPOCH_TARGET + " =? and " + MVCC + " =?";

    static final String DELETE_SQL = "delete from %s where " + EPOCH_TARGET + " =?";

    static final String SELECT_BY_EPOCH_TARGET_SQL = "select " + Joiner.on(",").join(EPOCH_ID, EPOCH_TARGET,
            CURRENT_EPOCH_OWNER, LAST_EPOCH_RENEW_TIME, SERVER_MODE, MAINTENANCE_MODE_REASON, MVCC) + " from %s where "
            + EPOCH_TARGET + " = '%s'";

    @Getter
    private static JdbcTemplate jdbcTemplate;
    @Getter
    private static String table;
    @Getter
    private static DataSourceTransactionManager transactionManager;

    public static EpochStore getEpochStore(KylinConfig config) {
        return Singletons.getInstance(JdbcEpochStore.class, (clz) -> new JdbcEpochStore(config));
    }

    private JdbcEpochStore(KylinConfig config) throws Exception {
        val url = config.getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = JdbcDataSource.getDataSource(props);
        transactionManager = new DataSourceTransactionManager(dataSource);
        jdbcTemplate = new JdbcTemplate(dataSource);
        table = url.getIdentifier() + EPOCH_SUFFIX;
    }

    public static String getEpochSql(String sql, String tableName) {
        return String.format(Locale.ROOT, sql, tableName, EPOCH_ID, EPOCH_TARGET, CURRENT_EPOCH_OWNER,
                LAST_EPOCH_RENEW_TIME, SERVER_MODE, MAINTENANCE_MODE_REASON, MVCC, tableName, EPOCH_TARGET,
                EPOCH_TARGET);
    }

    public void createIfNotExist() throws Exception {
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
        var sql = properties.getProperty(CREATE_EPOCH_TABLE);

        withTransaction(transactionManager, () -> {
            jdbcTemplate.execute(getEpochSql(sql, table));
            return 1;
        });
        log.info("Succeed to create table: {}", table);
    }

    @Override
    public void update(Epoch epoch) {
        if (Objects.isNull(epoch)) {
            return;
        }
        executeWithTransaction(() -> {
            int affectedRow = jdbcTemplate.update(String.format(Locale.ROOT, UPDATE_SQL, table), ps -> {
                genUpdateEpochStatement(ps, epoch);
            });
            if (affectedRow == 0) {
                throw new KylinException(FAILED_UPDATE_METADATA,
                        String.format(Locale.ROOT, "Failed to update or save epoch:%s", epoch.toString()));
            }
            return affectedRow;
        });
    }

    @Override
    public void insert(Epoch epoch) {
        if (Objects.isNull(epoch)) {
            return;
        }
        executeWithTransaction(() -> {
            int affectedRow = jdbcTemplate.update(String.format(Locale.ROOT, INSERT_SQL, table), ps -> {
                genInsertEpochStatement(ps, epoch);
            });
            if (affectedRow == 0) {
                throw new KylinException(FAILED_UPDATE_METADATA,
                        String.format(Locale.ROOT, "Failed to update or save epoch:%s", epoch.toString()));
            }
            return affectedRow;
        });
    }

    @Override
    public void updateBatch(List<Epoch> epochs) {
        if (CollectionUtils.isEmpty(epochs)) {
            return;
        }

        int[] affectedRows = jdbcTemplate.batchUpdate(String.format(Locale.ROOT, UPDATE_SQL, table),
                new BatchPreparedStatementSetter() {

                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        val epoch = epochs.get(i);
                        genUpdateEpochStatement(ps, epoch);
                    }

                    @Override
                    public int getBatchSize() {
                        return epochs.size();
                    }
                });

        checkBatchResult(affectedRows, epochs.size());
    }

    @Override
    public void insertBatch(List<Epoch> epochs) {
        if (CollectionUtils.isEmpty(epochs)) {
            return;
        }

        int[] affectedRows = jdbcTemplate.batchUpdate(String.format(Locale.ROOT, INSERT_SQL, table),
                new BatchPreparedStatementSetter() {

                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        val epoch = epochs.get(i);
                        genInsertEpochStatement(ps, epoch);
                    }

                    @Override
                    public int getBatchSize() {
                        return epochs.size();
                    }
                });

        checkBatchResult(affectedRows, epochs.size());
    }

    private void checkBatchResult(int[] affectedRows, int expected) {
        int actual = Arrays.stream(affectedRows).sum();
        if (actual != expected) {
            throw new KylinException(FAILED_UPDATE_METADATA, String.format(Locale.ROOT,
                    "Failed to updateBatch or insertBatch actual:%d expected:%d", actual, expected));
        }
    }

    private void genInsertEpochStatement(final PreparedStatement ps, final Epoch epoch) throws SQLException {
        ps.setLong(1, epoch.getEpochId());
        ps.setString(2, epoch.getEpochTarget());
        ps.setString(3, epoch.getCurrentEpochOwner());
        ps.setLong(4, epoch.getLastEpochRenewTime());
        ps.setString(5, epoch.getServerMode());
        ps.setString(6, epoch.getMaintenanceModeReason());
        ps.setLong(7, epoch.getMvcc());
    }

    private void genUpdateEpochStatement(final PreparedStatement ps, final Epoch epoch) throws SQLException {
        ps.setLong(1, epoch.getEpochId());
        ps.setString(2, epoch.getCurrentEpochOwner());
        ps.setLong(3, epoch.getLastEpochRenewTime());
        ps.setString(4, epoch.getServerMode());
        ps.setString(5, epoch.getMaintenanceModeReason());
        ps.setLong(6, epoch.getMvcc() + 1);
        ps.setString(7, epoch.getEpochTarget());
        ps.setLong(8, epoch.getMvcc());
    }

    @Override
    public Epoch getEpoch(String epochTarget) {
        return executeWithTransaction(() -> {
            List<Epoch> result = jdbcTemplate.query(
                    String.format(Locale.ROOT, SELECT_BY_EPOCH_TARGET_SQL, table, epochTarget), new EpochRowMapper());
            if (result.isEmpty()) {
                return null;
            }
            return result.get(0);
        });
    }

    @Override
    public List<Epoch> list() {
        return executeWithTransaction(
                () -> jdbcTemplate.query(String.format(Locale.ROOT, SELECT_SQL, table), new EpochRowMapper()));
    }

    @Override
    public void delete(String epochTarget) {
        withTransaction(transactionManager,
                () -> jdbcTemplate.update(String.format(Locale.ROOT, DELETE_SQL, table), epochTarget));
    }

    @Override
    public <T> T executeWithTransaction(Callback<T> callback) {
        return withTransaction(transactionManager, callback::handle);
    }

    @Override
    public <T> T executeWithTransaction(Callback<T> callback, int timeout) {
        return withTransactionTimeout(transactionManager, callback::handle, timeout);
    }
}
