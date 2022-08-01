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
package org.apache.kylin.common.persistence.metadata.jdbc;

import static org.apache.kylin.common.exception.ServerErrorCode.EXCEED_MAX_ALLOWED_PACKET;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.metadata.PersistException;
import org.apache.kylin.common.util.EncryptUtil;
import org.msgpack.core.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import lombok.val;

public class JdbcUtil {

    private static final Logger logger = LoggerFactory.getLogger(JdbcUtil.class);

    public static <T> T withTransaction(DataSourceTransactionManager transactionManager, Callback<T> consumer) {
        return withTransaction(transactionManager, consumer, TransactionDefinition.ISOLATION_REPEATABLE_READ);
    }

    public static <T> T withTransaction(DataSourceTransactionManager transactionManager, Callback<T> consumer,
            int isolationLevel) {
        val definition = new DefaultTransactionDefinition();
        definition.setIsolationLevel(isolationLevel);
        val status = transactionManager.getTransaction(definition);
        try {
            T result = consumer.handle();
            transactionManager.commit(status);
            return result;
        } catch (Exception e) {
            transactionManager.rollback(status);
            if (e instanceof DataIntegrityViolationException) {
                consumer.onError();
            }

            if (Objects.nonNull(e.getMessage()) && e.getMessage().contains("max_allowed_packet")) {
                throw new KylinException(EXCEED_MAX_ALLOWED_PACKET, MsgPicker.getMsg().getExceedMaxAllowedPacket());
            }

            throw new PersistException("persist messages failed", e);
        }
    }

    public static boolean isTableExists(Connection conn, String table) throws SQLException {
        return isAnyTableExists(conn, table, table.toUpperCase(Locale.ROOT), table.toLowerCase(Locale.ROOT));
    }

    private static boolean isAnyTableExists(Connection conn, String... tables) throws SQLException {
        try {
            for (String table : tables) {
                val resultSet = conn.getMetaData().getTables(conn.getCatalog(), null, table, null);
                if (resultSet.next()) {
                    return true;
                }
            }
        } catch (Exception e) {
            logger.error("Fail to know if table {} exists", tables, e);
        } finally {
            if (!conn.isClosed()) {
                conn.close();
            }
        }
        return false;
    }

    public static boolean isIndexExists(Connection conn, String table, String index) throws SQLException {
        return isIndexExists(conn, index, table, table.toUpperCase(Locale.ROOT), table.toLowerCase(Locale.ROOT));
    }

    private static boolean isIndexExists(Connection conn, String index, String... tables) throws SQLException {
        try {
            for (String table : tables) {
                val resultSet = conn.getMetaData().getIndexInfo(null, null, table, false, false);
                while (resultSet.next()) {
                    String indexName = resultSet.getString("INDEX_NAME");
                    if (Objects.equals(indexName, index)) {
                        return true;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Fail to know if table {} index {} exists", tables, index, e);
        } finally {
            if (!conn.isClosed())
                conn.close();
        }
        return false;
    }

    public static boolean isColumnExists(Connection conn, String table, String column) throws SQLException {
        return isColumnExists(conn, column, table, table.toUpperCase(Locale.ROOT), table.toLowerCase(Locale.ROOT));
    }

    private static boolean isColumnExists(Connection conn, String column, String... tables) throws SQLException {
        try {
            for (String table : tables) {
                val resultSet = conn.getMetaData().getColumns(null, null, table, null);
                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    if (StringUtils.equalsIgnoreCase(columnName, column)) {
                        return true;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Fail to know if table {} column {} exists", tables, column, e);
        } finally {
            if (!conn.isClosed())
                conn.close();
        }
        return false;
    }

    public static Properties datasourceParameters(StorageURL url) {
        return KylinConfig.getInstanceFromEnv().isUTEnv() //
                ? datasourceParametersForUT(url) //
                : datasourceParametersForProd(url);
    }

    public static Properties datasourceParametersForProd(StorageURL url) {
        Properties props = new Properties();
        props.put("driverClassName", "org.postgresql.Driver");
        props.put("url", "jdbc:postgresql://sandbox:5432/kylin");
        props.put("username", "postgres");
        props.put("maxTotal", "50");
        props.putAll(url.getAllParameters());
        String password = props.getProperty("password", "");
        if (EncryptUtil.isEncrypted(password)) {
            password = EncryptUtil.decryptPassInKylin(password);
        }
        props.put("password", password);
        return props;
    }

    public static Properties datasourceParametersForUT(StorageURL url) {
        Properties props = new Properties();
        props.put("driverClassName", "org.h2.Driver");
        props.put("url", "jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1;MODE=MySQL");
        props.put("username", "sa");
        props.put("password", "");
        props.put("maxTotal", "50");
        props.putAll(url.getAllParameters());
        return props;
    }

    public static Properties getProperties(BasicDataSource dataSource) throws IOException {
        String fileName;
        switch (dataSource.getDriverClassName()) {
        case "org.postgresql.Driver":
            fileName = "metadata-jdbc-postgresql.properties";
            break;
        case "com.mysql.jdbc.Driver":
            fileName = "metadata-jdbc-mysql.properties";
            break;
        case "org.h2.Driver":
            fileName = "metadata-jdbc-h2.properties";
            break;
        default:
            throw new IllegalArgumentException("Unsupported jdbc driver");
        }
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        Properties properties = new Properties();
        properties.load(is);
        return properties;
    }

    public interface Callback<T> {
        T handle() throws Exception;

        default void onError() {
            // do nothing by default
        }
    }

    @VisibleForTesting
    public static JdbcTemplate getJdbcTemplate(KylinConfig kylinConfig) throws Exception {
        val url = kylinConfig.getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }
}
