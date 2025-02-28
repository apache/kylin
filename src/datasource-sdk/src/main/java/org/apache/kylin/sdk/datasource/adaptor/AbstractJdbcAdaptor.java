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
package org.apache.kylin.sdk.datasource.adaptor;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.sql.rowset.CachedRowSet;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.base.Joiner;
import org.apache.kylin.guava30.shaded.common.cache.Cache;
import org.apache.kylin.guava30.shaded.common.cache.CacheBuilder;
import org.apache.kylin.sdk.datasource.framework.FixedCachedRowSetImpl;
import org.apache.kylin.sdk.datasource.framework.conv.DefaultConfigurer;
import org.apache.kylin.sdk.datasource.framework.conv.SqlConverter;
import org.apache.kylin.sdk.datasource.framework.def.DataSourceDef;
import org.apache.kylin.sdk.datasource.framework.def.DataSourceDefProvider;
import org.apache.kylin.source.SupportsSparkCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends this Abstract class to create Adaptors for new jdbc data source.
 */
public abstract class AbstractJdbcAdaptor implements Closeable, SupportsSparkCatalog {

    protected static final Logger logger = LoggerFactory.getLogger(DefaultAdaptor.class);
    protected final BasicDataSource dataSource;
    protected final AdaptorConfig config;
    protected final DataSourceDef dataSourceDef;
    protected SqlConverter.IConfigurer configurer;
    protected final Cache<String, List<String>> DATABASES_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.DAYS).maximumSize(4096).build();
    protected final Cache<String, List<String>> TABLES_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.DAYS).maximumSize(4096).build();
    protected final Cache<String, List<String>> COLUMNS_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.DAYS).maximumSize(4096).build();

    private static Joiner joiner = Joiner.on("_");

    // for ut only
    @VisibleForTesting
    public AdaptorConfig getConfig() {
        return config;
    }

    // for ut only
    @VisibleForTesting
    public BasicDataSource getDataSource() {
        return dataSource;
    }

    // Used by DefaultSourceConnector just for build, do not abuse it!
    protected AbstractJdbcAdaptor() {
        this.dataSource = null;
        this.config = null;
        this.dataSourceDef = null;
    }

    /**
     * Default constructor method.
     *
     * @param config Basic configuration of JDBC source, such as driver name, URL, username, password.
     * @throws Exception If datasource cannot be connected.
     */
    protected AbstractJdbcAdaptor(AdaptorConfig config) throws Exception {
        this.config = config;
        this.dataSourceDef = DataSourceDefProvider.getInstance().getById(config.datasourceId);

        Class.forName(config.driver);
        Properties props = new Properties();
        props.setProperty("driverClassName", config.driver);
        props.setProperty("url", config.url);
        props.setProperty("username", config.username);
        props.setProperty("password", config.password);
        props.setProperty("maxTotal", String.valueOf(config.poolMaxTotal));
        props.setProperty("maxIdle", String.valueOf(config.poolMaxIdle));
        props.setProperty("minIdle", String.valueOf(config.poolMinIdle));

        dataSource = BasicDataSourceFactory.createDataSource(props);

        DataSourceDefProvider provider = DataSourceDefProvider.getInstance();
        DataSourceDef jdbcDs = provider.getById(getDataSourceId());
        configurer = new DefaultConfigurer(this, jdbcDs);
    }

    /**
     * Used by <C>org.apache.commons.dbcp.BasicDataSource</C> to validate source connection.
     *
     * @return
     */
    protected String getSourceValidationSql() {
        if (dataSourceDef.getValidationQuery() != null)
            return dataSourceDef.getValidationQuery();

        // some default implementation
        switch (config.driver) {
        case "org.hsqldb.jdbcDriver":
            return "select 1 from INFORMATION_SCHEMA.SYSTEM_USERS";
        case "oracle.jdbc.driver.OracleDriver":
        case "oracle.jdbc.OracleDriver":
            return "select 1 from dual";
        case "com.ibm.db2.jcc.DB2Driver":
            return "select 1 from sysibm.sysdummy1";
        case "org.postgresql.Driver":
            return "select version();";
        case "org.apache.derby.jdbc.ClientDriver":
            return "values 1";
        default:
            return "select 1";
        }
    }

    /**
     * Convert a <C>ResultSet</C> to <C>CachedRowSet</C>,
     * because <C>ResultSet</C> relies on the connection kept Open, but <C>CachedRowSet</C> not.
     *
     * @param resultSet
     * @return
     * @throws SQLException
     */
    protected CachedRowSet cacheResultSet(ResultSet resultSet) throws SQLException {
        CachedRowSet crs = new FixedCachedRowSetImpl();
        crs.populate(resultSet);
        return crs;
    }

    /**
     * Create a new <C>java.sql.Connection</C> for this JDBC source.
     *
     * @return
     * @throws SQLException
     */
    public Connection getConnection() throws SQLException {
        if (config.getConnectRetryTimes() > 0) {
            int retry = 0;
            while (true) {
                try {
                    retry++;
                    return dataSource.getConnection();
                } catch (SQLException ex) {
                    logger.warn("Try connect to {} {} time(s) with error {}.", config.url, retry,
                            ex.getLocalizedMessage());
                    if (retry >= config.getConnectRetryTimes()) {
                        break;
                    }
                    try {
                        TimeUnit.MILLISECONDS.sleep(config.getSleepMillisecBetweenRetry());
                    } catch (InterruptedException e) {
                        logger.error("Retry connecting to {} is interrupted whiling sleeping:", config.url, e);
                        Thread.currentThread().interrupt();
                    }
                }
            }
            throw new SQLException(String.format(Locale.ROOT, "Can not connect to %s after retry %d time(s)",
                    config.url, config.getConnectRetryTimes()));
        } else {
            return dataSource.getConnection();
        }
    }

    public void setDefaultDb(Connection connection, String db) throws SQLException {
        //do nothing ,implement by sub class
        //connection.setSchema(db) or connection.setCatalog(db);

    }

    /**
     * Create a new <C>java.sql.Connection</C> for this JDBC source.
     *
     * @return
     * @throws SQLException
     */
    public Connection getConnectionWithDefaultDB(String dbName) throws SQLException {
        Connection connection = getConnection();
        if (StringUtils.isNotBlank(dbName)) {
            setDefaultDb(connection, dbName);
        }
        return connection;

    }

    /**
     * To close a <C>AutoCloseable</C> implementation, such as <C>java.sql.Connection</C>,
     * <C>java.sql.Statement</C>, <C><java.sql.ResultSet/C>.
     *
     * @param closeable Such as <C>java.sql.Connection</C>, <C>java.sql.Statement</C>, <C><java.sql.ResultSet/C>.
     */
    protected void close(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * To close the adaptor, because we need to close all connections on this JDBC source.
     *
     * @throws IOException If close failed.
     */
    @Override
    public void close() throws IOException {
        try {
            dataSource.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * To execute a sql statement, and won't expect the result. Usually used to execute some Update operations.
     *
     * @param sql A sql statement.
     * @throws SQLException If the sql statement executed failed.
     */
    public void executeUpdate(String sql) throws SQLException {
        Statement statement = null;
        Connection connection = getConnection();
        try {
            statement = connection.createStatement();
            statement.execute(sql);
        } finally {
            close(statement);
            close(connection);
        }
    }

    /**
     * To execute a set of sql statements, and won't expect the results.
     * Usually used to execute a set of Update operations.
     *
     * @param sqls A set of sql statements
     * @throws SQLException If one of the sql statements executed failed.
     */
    public void executeUpdate(String[] sqls) throws SQLException {
        Statement statement = null;
        Connection connection = getConnection();
        try {
            statement = connection.createStatement();
            for (String sql : sqls) {
                statement.execute(sql);
            }
        } finally {
            close(statement);
            close(connection);
        }
    }

    /**
     * Get JDBC Url for JDBC source, usually it's from the <C>AdaptorConfig</C> passed to constructor.
     *
     * @return The JDBC Url.
     */
    public String getJdbcUrl() {
        return dataSource.getUrl();
    }

    /**
     * Get JDBC Driver class name for JDBC source, usually it's from the <C>AdaptorConfig</C> passed to constructor.
     *
     * @return JDBC Driver class name.
     */
    public String getJdbcDriver() {
        return dataSource.getDriverClassName();
    }

    /**
     * Get JDBC Username for JDBC source, usually it's from the <C>AdaptorConfig</C> passed to constructor.
     *
     * @return The JDBC Username.
     */
    public String getJdbcUser() {
        return dataSource.getUsername();
    }

    /**
     * Get JDBC Password for JDBC source, usually it's from the <C>AdaptorConfig</C> passed to constructor.
     *
     * @return The JDBC password.
     */
    public String getJdbcPassword() {
        return dataSource.getPassword();
    }

    /**
     * A datasource Id relates to a datasource in xml configuration file, use to find function and type mapping.
     *
     * @return
     */
    public String getDataSourceId() {
        return config.datasourceId == null ? "default" : config.datasourceId;
    }
    // ================================================================================

    /**
     * To convert a column type from JDBC source to the JDBC type supported by Kylin.
     * More about JDBC type, see the definition in <C>java.sql.Types</C>
     * For example, Presto return <C>Types.LONGNVARCHAR</C> for "VARCHAR" type, we need to convert to <C>Types.VARCHAR</C> type.
     *
     * @param type   The column type name from JDBC source.
     * @param typeId The column type id from JDBC source.
     * @return The column type if supported by Calcite(Kylin).
     */
    public abstract int toKylinTypeId(String type, int typeId);

    /**
     * To convert a column type name from JDBC source to the JDBC Type supported by Kylin.
     *
     * @param sourceTypeId Column type id from Source
     * @return Column type name supported by kylin.
     */
    public abstract String toKylinTypeName(int sourceTypeId);

    /**
     * To convert a column type name which is defined in Kylin metadata,
     * to a column type name which is supported in JDBC source.
     * For example, Kylin defines a integer type as "INTEGER" in table metadata,
     * but JDBC source defines it as "INT". So we need to convert from "INTEGER" to "INT" here.
     *
     * @param kylinTypeName A column type name which is defined in Kylin.
     * @return A column type name which is supported in JDBC source.
     */
    public abstract String toSourceTypeName(String kylinTypeName);

    /**
     * To fix the sql to be smoothly executed in JDBC source,
     * because the SQL dialect may be different between Kylin and JDBC source.
     * The framework will convert the sql according to dialect of jdbc source
     * firstly (if skipDefaultSqlConvert() returns <C>FALSE</C>), and then
     * call this method.
     *
     * @param sql The SQL statement to be fixed.
     * @return The fixed SQL statement.
     */
    public abstract String fixSql(String sql);

    /**
     * fix case sensitive for identifier
     *
     * @param identifier
     * @return
     */
    public abstract String fixIdentifierCaseSensitive(String identifier);

    /**
     * To list all the available database names from JDBC source.
     * Some JDBC source will expose SYSTEM databases from the default implementation,
     * then developers can overwrite this method and do some filtering.
     * Besides, some RDBMS uses Catalog as the definition of database, you can find details in <C>MysqlAdaptor</C>
     *
     * @return The list of all the available database names.
     * @throws SQLException If metadata fetch failed.
     */
    public abstract List<String> listDatabases() throws SQLException;

    /**
     * list databases with cache
     *
     * @return
     * @throws SQLException
     */
    public List<String> listDatabasesWithCache() throws SQLException {
        return listDatabasesWithCache(false);
    }

    /**
     * list databases with cache
     *
     * @param init
     * @return
     * @throws SQLException
     */
    public List<String> listDatabasesWithCache(boolean init) throws SQLException {
        if (configurer.enableCache()) {
            String cacheKey = joiner.join(config.datasourceId, config.url, "databases");
            List<String> cachedDatabases;
            if (init || (cachedDatabases = DATABASES_CACHE.getIfPresent(cacheKey)) == null) {
                cachedDatabases = listDatabases();
                DATABASES_CACHE.put(cacheKey, cachedDatabases);
            }
            return cachedDatabases;
        }
        return listDatabases();
    }

    /**
     * To list all the available tables inside a database from JDBC source.
     * Developers can overwrite this method to do some filtering work.
     *
     * @param database The given database.
     * @return The list of all the available tables.
     * @throws SQLException If metadata fetch failed.
     */
    public abstract List<String> listTables(String database) throws SQLException;

    /**
     * list tables with cache
     *
     * @param database
     * @param init
     * @return
     * @throws SQLException
     */
    public List<String> listTablesWithCache(String database, boolean init) throws SQLException {
        if (configurer.enableCache()) {
            String cacheKey = joiner.join(config.datasourceId, config.url, database, "tables");
            List<String> cachedTables;
            if (init || (cachedTables = TABLES_CACHE.getIfPresent(cacheKey)) == null) {
                cachedTables = listTables(database);
                TABLES_CACHE.put(cacheKey, cachedTables);
            }
            return cachedTables;
        }
        return listTables(database);
    }

    /**
     * To list all the available columns inside a table in database from JDBC source.
     * Developers can overwrite this method to do some filtering work.
     *
     * @param database  The given database.
     * @param tableName The given table name
     * @return The list of all the available columns of a table.
     * @throws SQLException If metadata fetch failed.
     */
    public abstract List<String> listColumns(String database, String tableName) throws SQLException;

    /**
     * To get the metadata in form of <C>javax.sql.rowset.CachedRowSet</C> for a table inside a database.
     *
     * @param database The given database name
     * @param table    The given table name
     * @return The metadata of the given table in form of <C>javax.sql.rowset.CachedRowSet</C>
     * @throws SQLException If metadata fetch failed.
     */
    public abstract CachedRowSet getTable(String database, String table) throws SQLException;

    /**
     * To get all columns metadata in form of <C>javax.sql.rowset.CachedRowSet</C> for a table inside a database.
     *
     * @param database The given database name
     * @param table    The given table name
     * @return The metadata of all columns metadata in form of <C>javax.sql.rowset.CachedRowSet</C>
     * @throws SQLException If metadata fetch failed.
     */
    public abstract CachedRowSet getTableColumns(String database, String table) throws SQLException;

    /**
     * To build a set of sql statements to create a schema in JDBC source.
     *
     * @param schemaName The name of schema.
     * @return The sql statement which can be executed in JDBC source.
     */
    public abstract String[] buildSqlToCreateSchema(String schemaName);

    /**
     * To build a set sql statements to load data from local directory.
     *
     * @param tableName    The name of table
     * @param tableFileDir The path of local directory.
     * @return The sql statement which can be executed in JDBC source.
     */
    public abstract String[] buildSqlToLoadDataFromLocal(String tableName, String tableFileDir);

    /**
     * To build a set of sql statements to create table in JDBC source.
     *
     * @param identity   The name of table to be created.
     * @param columnInfo The column information, in the pair of NAME -> TYPE
     * @return A set of SQL Statements which can be executed in JDBC source.
     */
    public abstract String[] buildSqlToCreateTable(String identity, LinkedHashMap<String, String> columnInfo);

    /**
     * To build a set of sql statements to create view in JDBC source.
     *
     * @param viewName The name of view to be created.
     * @param sql      The sql statement to be used as the body of sql statement.
     * @return A set of SQL Statements which can be executed in JDBC source.
     */
    public abstract String[] buildSqlToCreateView(String viewName, String sql);

    public boolean isCaseSensitive() {
        return configurer.isCaseSensitive();
    }

    protected void addConnectionProperty(String name, String value) {
        dataSource.addConnectionProperty(name, value);
    }

}
