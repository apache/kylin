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
package org.apache.kylin.source.jdbc;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.SourceDialect;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.hive.DBConnConf;
import org.apache.kylin.source.jdbc.metadata.IJdbcMetadata;
import org.apache.kylin.source.jdbc.metadata.JdbcMetadataFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcExplorer implements ISourceMetadataExplorer, ISampleDataDeployer {
    private static final Logger logger = LoggerFactory.getLogger(JdbcExplorer.class);

    private final SourceDialect dialect;
    private final DBConnConf dbconf;
    private final IJdbcMetadata jdbcMetadataDialect;

    @Deprecated
    public JdbcExplorer() {
        this(KylinConfig.getInstanceFromEnv());
    }

    public JdbcExplorer(KylinConfig config) {
         String connectionUrl = config.getJdbcSourceConnectionUrl();
         String driverClass = config.getJdbcSourceDriver();
         String jdbcUser = config.getJdbcSourceUser();
         String jdbcPass = config.getJdbcSourcePass();
         this.dbconf = new DBConnConf(driverClass, connectionUrl, jdbcUser, jdbcPass);
         this.dialect = SourceDialect.getDialect(config.getJdbcSourceDialect());
         this.jdbcMetadataDialect = JdbcMetadataFactory.getJdbcMetadata(dialect, dbconf);
     }

    @Override
    public List<String> listDatabases() throws SQLException {
        return jdbcMetadataDialect.listDatabases();
    }

    @Override
    public List<String> listTables(String schema) throws SQLException {
        return jdbcMetadataDialect.listTables(schema);
    }

    @Override
    public Pair<TableDesc, TableExtDesc> loadTableMetadata(String database, String table, String prj)
            throws SQLException {
        TableDesc tableDesc = new TableDesc();
        tableDesc.setDatabase(database.toUpperCase(Locale.ROOT));
        tableDesc.setName(table.toUpperCase(Locale.ROOT));
        tableDesc.setUuid(RandomUtil.randomUUID().toString());
        tableDesc.setLastModified(0);
        tableDesc.setSourceType(ISourceAware.ID_JDBC);

        Connection con = SqlUtil.getConnection(dbconf);
        DatabaseMetaData dbmd = con.getMetaData();

        try (ResultSet rs = jdbcMetadataDialect.getTable(dbmd, database, table)) {
            String tableType = null;
            while (rs.next()) {
                tableType = rs.getString("TABLE_TYPE");
            }
            if (tableType != null) {
                tableDesc.setTableType(tableType);
            } else {
                throw new RuntimeException(
                        String.format(Locale.ROOT, "table %s not found in schema:%s", table, database));
            }
        }

        try (ResultSet rs = jdbcMetadataDialect.listColumns(dbmd, database, table)) {
            tableDesc.setColumns(extractColumnFromMeta(rs));
        } finally {
            DBUtils.closeQuietly(con);
        }

        TableExtDesc tableExtDesc = new TableExtDesc();
        tableExtDesc.setIdentity(tableDesc.getIdentity());
        tableExtDesc.setUuid(RandomUtil.randomUUID().toString());
        tableExtDesc.setLastModified(0);
        tableExtDesc.init(prj);

        return Pair.newPair(tableDesc, tableExtDesc);
    }

    private String getSqlDataType(String javaDataType) {
        if (SourceDialect.VERTICA.equals(dialect) || SourceDialect.MYSQL.equals(dialect)) {
            if (javaDataType.toLowerCase(Locale.ROOT).equals("double")) {
                return "float";
            }
        }

        return javaDataType.toLowerCase(Locale.ROOT);
    }

    @Override
    public void createSampleDatabase(String database) throws Exception {
        executeSQL(generateCreateSchemaSql(database));
    }

    private String generateCreateSchemaSql(String schemaName) {
        if (SourceDialect.VERTICA.equals(dialect) || SourceDialect.MYSQL.equals(dialect)) {
            return String.format(Locale.ROOT, "CREATE schema IF NOT EXISTS %s", schemaName);
        } else if (SourceDialect.MSSQL.equals(dialect)) {
            return String.format(Locale.ROOT,
                    "IF NOT EXISTS (SELECT name FROM sys.schemas WHERE name = N'%s') EXEC('CREATE SCHEMA"
                            + " [%s] AUTHORIZATION [dbo]')",
                    schemaName, schemaName);
        } else {
            logger.error("unsupported dialect {}.", dialect);
            return null;
        }
    }

    @Override
    public void loadSampleData(String tableName, String tmpDataDir) throws Exception {
        executeSQL(generateLoadDataSql(tableName, tmpDataDir));
    }

    private String generateLoadDataSql(String tableName, String tableFileDir) {
        if (SourceDialect.VERTICA.equals(dialect)) {
            return String.format(Locale.ROOT, "copy %s from local '%s/%s.csv' delimiter as ',';", tableName,
                    tableFileDir, tableName);
        } else if (SourceDialect.MYSQL.equals(dialect)) {
            return String.format(Locale.ROOT, "LOAD DATA INFILE '%s/%s.csv' INTO %s FIELDS TERMINATED BY ',';",
                    tableFileDir, tableName, tableName);
        } else if (SourceDialect.MSSQL.equals(dialect)) {
            return String.format(Locale.ROOT, "BULK INSERT %s FROM '%s/%s.csv' WITH(FIELDTERMINATOR = ',')", tableName,
                    tableFileDir, tableName);
        } else {
            logger.error("unsupported dialect {}.", dialect);
            return null;
        }
    }

    @Override
    public void createSampleTable(TableDesc table) throws Exception {
        executeSQL(generateCreateTableSql(table));
    }

    private String[] generateCreateTableSql(TableDesc tableDesc) {
        logger.info("Generate create table sql: {}", tableDesc);
        String tableIdentity = String
                .format(Locale.ROOT, "%s.%s", tableDesc.getDatabase().toUpperCase(Locale.ROOT), tableDesc.getName())
                .toUpperCase(Locale.ROOT);
        String dropsql = "DROP TABLE IF EXISTS " + tableIdentity;
        String dropsql2 = "DROP VIEW IF EXISTS " + tableIdentity;

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE " + tableIdentity + "\n");
        ddl.append("(" + "\n");

        for (int i = 0; i < tableDesc.getColumns().length; i++) {
            ColumnDesc col = tableDesc.getColumns()[i];
            if (i > 0) {
                ddl.append(",");
            }
            ddl.append(col.getName() + " " + getSqlDataType((col.getDatatype())) + "\n");
        }

        ddl.append(")");

        return new String[] { dropsql, dropsql2, ddl.toString() };
    }

    @Override
    public void createWrapperView(String origTableName, String viewName) throws Exception {
        executeSQL(generateCreateViewSql(viewName, origTableName));
    }

    private String[] generateCreateViewSql(String viewName, String tableName) {

        String dropView = "DROP VIEW IF EXISTS " + viewName;
        String dropTable = "DROP TABLE IF EXISTS " + viewName;

        String createSql = ("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);

        return new String[] { dropView, dropTable, createSql };
    }

    private void executeSQL(String sql) throws SQLException {
        Connection con = SqlUtil.getConnection(dbconf);
        logger.info("Executing sql : {}", sql);
        try {
            SqlUtil.execUpdateSQL(con, sql);
        } finally {
            DBUtils.closeQuietly(con);
        }
    }

    private void executeSQL(String[] sqls) throws SQLException {
        try (Connection con = SqlUtil.getConnection(dbconf)) {
            for (String sql : sqls) {
                logger.info("Executing sql : {}", sql);
                SqlUtil.execUpdateSQL(con, sql);
            }
        }
    }

    @Override
    public List<String> getRelatedKylinResources(TableDesc table) {
        return Collections.emptyList();
    }

    @Override
    public ColumnDesc[] evalQueryMetadata(String query) {
        if (StringUtils.isEmpty(query)) {
            throw new RuntimeException("Evaluate query shall not be empty.");
        }

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String tmpDatabase = config.getHiveDatabaseForIntermediateTable();
        String tmpView = tmpDatabase + ".kylin_eval_query_"
                + UUID.nameUUIDFromBytes(query.getBytes(StandardCharsets.UTF_8)).toString().replaceAll("-", "");

        String dropViewSql = "DROP VIEW IF EXISTS " + tmpView;
        String evalViewSql = "CREATE VIEW " + tmpView + " as " + query;

        Connection con = null;
        ResultSet rs = null;
        try {
            logger.debug("Removing duplicate view {}", tmpView);
            executeSQL(dropViewSql);
            logger.debug("Creating view {} for query: {}", tmpView, query);
            executeSQL(evalViewSql);
            logger.debug("Evaluating query columns' metadata");
            con = SqlUtil.getConnection(dbconf);
            DatabaseMetaData dbmd = con.getMetaData();
            rs = dbmd.getColumns(null, tmpDatabase, tmpView, null);
            ColumnDesc[] result = extractColumnFromMeta(rs);
            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Cannot evaluate metadata of query: " + query, e);
        } finally {
            DBUtils.closeQuietly(con);
            DBUtils.closeQuietly(rs);
            try {
                logger.debug("Cleaning up temp view.");
                executeSQL(dropViewSql);
            } catch (SQLException e) {
                logger.warn("Failed to clean up temp view of query: {}", query, e);
            }
        }
    }

    @Override
    public void validateSQL(String query) throws Exception {
        executeSQL(query);
    }

    private ColumnDesc[] extractColumnFromMeta(ResultSet meta) throws SQLException {
        List<ColumnDesc> columns = new ArrayList<>();

        while (meta.next()) {
            String cname = meta.getString("COLUMN_NAME");
            int type = meta.getInt("DATA_TYPE");
            int csize = meta.getInt("COLUMN_SIZE");
            int digits = meta.getInt("DECIMAL_DIGITS");
            int pos = meta.getInt("ORDINAL_POSITION");
            String remarks = meta.getString("REMARKS");

            ColumnDesc cdesc = new ColumnDesc();
            cdesc.setName(cname.toUpperCase(Locale.ROOT));

            String kylinType = SqlUtil.jdbcTypeToKylinDataType(type);
            int precision = (SqlUtil.isPrecisionApplicable(kylinType) && csize > 0) ? csize : -1;
            precision = Math.min(precision, KylinConfig.getInstanceFromEnv().getDefaultVarcharPrecision());
            int scale = (SqlUtil.isScaleApplicable(kylinType) && digits > 0) ? digits : -1;

            cdesc.setDatatype(new DataType(kylinType, precision, scale).toString());
            cdesc.setId(String.valueOf(pos));
            cdesc.setComment(remarks);
            columns.add(cdesc);
        }

        return columns.toArray(new ColumnDesc[columns.size()]);
    }
}
