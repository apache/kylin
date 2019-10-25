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

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.SourceDialect;
import org.apache.kylin.source.IReadableTable.TableReader;
import org.apache.kylin.source.hive.DBConnConf;
import org.apache.kylin.source.jdbc.metadata.IJdbcMetadata;
import org.apache.kylin.source.jdbc.metadata.JdbcMetadataFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of TableReader with JDBC.
 */
public class JdbcTableReader implements TableReader {
    private static final Logger logger = LoggerFactory.getLogger(JdbcTableReader.class);

    private String dbName;
    private String tableName;

    private DBConnConf dbconf;
    private Connection jdbcCon;
    private Statement statement;
    private ResultSet rs;
    private int colCount;

    /**
     * Constructor for reading whole jdbc table
     * @param dbName
     * @param tableName
     * @throws IOException
     */
    public JdbcTableReader(String dbName, String tableName) throws IOException {
        this.dbName = dbName;
        this.tableName = tableName;
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String connectionUrl = config.getJdbcSourceConnectionUrl();
        String driverClass = config.getJdbcSourceDriver();
        String jdbcUser = config.getJdbcSourceUser();
        String jdbcPass = config.getJdbcSourcePass();
        dbconf = new DBConnConf(driverClass, connectionUrl, jdbcUser, jdbcPass);
        jdbcCon = SqlUtil.getConnection(dbconf);
        IJdbcMetadata meta = JdbcMetadataFactory
                .getJdbcMetadata(SourceDialect.getDialect(config.getJdbcSourceDialect()), dbconf);

        Map<String, String> metadataCache = new TreeMap<>();
        JdbcHiveInputBase.JdbcBaseBatchCubingInputSide.calCachedJdbcMeta(metadataCache, dbconf, meta);
        String database = JdbcHiveInputBase.getSchemaQuoted(metadataCache, dbName, meta, true);
        String table = JdbcHiveInputBase.getTableIdentityQuoted(dbName, tableName, metadataCache, meta, true);

        String sql = String.format(Locale.ROOT, "select * from %s.%s", database, table);
        try {
            statement = jdbcCon.createStatement();
            rs = statement.executeQuery(sql);
            colCount = rs.getMetaData().getColumnCount();
        } catch (SQLException e) {
            throw new IOException(String.format(Locale.ROOT, "error while exec %s", sql), e);
        }
    }

    @Override
    public boolean next() throws IOException {
        try {
            return rs.next();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public String[] getRow() {
        String[] ret = new String[colCount];
        for (int i = 1; i <= colCount; i++) {
            try {
                Object o = rs.getObject(i);
                String result;
                if (null == o || o instanceof byte[]) {
                    result = null;
                } else {
                    result = o.toString();
                }
                ret[i - 1] = result;
            } catch (Exception e) {
                logger.error("", e);
            }
        }
        return ret;
    }

    @Override
    public void close() throws IOException {
        SqlUtil.closeResources(jdbcCon, statement);
    }

    public String toString() {
        return "jdbc table reader for: " + dbName + "." + tableName;
    }
}
