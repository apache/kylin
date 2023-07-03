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
package org.apache.kylin.tool.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.logging.LogOutputStream;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetadataUtil {

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    private MetadataUtil() {
    }

    public static String getMetadataUrl(String rootPath) {
        if (rootPath.startsWith("file://")) {
            rootPath = rootPath.replace("file://", "");
        }
        return org.apache.commons.lang3.StringUtils.appendIfMissing(rootPath, "/");
    }

    public static DataSource getDataSource(KylinConfig kylinConfig) throws Exception {
        val url = kylinConfig.getMetadataUrl();
        val props = JdbcUtil.datasourceParameters(url);
        return JdbcDataSource.getDataSource(props);
    }

    public static void createTableIfNotExist(BasicDataSource dataSource, String tableName, String tableSql,
            List<String> indexSqlList) throws IOException, SQLException {
        if (JdbcUtil.isTableExists(dataSource.getConnection(), tableName)) {
            return;
        }

        if (null == indexSqlList) {
            indexSqlList = Lists.newArrayList();
        }

        Properties properties = JdbcUtil.getProperties(dataSource);
        String createTableStmt = String.format(Locale.ROOT, properties.getProperty(tableSql), tableName);
        List<String> crateIndexStmtList = indexSqlList.stream()
                .map(indexSql -> String.format(Locale.ROOT, properties.getProperty(indexSql), tableName, tableName))
                .collect(Collectors.toList());
        try (Connection connection = dataSource.getConnection()) {
            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.setStopOnError(true);
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(createTableStmt.getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            crateIndexStmtList.forEach(crateIndexStmt -> sr.runScript(new InputStreamReader(
                    new ByteArrayInputStream(crateIndexStmt.getBytes(DEFAULT_CHARSET)), DEFAULT_CHARSET)));

        }
    }

    public static void createTableIfNotExist(BasicDataSource dataSource, String tableName, String createTableStmt)
            throws SQLException {
        if (JdbcUtil.isTableExists(dataSource.getConnection(), tableName)) {
            return;
        }

        try (Connection connection = dataSource.getConnection()) {
            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.setStopOnError(true);
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(createTableStmt.getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
        }
    }
}
