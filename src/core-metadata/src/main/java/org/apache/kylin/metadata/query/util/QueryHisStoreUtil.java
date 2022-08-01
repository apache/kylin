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

package org.apache.kylin.metadata.query.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.apache.ibatis.type.JdbcType;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.logging.LogOutputStream;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.query.QueryHistoryDAO;
import org.apache.kylin.metadata.query.QueryHistoryMapper;
import org.apache.kylin.metadata.query.QueryHistoryRealizationMapper;
import org.apache.kylin.metadata.query.QueryStatisticsMapper;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryHisStoreUtil {

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    private static final String CREATE_QUERY_HISTORY_TABLE = "create.queryhistory.store.table";
    private static final String CREATE_QUERY_HISTORY_INDEX1 = "create.queryhistory.store.tableindex1";
    private static final String CREATE_QUERY_HISTORY_INDEX2 = "create.queryhistory.store.tableindex2";
    private static final String CREATE_QUERY_HISTORY_INDEX3 = "create.queryhistory.store.tableindex3";
    private static final String CREATE_QUERY_HISTORY_INDEX4 = "create.queryhistory.store.tableindex4";
    private static final String CREATE_QUERY_HISTORY_INDEX5 = "create.queryhistory.store.tableindex5";

    private static final String CREATE_QUERY_HISTORY_REALIZATION_TABLE = "create.queryhistoryrealization.store.table";
    private static final String CREATE_QUERY_HISTORY_REALIZATION_INDEX1 = "create.queryhistoryrealization.store.tableindex1";
    private static final String CREATE_QUERY_HISTORY_REALIZATION_INDEX2 = "create.queryhistoryrealization.store.tableindex2";

    private QueryHisStoreUtil() {
    }

    public static SqlSessionFactory getSqlSessionFactory(DataSource dataSource, String qhTableName,
            String qhRealizationTableName) {
        return Singletons.getInstance("query-history-sql-session-factory", SqlSessionFactory.class, clz -> {
            TransactionFactory transactionFactory = new JdbcTransactionFactory();
            Environment environment = new Environment("query history", transactionFactory, dataSource);
            Configuration configuration = new Configuration(environment);
            configuration.setUseGeneratedKeys(true);
            configuration.setJdbcTypeForNull(JdbcType.NULL);
            configuration.addMapper(QueryHistoryMapper.class);
            configuration.addMapper(QueryHistoryRealizationMapper.class);
            configuration.addMapper(QueryStatisticsMapper.class);
            createQueryHistoryIfNotExist((BasicDataSource) dataSource, qhTableName);
            createQueryHistoryRealizationIfNotExist((BasicDataSource) dataSource, qhRealizationTableName);
            return new SqlSessionFactoryBuilder().build(configuration);
        });
    }

    private static void createQueryHistoryIfNotExist(BasicDataSource dataSource, String qhTableName)
            throws SQLException, IOException {
        if(JdbcTableUtil.isTableExist(dataSource, qhTableName)) {
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            Properties properties = JdbcUtil.getProperties(dataSource);

            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_TABLE), qhTableName)
                            .getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_INDEX1), qhTableName,
                            qhTableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_INDEX2), qhTableName,
                            qhTableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_INDEX3), qhTableName,
                            qhTableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_INDEX4), qhTableName,
                            qhTableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_INDEX5), qhTableName,
                            qhTableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
        }
    }

    private static void createQueryHistoryRealizationIfNotExist(BasicDataSource dataSource,
            String qhRealizationTableName) throws SQLException, IOException {
        try (Connection connection = dataSource.getConnection()) {
            if (JdbcUtil.isTableExists(connection, qhRealizationTableName)) {
                return;
            }
        } catch (Exception e) {
            log.error("Fail to know if table {} exists", qhRealizationTableName, e);
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            Properties properties = JdbcUtil.getProperties(dataSource);

            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_REALIZATION_TABLE),
                            qhRealizationTableName).getBytes(Charset.defaultCharset())),
                    Charset.defaultCharset()));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_REALIZATION_INDEX1),
                            qhRealizationTableName, qhRealizationTableName).getBytes(Charset.defaultCharset())),
                    Charset.defaultCharset()));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_REALIZATION_INDEX2),
                            qhRealizationTableName, qhRealizationTableName).getBytes(Charset.defaultCharset())),
                    Charset.defaultCharset()));
        }
    }

    public static void cleanQueryHistory() {
        try (SetThreadName ignored = new SetThreadName("QueryHistoryCleanWorker")) {
            val config = KylinConfig.getInstanceFromEnv();
            val projectManager = NProjectManager.getInstance(config);
            getQueryHistoryDao().deleteQueryHistoriesIfMaxSizeReached();
            getQueryHistoryDao().deleteQueryHistoriesIfRetainTimeReached();
            for (ProjectInstance project : projectManager.listAllProjects()) {
                if (!EpochManager.getInstance().checkEpochOwner(project.getName()))
                    continue;
                try {
                    long startTime = System.currentTimeMillis();
                    log.info("Start to delete query histories that are beyond max size for project<{}>",
                            project.getName());
                    getQueryHistoryDao().deleteQueryHistoriesIfProjectMaxSizeReached(project.getName());
                    log.info("Query histories cleanup for project<{}> finished, it took {}ms", project.getName(),
                            System.currentTimeMillis() - startTime);
                } catch (Exception e) {
                    log.error("clean query histories<" + project.getName() + "> failed", e);
                }
            }
        }
    }

    private static QueryHistoryDAO getQueryHistoryDao() {
        return RDBMSQueryHistoryDAO.getInstance();
    }
}
