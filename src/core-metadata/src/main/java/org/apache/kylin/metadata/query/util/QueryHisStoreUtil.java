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

import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.isIndexExists;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.time.StopWatch;
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
import org.apache.kylin.common.logging.LogOutputStream;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.guava30.shaded.common.base.Strings;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.query.QueryHistoryDAO;
import org.apache.kylin.metadata.query.QueryHistoryMapper;
import org.apache.kylin.metadata.query.QueryHistoryRealizationMapper;
import org.apache.kylin.metadata.query.QueryStatisticsMapper;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;

import lombok.SneakyThrows;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryHisStoreUtil {

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    private static final String CREATE_QUERY_HISTORY_TABLE = "create.queryhistory.store.table";
    private static final String CREATE_QUERY_HISTORY_INDEX_PREFIX = "create.queryhistory.store.tableindex";
    private static final int CREATE_QUERY_HISTORY_INDEX_SIZE = 12;
    static final String[] QUERY_HISTORY_INDEX_NAMES = new String[CREATE_QUERY_HISTORY_INDEX_SIZE];
    static {
        for (int i = 0; i < CREATE_QUERY_HISTORY_INDEX_SIZE; i++) {
            QUERY_HISTORY_INDEX_NAMES[i] = CREATE_QUERY_HISTORY_INDEX_PREFIX + (i + 1);
        }
    }

    private static final String CREATE_QUERY_HISTORY_REALIZATION_TABLE = "create.queryhistoryrealization.store.table";
    private static final String CREATE_QUERY_HISTORY_REALIZATION_INDEX1 = "create.queryhistoryrealization.store.tableindex1";
    private static final String CREATE_QUERY_HISTORY_REALIZATION_INDEX2 = "create.queryhistoryrealization.store.tableindex2";
    private static final String CREATE_QUERY_HISTORY_REALIZATION_INDEX3 = "create.queryhistoryrealization.store.tableindex3";
    private static final String CREATE_QUERY_HISTORY_REALIZATION_INDEX4 = "create.queryhistoryrealization.store.tableindex4";
    private static final String CREATE_QUERY_HISTORY_REALIZATION_INDEX5 = "create.queryhistoryrealization.store.tableindex5";
    private static final String CREATE_QUERY_HISTORY_REALIZATION_INDEX6 = "create.queryhistoryrealization.store.tableindex6";
    static final String[] QUERY_HISTORY_REALIZATION_INDEX_NAMES = { CREATE_QUERY_HISTORY_REALIZATION_INDEX1,
            CREATE_QUERY_HISTORY_REALIZATION_INDEX2, CREATE_QUERY_HISTORY_REALIZATION_INDEX3,
            CREATE_QUERY_HISTORY_REALIZATION_INDEX4, CREATE_QUERY_HISTORY_REALIZATION_INDEX5,
            CREATE_QUERY_HISTORY_REALIZATION_INDEX6 };

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
        try (Connection connection = dataSource.getConnection()) {
            Properties properties = JdbcUtil.getProperties(dataSource);
            if (!JdbcUtil.isTableExists(connection, qhTableName, false)) {
                ScriptRunner sr = new ScriptRunner(connection);
                sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
                sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                        String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_TABLE), qhTableName)
                                .getBytes(DEFAULT_CHARSET)),
                        DEFAULT_CHARSET));
                log.info("Succeed to create query history table: {}", qhTableName);
            }
        }
        // create index for query history table
        createIndexIfNotExist(dataSource, qhTableName, QUERY_HISTORY_INDEX_NAMES);
    }

    private static void createIndexIfNotExist(BasicDataSource dataSource, String tableName, String[] indexNames) {
        try (Connection connection = dataSource.getConnection()) {
            for (int i = 0; i < indexNames.length; i++) {
                String indexConfig = indexNames[i];
                // index name format is %s_ix%s
                String indexName = tableName + "_ix" + (i + 1);
                Properties properties = JdbcUtil.getProperties(dataSource);
                var sql = properties.getProperty(indexConfig);
                if (Strings.isNullOrEmpty(sql) || isIndexExists(connection, tableName, indexName, false)) {
                    continue;
                }
                ScriptRunner sr = new ScriptRunner(connection);
                sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                        String.format(Locale.ROOT, properties.getProperty(indexConfig), tableName, tableName)
                                .getBytes(DEFAULT_CHARSET)),
                        DEFAULT_CHARSET));
                log.info("Succeed to create table {} index: {}", tableName, indexName);
            }
        } catch (Exception e) {
            // Failure to build an index is not a fatal error, so there is no need to throw an exception
            log.warn("Failed create index on table {}", tableName, e);
        }
    }

    private static void createQueryHistoryRealizationIfNotExist(BasicDataSource dataSource,
            String qhRealizationTableName) throws SQLException, IOException {
        try (Connection connection = dataSource.getConnection()) {
            Properties properties = JdbcUtil.getProperties(dataSource);
            if (!JdbcUtil.isTableExists(connection, qhRealizationTableName, false)) {
                ScriptRunner sr = new ScriptRunner(connection);
                sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
                sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                        String.format(Locale.ROOT, properties.getProperty(CREATE_QUERY_HISTORY_REALIZATION_TABLE),
                                qhRealizationTableName).getBytes(DEFAULT_CHARSET)),
                        DEFAULT_CHARSET));
            }
        }
        // create index for query history realization table
        createIndexIfNotExist(dataSource, qhRealizationTableName, QUERY_HISTORY_REALIZATION_INDEX_NAMES);
    }

    @SneakyThrows
    public static void cleanQueryHistory() {
        try (SetThreadName ignored = new SetThreadName("QueryHistoryCleanWorker")) {
            val config = KylinConfig.getInstanceFromEnv();
            val projectManager = NProjectManager.getInstance(config);

            getQueryHistoryDao().deleteQueryHistoriesIfMaxSizeReached();
            getQueryHistoryDao().deleteQueryHistoriesIfRetainTimeReached();

            Map<String, Long> projectCounts = getQueryHistoryDao().getQueryCountByProject();
            for (ProjectInstance project : projectManager.listAllProjects()) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Thread is interrupted: " + Thread.currentThread().getName());
                }
                long projectCount = projectCounts.getOrDefault(project.getName(), 0L);
                cleanQueryHistory(project.getName(), projectCount);
            }
        }
    }

    @SneakyThrows
    public static Long getQueryHistoryMinQueryTime() {
        return getQueryHistoryDao().getQueryHistoryMinQueryTime();
    }

    public static void cleanQueryHistory(String projectName, long historyCount) {
        long projectMaxSize = KylinConfig.getInstanceFromEnv().getQueryHistoryProjectMaxSize();
        if (historyCount <= projectMaxSize) {
            log.info("Query histories of project<{}> is less than the maximum limit, so skip it.", projectName);
            return;
        }
        try {
            StopWatch watch = StopWatch.createStarted();
            log.info("Start to delete query histories that are beyond max size for project<{}>, records:{}",
                    projectName, historyCount);
            getQueryHistoryDao().deleteOldestQueryHistoriesByProject(projectName,
                    (int) (historyCount - projectMaxSize));
            watch.stop();
            log.info("Query histories cleanup for project<{}> finished, it took {}ms", projectName, watch.getTime());
        } catch (Exception e) {
            log.error("Clean query histories for project<{}> failed", projectName, e);
        }
    }

    private static QueryHistoryDAO getQueryHistoryDao() {
        return RDBMSQueryHistoryDAO.getInstance();
    }
}
