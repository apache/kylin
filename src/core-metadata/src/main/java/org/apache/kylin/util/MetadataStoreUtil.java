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

package org.apache.kylin.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.type.JdbcType;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.logging.LogOutputStream;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.metadata.favorite.AsyncTaskMapper;
import org.apache.kylin.metadata.favorite.FavoriteRuleMapper;
import org.apache.kylin.metadata.favorite.ModelFavoriteRuleMapper;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffsetMapper;
import org.apache.kylin.metadata.job.JobTokenMapper;
import org.mybatis.spring.transaction.SpringManagedTransactionFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetadataStoreUtil {
    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    public static final String CREATE_OFFSET_TABLE = "create.queryhistoryoffset.store.table";
    public static final String CREATE_FAVORITE_RULE_TABLE = "create.favoriterule.store.table";
    public static final String CREATE_MODEL_FAVORITE_RULE_TABLE = "create.model.favoriterule.store.table";
    public static final String CREATE_ASYNC_TASK_TABLE = "create.asynctask.store.table";
    public static final String CREATE_REC_TABLE = "create.job-token.table";

    private static final Map<TableType, String> SQL_TEMPLATE_MAP = new EnumMap<>(TableType.class);
    private static final Map<TableType, Class<?>> TABLE_MAPPER_MAP = new EnumMap<>(TableType.class);
    static {
        SQL_TEMPLATE_MAP.put(TableType.FAVORITE_RULE, CREATE_FAVORITE_RULE_TABLE);
        SQL_TEMPLATE_MAP.put(TableType.MODEL_FAVORITE_RULE, CREATE_MODEL_FAVORITE_RULE_TABLE);
        SQL_TEMPLATE_MAP.put(TableType.QUERY_HISTORY_OFFSET, CREATE_OFFSET_TABLE);
        SQL_TEMPLATE_MAP.put(TableType.ASYNC_TASK, CREATE_ASYNC_TASK_TABLE);
        SQL_TEMPLATE_MAP.put(TableType.JOB_TOKEN, CREATE_REC_TABLE);
        TABLE_MAPPER_MAP.put(TableType.FAVORITE_RULE, FavoriteRuleMapper.class);
        TABLE_MAPPER_MAP.put(TableType.MODEL_FAVORITE_RULE, ModelFavoriteRuleMapper.class);
        TABLE_MAPPER_MAP.put(TableType.QUERY_HISTORY_OFFSET, QueryHistoryIdOffsetMapper.class);
        TABLE_MAPPER_MAP.put(TableType.ASYNC_TASK, AsyncTaskMapper.class);
        TABLE_MAPPER_MAP.put(TableType.JOB_TOKEN, JobTokenMapper.class);
    }

    public static SqlSessionFactory getSqlSessionFactory(DataSource dataSource, String tableName, TableType type) {
        return Singletons.getInstance(type + "-session-factory", SqlSessionFactory.class, clz -> {
            log.info("Start to build SqlSessionFactory");
            TransactionFactory transactionFactory = new SpringManagedTransactionFactory();
            Environment environment = new Environment(type.name(), transactionFactory, dataSource);
            Configuration configuration = new Configuration(environment);
            configuration.setUseGeneratedKeys(true);
            configuration.setJdbcTypeForNull(JdbcType.NULL);
            configuration.addMapper(TABLE_MAPPER_MAP.get(type));
            createTableIfNotExist((BasicDataSource) dataSource, SQL_TEMPLATE_MAP.get(type), tableName);
            return new SqlSessionFactoryBuilder().build(configuration);
        });
    }

    private static void createTableIfNotExist(BasicDataSource dataSource, String sqlTemplate, String tableName)
            throws IOException, SQLException {
        if (JdbcUtil.isTableExists(dataSource.getConnection(), tableName)) {
            log.info("{} already existed in database", tableName);
            return;
        }

        Properties properties = JdbcUtil.getProperties(dataSource);
        String createTableStmt = String.format(Locale.ROOT, properties.getProperty(sqlTemplate), tableName);
        try (Connection connection = dataSource.getConnection()) {
            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            log.debug("start to create table({})", tableName);
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(createTableStmt.getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            log.debug("create table finished");
        }

        if (!JdbcUtil.isTableExists(dataSource.getConnection(), tableName)) {
            log.debug("failed to create table({})", tableName);
            throw new IllegalStateException(String.format(Locale.ROOT, "create table(%s) failed", tableName));
        } else {
            log.debug("table({}) already exists.", tableName);
        }
    }

    public enum TableType {
        FAVORITE_RULE, MODEL_FAVORITE_RULE, QUERY_HISTORY_OFFSET, ASYNC_TASK, JOB_TOKEN
    }
}
