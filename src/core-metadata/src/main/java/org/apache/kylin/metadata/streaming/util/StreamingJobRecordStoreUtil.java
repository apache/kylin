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

package org.apache.kylin.metadata.streaming.util;

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
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.logging.LogOutputStream;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.metadata.streaming.StreamingJobRecordManager;
import org.apache.kylin.metadata.streaming.StreamingJobRecordMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingJobRecordStoreUtil {

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    private static final String CREATE_STREAMING_JOB_RECORD_TABLE = "create.streamingjobrecord.store.table";
    private static final String CREATE_STREAMING_JOB_RECORD_INDEX1 = "create.streamingjobrecord.store.tableindex1";
    private static final String CREATE_STREAMING_JOB_RECORD_INDEX2 = "create.streamingjobrecord.store.tableindex2";

    private StreamingJobRecordStoreUtil() {
    }

    public static SqlSessionFactory getSqlSessionFactory(DataSource dataSource, String tableName) {
        return Singletons.getInstance("streaming-job-record-session-factory", SqlSessionFactory.class, clz -> {
            TransactionFactory transactionFactory = new JdbcTransactionFactory();
            Environment environment = new Environment("streaming job record", transactionFactory, dataSource);
            Configuration configuration = new Configuration(environment);
            configuration.setUseGeneratedKeys(true);
            configuration.setJdbcTypeForNull(JdbcType.NULL);
            configuration.addMapper(StreamingJobRecordMapper.class);
            createStreamingJobTableIfNotExist((BasicDataSource) dataSource, tableName);
            return new SqlSessionFactoryBuilder().build(configuration);
        });
    }

    private static void createStreamingJobTableIfNotExist(BasicDataSource dataSource, String tableName)
            throws SQLException, IOException {
        try (Connection connection = dataSource.getConnection()) {
            if (JdbcUtil.isTableExists(connection, tableName)) {
                return;
            }
        } catch (Exception e) {
            log.error("Fail to know if table {} exists", tableName, e);
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            Properties properties = JdbcUtil.getProperties(dataSource);
            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(
                    String.format(Locale.ROOT, properties.getProperty(CREATE_STREAMING_JOB_RECORD_TABLE), tableName)
                            .getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(
                    String.format(Locale.ROOT, properties.getProperty(CREATE_STREAMING_JOB_RECORD_INDEX1), tableName,
                            tableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(
                    String.format(Locale.ROOT, properties.getProperty(CREATE_STREAMING_JOB_RECORD_INDEX2), tableName,
                            tableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
        }
    }

    public static void cleanStreamingJobRecord() {
        String oldThreadName = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName("streamingJobRecordCleanWorker");
            StreamingJobRecordManager.getInstance().deleteIfRetainTimeReached();
        } finally {
            Thread.currentThread().setName(oldThreadName);
        }
    }

}
