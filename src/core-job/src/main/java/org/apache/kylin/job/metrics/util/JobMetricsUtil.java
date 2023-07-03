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

package org.apache.kylin.job.metrics.util;

import lombok.extern.slf4j.Slf4j;
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
import org.apache.kylin.job.metrics.JobMetricsMapper;
import org.apache.kylin.job.metrics.JobMetricsStatisticsMapper;
import org.apache.kylin.metadata.query.util.JdbcTableUtil;

import javax.sql.DataSource;
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

@Slf4j
public class JobMetricsUtil {

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    private static final String CREATE_JOB_METRICS_TABLE = "create.jobmetrics.store.table";

    private static final String CREATE_JOB_METRICS_INDEX1 = "create.jobmetrics.store.tableindex1";

    private static final String CREATE_JOB_METRICS_INDEX2 = "create.jobmetrics.store.tableindex2";

    private static final String CREATE_JOB_METRICS_INDEX3 = "create.jobmetrics.store.tableindex3";

    private static final String CREATE_JOB_METRICS_INDEX4 = "create.jobmetrics.store.tableindex4";

    private static final String CREATE_JOB_METRICS_INDEX5 = "create.jobmetrics.store.tableindex5";

    private JobMetricsUtil() {
    }

    public static SqlSessionFactory getSqlSessionFactory(DataSource dataSource, String jobMetricsTableName) {
        return Singletons.getInstance("job-metrics-sql-session-factory", SqlSessionFactory.class, clz -> {
            TransactionFactory transactionFactory = new JdbcTransactionFactory();
            Environment environment = new Environment("job metrics", transactionFactory, dataSource);
            Configuration configuration = new Configuration(environment);
            configuration.setUseGeneratedKeys(true);
            configuration.setJdbcTypeForNull(JdbcType.NULL);
            configuration.addMapper(JobMetricsMapper.class);
            configuration.addMapper(JobMetricsStatisticsMapper.class);
            createJobMetricsIfNotExist((BasicDataSource) dataSource, jobMetricsTableName);
            return new SqlSessionFactoryBuilder().build(configuration);
        });
    }

    private static void createJobMetricsIfNotExist(BasicDataSource dataSource, String jobMetricsTableName)
            throws SQLException, IOException {
        if (JdbcTableUtil.isTableExist(dataSource, jobMetricsTableName)) {
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            Properties properties = JdbcUtil.getProperties(dataSource);

            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_JOB_METRICS_TABLE), jobMetricsTableName)
                            .getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_JOB_METRICS_INDEX1), jobMetricsTableName,
                            jobMetricsTableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_JOB_METRICS_INDEX2), jobMetricsTableName,
                            jobMetricsTableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_JOB_METRICS_INDEX3), jobMetricsTableName,
                            jobMetricsTableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_JOB_METRICS_INDEX4), jobMetricsTableName,
                            jobMetricsTableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(//
                    String.format(Locale.ROOT, properties.getProperty(CREATE_JOB_METRICS_INDEX5), jobMetricsTableName,
                            jobMetricsTableName).getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
        }
    }
}
