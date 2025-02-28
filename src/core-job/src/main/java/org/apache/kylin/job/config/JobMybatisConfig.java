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

package org.apache.kylin.job.config;

import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.isTableExists;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.metadata.FileSystemMetadataStore;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.condition.JobModeCondition;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Conditional;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.security.util.InMemoryResource;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Conditional(JobModeCondition.class)
public class JobMybatisConfig implements InitializingBean {

    private static final String CREATE_JOB_INFO_TABLE = "create.job.info.table";

    private static final String CREATE_JOB_LOCK_TABLE = "create.job.lock.table";

    private static final String CREATE_JOB_INFO_INDEX_1 = "create.job.info.index1";

    private static final String CREATE_JOB_INFO_INDEX_2 = "create.job.info.index2";

    public static final String JOB_INFO_SUFFIX = "_job_info_v2";

    public static final String JOB_LOCK_SUFFIX = "_job_lock_v2";

    private DataSource dataSource;

    private String database;

    private String jobInfoTableName = "job_info";
    private String jobLockTableName = "job_lock";

    public String getDatabase() {
        return database;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        setupJobTables();
    }

    public String getJobInfoTableName() {
        return jobInfoTableName;
    }

    public String getJobLockTableName() {
        return jobLockTableName;
    }

    public void setupJobTables() throws Exception {
        val url = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        if (null == dataSource) {
            val props = JdbcUtil.datasourceParameters(url);
            dataSource = JdbcDataSource.getDataSource(props);
        }
        String keIdentified = url.getIdentifier();
        if (FileSystemMetadataStore.FILE_SCHEME.equals(url.getScheme())) {
            log.info("metadata from file");
            keIdentified = "file";
            if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
                String uuid = RandomUtil.randomUUIDStr().replace("-", "_");
                keIdentified = "UT_" + uuid;
            }
        }
        jobInfoTableName = keIdentified + JOB_INFO_SUFFIX;
        jobLockTableName = keIdentified + JOB_LOCK_SUFFIX;
        database = Database.MYSQL.databaseId;

        Method[] declaredMethods = ReflectionUtils.getDeclaredMethods(dataSource.getClass());
        List<Method> getDriverClassNameMethodList = Arrays.stream(declaredMethods)
                .filter(method -> "getDriverClassName".equals(method.getName())).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(getDriverClassNameMethodList)) {
            log.warn("can not get method of [getDriverClassName] from datasource type = {}", dataSource.getClass());
        } else {
            Method methodOfgetDriverClassName = getDriverClassNameMethodList.get(0);
            String driverClassName = (String) ReflectionUtils.invokeMethod(methodOfgetDriverClassName, dataSource);
            if (driverClassName.startsWith("com.mysql")) {
                // mysql, is default
                log.info("driver class name = {}, is mysql", driverClassName);
            } else if (driverClassName.startsWith("org.postgresql")) {
                log.info("driver class name = {}, is postgresql", driverClassName);
                database = Database.POSTGRESQL.databaseId;
            } else if (driverClassName.equals("org.h2.Driver")) {
                log.info("driver class name = {}, is H2 ", driverClassName);
                database = Database.H2.databaseId;
            } else {
                String errorMsg = String.format(Locale.ROOT, "driver class name = %s, should add support",
                        driverClassName);
                log.error(errorMsg);
                throw new KylinRuntimeException(errorMsg);
            }
        }

        Properties sqlProperties = JdbcUtil.getProperties((BasicDataSource) dataSource);
        checkAndCreateTable(sqlProperties);
        checkAndCreateTableIndex(sqlProperties);
    }

    private void checkAndCreateTable(Properties sqlProperties) {
        try {
            if (!isTableExists(dataSource.getConnection(), jobInfoTableName)) {
                String jobInfoTableSql = String.format(Locale.ROOT, sqlProperties.getProperty(CREATE_JOB_INFO_TABLE),
                        jobInfoTableName);
                executeSql(jobInfoTableSql);
            }

            if (!isTableExists(dataSource.getConnection(), jobLockTableName)) {
                String jobLockTableSql = String.format(Locale.ROOT, sqlProperties.getProperty(CREATE_JOB_LOCK_TABLE),
                        jobLockTableName);
                executeSql(jobLockTableSql);
            }

        } catch (SQLException e) {
            throw new KylinRuntimeException(e);
        }
    }

    private void checkAndCreateTableIndex(Properties sqlProperties) {
        try {
            String jobInfoIndex1 = jobInfoTableName + "_ix";
            if (!JdbcUtil.isIndexExists(dataSource.getConnection(), jobInfoTableName, jobInfoIndex1)) {
                String jobInfoIndex1Sql = String.format(Locale.ROOT, sqlProperties.getProperty(CREATE_JOB_INFO_INDEX_1),
                        jobInfoIndex1, jobInfoTableName);
                executeSql(jobInfoIndex1Sql);
            }

            String jobInfoIndex2 = jobInfoTableName + "_project_model_id_ix";
            if (!JdbcUtil.isIndexExists(dataSource.getConnection(), jobInfoTableName, jobInfoIndex2)) {
                String jobInfoIndex2Sql = String.format(Locale.ROOT, sqlProperties.getProperty(CREATE_JOB_INFO_INDEX_2),
                        jobInfoIndex2, jobInfoTableName);
                executeSql(jobInfoIndex2Sql);
            }
        } catch (Exception e) {
            log.warn("Check and create index for job info table failed.", e);
        }
    }

    private void executeSql(String sql) {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new InMemoryResource(sql));
        populator.setContinueOnError(false);
        DatabasePopulatorUtils.execute(populator, dataSource);
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    enum Database {
        MYSQL("mysql"), POSTGRESQL("postgresql"), H2("h2");

        String databaseId;

        Database(String databaseId) {
            this.databaseId = databaseId;
        }

    }
}
