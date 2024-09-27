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

import static org.apache.kylin.job.config.JobMybatisConfig.JOB_INFO_SUFFIX;
import static org.apache.kylin.job.config.JobMybatisConfig.JOB_LOCK_SUFFIX;

import java.util.Properties;

import javax.sql.DataSource;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class JobMybatisConfigTest extends NLocalFileMetadataTestCase {

    private JdbcTemplate jdbcTemplate;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        jdbcTemplate = JdbcUtil.getJdbcTemplate(getTestConfig());
    }

    @After
    public void tearDown() {
        if (jdbcTemplate != null) {
            jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        }
        cleanupTestMetadata();
    }

    private DataSource getDataSource(KylinConfig config) throws Exception {
        StorageURL url = config.getMetadataUrl();
        Properties props = JdbcUtil.datasourceParameters(url);
        return JdbcDataSource.getDataSource(props);
    }

    @Test
    public void testSetUpMySqlTable() throws Exception {
        KylinConfig config = getTestConfig();

        config.setMetadataUrl(
                "test@jdbc,driverClassName=com.mysql.jdbc.Driver,url=jdbc:mysql://test:3306/t?username=t,password=t");
        DataSource dataSource = getDataSource(config);

        JobMybatisConfig jobMybatisConfig = new JobMybatisConfig();
        jobMybatisConfig.setDataSource(dataSource);
        try {
            jobMybatisConfig.afterPropertiesSet();
        } catch (KylinRuntimeException e) {
            // do nothing
        }
        Assert.assertEquals(JobMybatisConfig.Database.MYSQL.databaseId, jobMybatisConfig.getDatabase());
        Assert.assertEquals("test" + JOB_INFO_SUFFIX, jobMybatisConfig.getJobInfoTableName());
        Assert.assertEquals("test" + JOB_LOCK_SUFFIX, jobMybatisConfig.getJobLockTableName());
    }

    @Test
    public void testSetUpPgTable() throws Exception {
        KylinConfig config = getTestConfig();

        config.setMetadataUrl(
                "test@jdbc,driverClassName=org.postgresql.Driver,url=jdbc:postgresql://h:5432/t,username=t,password=t");
        DataSource dataSource = getDataSource(config);

        JobMybatisConfig jobMybatisConfig = new JobMybatisConfig();
        jobMybatisConfig.setDataSource(dataSource);
        try {
            jobMybatisConfig.afterPropertiesSet();
        } catch (KylinRuntimeException e) {
            // do nothing
        }
        Assert.assertEquals(JobMybatisConfig.Database.POSTGRESQL.databaseId, jobMybatisConfig.getDatabase());
        Assert.assertEquals("test" + JOB_INFO_SUFFIX, jobMybatisConfig.getJobInfoTableName());
        Assert.assertEquals("test" + JOB_LOCK_SUFFIX, jobMybatisConfig.getJobLockTableName());
    }

    @Test
    public void testSetUpFileTable() throws Exception {
        KylinConfig config = getTestConfig();

        DataSource dataSource = getDataSource(config);

        config.setProperty("kylin.env", "DEV");

        JobMybatisConfig jobMybatisConfig = new JobMybatisConfig();
        jobMybatisConfig.setDataSource(dataSource);
        try {
            jobMybatisConfig.afterPropertiesSet();
        } catch (KylinRuntimeException e) {
            // do nothing
        }
        Assert.assertEquals("file" + JOB_INFO_SUFFIX, jobMybatisConfig.getJobInfoTableName());
        Assert.assertEquals("file" + JOB_LOCK_SUFFIX, jobMybatisConfig.getJobLockTableName());
        config.setProperty("kylin.env", "UT");
    }

    @Test
    public void testTableExists() throws Exception {
        KylinConfig config = getTestConfig();

        DataSource dataSource = getDataSource(config);
        JobMybatisConfig jobMybatisConfig = new JobMybatisConfig();
        jobMybatisConfig.setDataSource(dataSource);
        config.setProperty("kylin.env", "DEV");

        jobMybatisConfig.afterPropertiesSet();

        Assert.assertTrue(JdbcUtil.isTableExists(dataSource.getConnection(), "file" + JOB_INFO_SUFFIX));
        Assert.assertTrue(JdbcUtil.isTableExists(dataSource.getConnection(), "file" + JOB_LOCK_SUFFIX));

        jobMybatisConfig.afterPropertiesSet();

        Assert.assertTrue(JdbcUtil.isTableExists(dataSource.getConnection(), "file" + JOB_INFO_SUFFIX));
        Assert.assertTrue(JdbcUtil.isTableExists(dataSource.getConnection(), "file" + JOB_LOCK_SUFFIX));
    }

    @Test
    public void testErrorMetadataUrl() throws Exception {
        KylinConfig config = getTestConfig();

        config.setMetadataUrl(
                "test@jdbc,driverClassName=org.pg.Driver,url=jdbc:postgresql://localhost:5432/t,username=t,password=t");
        DataSource dataSource = getDataSource(config);

        JobMybatisConfig jobMybatisConfig = new JobMybatisConfig();
        jobMybatisConfig.setDataSource(dataSource);
        try {
            jobMybatisConfig.afterPropertiesSet();
            Assert.fail("Expected an KylinRuntimeException to be thrown");
        } catch (KylinRuntimeException e) {
            Assert.assertEquals("driver class name = org.pg.Driver, should add support", e.getMessage());
        }
    }
}
