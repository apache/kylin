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

package org.apache.kylin.metadata.job;

import static org.apache.kylin.util.MetadataStoreUtil.TableType.JOB_TOKEN;
import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.select;

import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.util.MetadataStoreUtil;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.SqlBuilder;
import org.mybatis.dynamic.sql.delete.DeleteModel;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.util.DigestUtils;

import lombok.Getter;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobTokenManager {

    public static final String JOB_TOKEN_TABLE = "_job_token";

    private final JobTokenTable table;
    @VisibleForTesting
    @Getter
    private final SqlSessionTemplate sqlSessionTemplate;
    @Getter
    private final DataSourceTransactionManager transactionManager;

    public static JobTokenManager getInstance(KylinConfig config) {
        return config.getManager(JobTokenManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static JobTokenManager newInstance(KylinConfig config) {
        try {
            String cls = JobTokenManager.class.getName();
            Class<? extends JobTokenManager> clz = ClassUtil.forName(cls, JobTokenManager.class);
            return clz.getConstructor(KylinConfig.class).newInstance(config);
        } catch (Exception e) {
            throw new KylinRuntimeException("Failed to init JobTokenManager from " + config, e);
        }
    }

    @SuppressWarnings("unused") // used by reflection
    public JobTokenManager(KylinConfig config) throws Exception {
        this(config, getJobTokenTable(config));
    }

    private JobTokenManager(KylinConfig config, String tableName) throws Exception {
        StorageURL url = KylinConfig.getInstanceFromEnv().isUTEnv() ? config.getQueryHistoryUrl()
                : config.getJDBCDistributedLockURL();
        Properties props = JdbcUtil.datasourceParameters(url);
        DataSource dataSource = JdbcDataSource.getDataSource(props);
        table = new JobTokenTable(tableName);
        transactionManager = new DataSourceTransactionManager(dataSource);
        sqlSessionTemplate = new SqlSessionTemplate(
                MetadataStoreUtil.getSqlSessionFactory(dataSource, table.tableNameAtRuntime(), JOB_TOKEN));
    }

    public String encrypt(String token) {
        return DigestUtils.md5DigestAsHex(token.getBytes(Charset.defaultCharset()));
    }

    private static String getJobTokenTable(KylinConfig config) {
        StorageURL url = config.getQueryHistoryUrl();
        String tablePrefix = config.isUTEnv() ? "test_job_token" : url.getIdentifier();
        return tablePrefix + JOB_TOKEN_TABLE;
    }

    public void save(JobTokenItem item) {
        // delete outdated token first.
        delete(item.getJobId());
        JobTokenMapper mapper = sqlSessionTemplate.getMapper(JobTokenMapper.class);
        InsertStatementProvider<JobTokenItem> insertStatement = getInsertProvider(item);
        int rows = mapper.insert(insertStatement);
        if (rows > 0) {
            log.debug("Insert one token({}/{}) into database.", item.getJobId(), item.getToken());
        } else {
            throw new KylinRuntimeException(String.format(Locale.ROOT, "Failed to insert token (jobId: %s, token: %s",
                    item.getJobId(), item.getToken()));
        }
    }

    public JobTokenItem query(String jobId) {
        JobTokenMapper mapper = sqlSessionTemplate.getMapper(JobTokenMapper.class);
        SelectStatementProvider statementProvider = getSelectByIdStatementProvider(jobId);
        return mapper.selectOne(statementProvider);
    }

    public void delete(String jobId) {
        long startTime = System.currentTimeMillis();
        try {
            JobTokenMapper mapper = sqlSessionTemplate.getMapper(JobTokenMapper.class);
            DeleteModel deleteStatement = SqlBuilder.deleteFrom(table).where(table.jobId, isEqualTo(jobId)).build();
            int rows = mapper.delete(deleteStatement.render(RenderingStrategies.MYBATIS3));
            if (rows > 0) {
                log.debug("Delete one token of jobId: {} takes {} ms.", jobId, System.currentTimeMillis() - startTime);
            }
        } catch (Exception e) {
            log.error("Fail to delete token of jobId:{}.", jobId, e);
        }
    }

    public InsertStatementProvider<JobTokenItem> getInsertProvider(JobTokenItem item) {
        var provider = SqlBuilder.insert(item).into(table);
        return provider.map(table.jobId).toProperty("jobId") //
                .map(table.token).toProperty("token") //
                .map(table.createTime).toProperty("createTime") //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    public SelectStatementProvider getSelectByIdStatementProvider(String jobId) {
        return select(BasicColumn.columnList(table.jobId, table.token, table.createTime)) //
                .from(table) //
                .where(table.jobId, isEqualTo(jobId)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }
}
