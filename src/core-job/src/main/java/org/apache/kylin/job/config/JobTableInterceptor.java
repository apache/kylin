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

import java.util.List;
import java.util.Objects;

import org.apache.ibatis.binding.MapperMethod;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.condition.JobModeCondition;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.domain.JobLock;
import org.apache.kylin.job.mapper.JobInfoMapper;
import org.apache.kylin.job.mapper.JobLockMapper;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

@Conditional(JobModeCondition.class)
@Component
@Intercepts({
        @Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class,
                RowBounds.class, ResultHandler.class, CacheKey.class, BoundSql.class }),
        @Signature(type = Executor.class, method = "query", args = { MappedStatement.class, Object.class,
                RowBounds.class, ResultHandler.class }),
        @Signature(type = Executor.class, method = "queryCursor", args = { MappedStatement.class, Object.class,
                RowBounds.class }),
        @Signature(type = Executor.class, method = "update", args = { MappedStatement.class, Object.class }) })
public class JobTableInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(JobTableInterceptor.class);

    List<String> controlledMappers = Lists.newArrayList(JobInfoMapper.class.getName(), JobLockMapper.class.getName());

    @Autowired
    private JobMybatisConfig jobMybatisConfig;

    @Override
    public Object intercept(Invocation invocation) throws Throwable {

        Object[] args = invocation.getArgs();

        String jobInfoTableName = jobMybatisConfig.getJobInfoTableName();
        String jobLockTableName = jobMybatisConfig.getJobLockTableName();
        if (jobInfoTableName == null || jobLockTableName == null) {
            logger.info("mybatis table not init, skip");
            return null;
        }

        MappedStatement mappedStatement = (MappedStatement) args[0];
        Objects.requireNonNull(mappedStatement);
        String mappedStatementId = mappedStatement.getId();
        Objects.requireNonNull(mappedStatementId);

        if (!isControlledMapper(mappedStatementId)) {
            logger.info("not controlled mapper find, mappedStatementId = {}, ignore", mappedStatementId);
            return invocation.proceed();
        }
        String database = null;
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            database = "h2";
        } else {
            database = jobMybatisConfig.getDatabase();
        }
        if (args[1] == null) {
            MapperMethod.ParamMap map = new MapperMethod.ParamMap();
            map.put("jobLockTable", jobLockTableName);
            map.put("jobInfoTable", jobInfoTableName);
            map.put("database", database);
            invocation.getArgs()[1] = map;
        } else if (args[1] instanceof MapperMethod.ParamMap) {
            MapperMethod.ParamMap map = (MapperMethod.ParamMap) args[1];
            map.put("jobLockTable", jobLockTableName);
            map.put("jobInfoTable", jobInfoTableName);
            map.put("database", database);
        } else if (args[1] instanceof JobMapperFilter) {
            JobMapperFilter mapperFilter = (JobMapperFilter) args[1];
            mapperFilter.setJobInfoTable(jobInfoTableName);
        } else if (args[1] instanceof JobInfo) {
            JobInfo jobInfo = (JobInfo) args[1];
            jobInfo.setJobInfoTable(jobInfoTableName);
        } else if (args[1] instanceof JobLock) {
            JobLock jobLock = (JobLock) args[1];
            jobLock.setJobLockTable(jobLockTableName);
            jobLock.setDatabase(database);
        } else {
            logger.error("miss type of param {}", args[1].getClass());
        }

        return invocation.proceed();
    }

    private boolean isControlledMapper(String mappedStatementId) {
        for (String controlledMapper : controlledMappers) {
            if (mappedStatementId.startsWith(controlledMapper)) {
                // ok, find controlled mapper, mappedStatementId like "org.apache.kylin.job.mapper.JobLockMapper.findCount"
                return true;
            }
        }
        return false;
    }

    public void setJobMybatisConfig(JobMybatisConfig jobMybatisConfig) {
        this.jobMybatisConfig = jobMybatisConfig;
    }
}
