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

package org.apache.kylin.job;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.condition.JobModeCondition;
import org.apache.kylin.job.core.AbstractJobExecutable;
import org.apache.kylin.job.core.lock.JdbcLockClient;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.mapper.JobInfoMapper;
import org.apache.kylin.job.mapper.JobLockMapper;
import org.apache.kylin.job.runners.JobCheckRunner;
import org.apache.kylin.job.runners.JobCheckUtil;
import org.apache.kylin.job.runners.QuotaStorageCheckRunner;
import org.apache.kylin.job.scheduler.JdbcJobScheduler;
import org.apache.kylin.job.scheduler.ResourceAcquirer;
import org.apache.kylin.job.scheduler.SharedFileProgressReporter;
import org.apache.kylin.rest.ISmartApplicationListenerForSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Component;

@Component
@DependsOn({ "springContext", "jobMybatisConfig" })
@Conditional(JobModeCondition.class)
public class JobContext implements InitializingBean, DisposableBean, ISmartApplicationListenerForSystem {

    private static final Logger logger = LoggerFactory.getLogger(LogConstant.BUILD_CATEGORY);

    // resource block
    // progress report
    // status control

    private String serverNode;

    private KylinConfig kylinConfig;

    @Resource
    private JobInfoMapper jobInfoMapper;

    @Resource
    private JobLockMapper jobLockMapper;

    @Autowired
    private DataSourceTransactionManager transactionManager;

    private Map<String, Boolean> projectReachQuotaLimitMap;

    private ResourceAcquirer resourceAcquirer;

    private SharedFileProgressReporter progressReporter;

    private JdbcLockClient lockClient;

    private JdbcJobScheduler jobScheduler;

    @Override
    public void destroy() throws Exception {
        if (Objects.nonNull(resourceAcquirer)) {
            resourceAcquirer.destroy();
        }

        if (Objects.nonNull(progressReporter)) {
            progressReporter.destroy();
        }

        if (Objects.nonNull(jobScheduler)) {
            jobScheduler.destroy();
        }

        if (Objects.nonNull(lockClient)) {
            lockClient.destroy();
        }

        JobCheckUtil.stopJobCheckScheduler();

    }

    // for ut only
    @VisibleForTesting
    public void setJobInfoMapper(JobInfoMapper jobInfoMapper) {
        this.jobInfoMapper = jobInfoMapper;
    }

    // for ut only
    @VisibleForTesting
    public void setJobLockMapper(JobLockMapper jobLockMapper) {
        this.jobLockMapper = jobLockMapper;
    }

    // for ut only
    @VisibleForTesting
    public void setTransactionManager(DataSourceTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }

    // for ut only
    @VisibleForTesting
    public void init() {
        serverNode = AddressUtil.getLocalInstance();

        kylinConfig = KylinConfig.getInstanceFromEnv();

        projectReachQuotaLimitMap = Maps.newConcurrentMap();
        QuotaStorageCheckRunner quotaStorageCheckRunner = new QuotaStorageCheckRunner(this);
        JobCheckUtil.startQuotaStorageCheckRunner(quotaStorageCheckRunner);

        if (kylinConfig.isJobNode() || kylinConfig.isDataLoadingNode() || kylinConfig.isUTEnv()) {

            resourceAcquirer = new ResourceAcquirer(kylinConfig);
            resourceAcquirer.start();

            progressReporter = new SharedFileProgressReporter(kylinConfig);
            progressReporter.start();

            lockClient = new JdbcLockClient(this);
            lockClient.start();

            jobScheduler = new JdbcJobScheduler(this);
            jobScheduler.start();

            JobCheckRunner jobCheckRunner = new JobCheckRunner(this);
            JobCheckUtil.startJobCheckRunner(jobCheckRunner);
        }
    }

    public String getServerNode() {
        return serverNode;
    }

    public KylinConfig getKylinConfig() {
        return kylinConfig;
    }

    @VisibleForTesting
    public void setKylinConfig(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }

    public DataSourceTransactionManager getTransactionManager() {
        return transactionManager;
    }

    public JobInfoMapper getJobInfoMapper() {
        return jobInfoMapper;
    }

    public JobLockMapper getJobLockMapper() {
        return jobLockMapper;
    }

    public ResourceAcquirer getResourceAcquirer() {
        return resourceAcquirer;
    }

    public SharedFileProgressReporter getProgressReporter() {
        return progressReporter;
    }

    public JdbcLockClient getLockClient() {
        return lockClient;
    }

    public JdbcJobScheduler getJobScheduler() {
        return jobScheduler;
    }

    // only for UT
    public void setJobScheduler(JdbcJobScheduler jobScheduler) {
        this.jobScheduler = jobScheduler;
    }

    public void setProjectReachQuotaLimit(String project, Boolean reachQuotaLimit) {
        projectReachQuotaLimitMap.put(project, reachQuotaLimit);
    }

    public boolean isProjectReachQuotaLimit(String project) {
        if (!projectReachQuotaLimitMap.containsKey(project)) {
            return false;
        }
        return projectReachQuotaLimitMap.get(project);
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        if (event instanceof ContextClosedEvent) {
            if (null == jobScheduler || !jobScheduler.hasRunningJob()) {
                return;
            }
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
                logger.info("Stop kyligence node, kill spark application for cluster mode");
            }
            List<AbstractJobExecutable> runningJobs = jobScheduler.getRunningJob().values().stream().map(Pair::getFirst)
                    .collect(Collectors.toList());
            runningJobs.forEach(jobExecutable -> {
                ExecutableManager executableManager = ExecutableManager.getInstance(kylinConfig,
                        jobExecutable.getProject());
                executableManager.cancelJobSubTasks((AbstractExecutable) jobExecutable);
            });
        }
    }

    @Override
    public int getOrder() {
        return HIGHEST_PRECEDENCE;
    }
}
