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
package org.apache.kylin.job.dao;

import static org.apache.kylin.job.execution.JobTypeEnum.Category.CRON;
import static org.apache.kylin.job.execution.JobTypeEnum.Category.INTERNAL;
import static org.apache.kylin.job.execution.JobTypeEnum.Category.SNAPSHOT;
import static org.apache.kylin.job.util.JobInfoUtil.JOB_SERIALIZER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.domain.JobLock;
import org.apache.kylin.job.exception.ExecuteRuntimeException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.mapper.JobInfoMapper;
import org.apache.kylin.job.mapper.JobLockMapper;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.job.util.JobInfoUtil;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Component;

import lombok.Setter;
import lombok.val;

@Component
public class JobInfoDao {

    private static final Logger logger = LoggerFactory.getLogger(JobInfoDao.class);

    @Autowired(required = false)
    @Setter
    private JobInfoMapper jobInfoMapper;

    @Autowired(required = false)
    @Setter
    private JobLockMapper jobLockMapper;

    public List<JobInfo> getJobInfoListByFilter(final JobMapperFilter jobMapperFilter) {
        List<JobInfo> jobInfoList = jobInfoMapper.selectByJobFilter(jobMapperFilter);
        return jobInfoList;
    }

    public long countByFilter(JobMapperFilter jobMapperFilter) {
        return jobInfoMapper.countByJobFilter(jobMapperFilter);
    }

    public List<ExecutablePO> getJobs(String project) {
        JobMapperFilter filter = new JobMapperFilter();
        filter.setProject(project);
        return jobInfoMapper.selectByJobFilter(filter).stream().map(JobInfoUtil::deserializeExecutablePO)
                .collect(Collectors.toList());
    }

    public List<ExecutablePO> getJobs(String project, long timeStart, long timeEndExclusive) {
        return getJobs(project).stream()
                .filter(x -> x.getLastModified() >= timeStart && x.getLastModified() < timeEndExclusive)
                .collect(Collectors.toList());
    }

    public ExecutablePO addJob(ExecutablePO executablePO) {
        if (getExecutablePOByUuid(executablePO.getUuid()) != null) {
            throw new IllegalArgumentException("job id:" + executablePO.getUuid() + " already exists");
        }
        executablePO.setLastModified(System.currentTimeMillis());
        jobInfoMapper.insertJobInfoSelective(constructJobInfo(executablePO, 1));
        return executablePO;
    }

    public void updateJob(String uuid, Predicate<ExecutablePO> updater) {

        if (!KylinConfig.getInstanceFromEnv().isUTEnv() && !JdbcUtil.isInExistingTx()) {
            logger.warn("Job is updated without explicitly opening a transaction.");
        }

        JobInfo jobInfo = jobInfoMapper.selectByJobId(uuid);
        Preconditions.checkNotNull(jobInfo);
        val job = JobInfoUtil.deserializeExecutablePO(jobInfo);
        Preconditions.checkNotNull(job);
        val copyForWrite = JsonUtil.copyBySerialization(job, JOB_SERIALIZER, null);
        copyForWrite.setProject(job.getProject());
        if (updater.test(copyForWrite)) {
            copyForWrite.setLastModified(System.currentTimeMillis());
            int updateAffect = jobInfoMapper.updateByJobIdSelective(constructJobInfo(copyForWrite, jobInfo.getMvcc()));
            if (updateAffect == 0) {
                String errorMeg = String.format(Locale.ROOT, "job_info update fail for mvcc, job_id = %1s, mvcc = %2d",
                        job.getId(), jobInfo.getMvcc());
                logger.warn(errorMeg);
                throw new OptimisticLockingFailureException(errorMeg);
            }
        }
    }

    public ExecutablePO getExecutablePOByUuid(String uuid) {
        JobInfo jobInfo = jobInfoMapper.selectByJobId(uuid);
        if (null != jobInfo) {
            return JobInfoUtil.deserializeExecutablePO(jobInfo);
        }
        return null;
    }

    public List<ExecutablePO> getExecutablePoByStatus(String project, List<String> jobIds,
            List<ExecutableState> filterStatuses) {
        JobMapperFilter jobMapperFilter = new JobMapperFilter();
        jobMapperFilter.setProject(project);
        jobMapperFilter.setStatuses(filterStatuses);
        jobMapperFilter.setJobIds(jobIds);
        List<JobInfo> jobInfoList = jobInfoMapper.selectByJobFilter(jobMapperFilter);
        if (CollectionUtils.isEmpty(jobInfoList)) {
            return new ArrayList<>();
        }
        return jobInfoList.stream().map(jobInfo -> JobInfoUtil.deserializeExecutablePO(jobInfo))
                .collect(Collectors.toList());
    }

    public void dropJob(String jobId) {
        jobInfoMapper.deleteByJobId(jobId);
    }

    public void dropJobByIdList(List<String> jobIdList) {
        jobInfoMapper.deleteByJobIdList(Arrays.stream(ExecutableState.getFinalStates())
                .map(executableState -> executableState.name()).collect(Collectors.toList()), jobIdList);
    }

    public void dropAllJobs() {
        jobInfoMapper.deleteAllJob();
    }

    // visible for UT
    public JobInfo constructJobInfo(ExecutablePO executablePO, long mvcc) {
        JobInfo jobInfo = new JobInfo();
        jobInfo.setJobId(executablePO.getId());
        jobInfo.setJobType(executablePO.getJobType().name());
        ExecutableState executableState = ExecutableState.valueOf(executablePO.getOutput().getStatus());
        jobInfo.setJobStatus(executableState.name());
        jobInfo.setProject(executablePO.getProject());
        jobInfo.setPriority(executablePO.getPriority());

        String subject = null;
        switch (executablePO.getJobType().getCategory()) {
        case INTERNAL:
        case SNAPSHOT:
            subject = executablePO.getParams().get(NBatchConstants.P_TABLE_NAME);
            break;
        case CRON:
            subject = executablePO.getJobType().name();
            break;
        default:
            if (JobTypeEnum.TABLE_SAMPLING == executablePO.getJobType()
                    || JobTypeEnum.LAYOUT_DATA_OPTIMIZE == executablePO.getJobType()) {
                subject = executablePO.getTargetModel();
            } else if (null != executablePO.getTargetModel() && null != executablePO.getProject()) {
                if (executablePO.getParams().containsKey(NBatchConstants.P_MODEL_NAME)) {
                    subject = executablePO.getParams().get(NBatchConstants.P_MODEL_NAME);
                } else {
                    NDataModelManager nDataModelManager = NDataModelManager
                            .getInstance(KylinConfig.getInstanceFromEnv(), executablePO.getProject());
                    NDataModel nDataModel = nDataModelManager.getDataModelDesc(executablePO.getTargetModel());
                    if (null == nDataModel) {
                        logger.warn("Can not get modelName by modelId {}, project {}", executablePO.getTargetModel(),
                                executablePO.getProject());
                    } else {
                        subject = nDataModel.getAlias();
                    }
                }
            }
        }

        jobInfo.setSubject(subject);
        jobInfo.setModelId(executablePO.getTargetModel());
        jobInfo.setCreateTime(executablePO.getCreateTime());
        jobInfo.setUpdateTime(executablePO.getLastModified());
        jobInfo.setJobContent(checkAndCompressJobContent(JobInfoUtil.serializeExecutablePO(executablePO)));

        ExecutableManager executableManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(),
                executablePO.getProject());
        AbstractExecutable executable = executableManager.fromPO(executablePO);
        long duration = executable.getDurationFromStepOrStageDurationSum(executablePO);
        jobInfo.setJobDurationMillis(duration);
        jobInfo.setMvcc(mvcc);
        return jobInfo;
    }

    private byte[] checkAndCompressJobContent(byte[] jobContent) {
        try {
            return CompressionUtils.compress(jobContent);
        } catch (IOException e) {
            throw new ExecuteRuntimeException("Compress job content failed.", e);
        }
    }

    public void deleteJobsByProject(String project) {
        int count = jobInfoMapper.deleteByProject(project);
        logger.info("delete {} jobs for project {}", count, project);
    }

    public List<JobLock> fetchAllJobLock() {
        return jobLockMapper.fetchAll();
    }

    public void restoreJobInfo(List<JobInfo> jobInfos, String project, boolean afterTruncate) {
        JobContextUtil.withTxAndRetry(() -> {
            if (afterTruncate) {
                jobInfoMapper.deleteByProject(project);
            }
            for (JobInfo jobInfo : jobInfos) {
                jobInfo.setJobContent(checkAndCompressJobContent(jobInfo.getJobContent()));
                JobInfo currentJobInfo = jobInfoMapper.selectByJobId(jobInfo.getJobId());
                if (currentJobInfo == null) {
                    jobInfoMapper.insert(jobInfo);
                } else {
                    jobInfo.setMvcc(currentJobInfo.getMvcc());
                    jobInfoMapper.updateByJobIdSelective(jobInfo);
                }
            }
            return null;
        });
    }

    public Long getEarliestJobCreateTime() {
        return getEarliestJobCreateTime(null);

    }

    public Long getEarliestJobCreateTime(String project) {
        return jobInfoMapper.getEarliestCreateTime(project);
    }
}
