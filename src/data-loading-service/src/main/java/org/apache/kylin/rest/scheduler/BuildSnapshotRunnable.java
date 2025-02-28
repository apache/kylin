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

package org.apache.kylin.rest.scheduler;

import static org.apache.kylin.common.constant.Constants.BACKSLASH;
import static org.apache.kylin.common.constant.Constants.SNAPSHOT_JOB;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.job.execution.JobTypeEnum.SNAPSHOT_BUILD;
import static org.apache.kylin.job.execution.JobTypeEnum.SNAPSHOT_REFRESH;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpStatus;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.engine.spark.job.NSparkSnapshotJob;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BuildSnapshotRunnable extends AbstractSchedulerRunnable {
    private static final String BUILD_SNAPSHOT_ERROR_MESSAGE = "Project[%s] Snapshot[%s] buildSnapshot failed";

    @Override
    public void execute() {
        buildSnapshot();
    }

    public void buildSnapshot() {
        if (Boolean.TRUE.equals(needRefresh) || Boolean.TRUE.equals(checkSnapshotJobFile())) {
            try {
                val url = String.format(Locale.ROOT, "http://%s/kylin/api/snapshots/auto_refresh",
                        config.getServerAddress());
                val req = createRequestAndCheckRunningJob();
                log.debug("buildSnapshot request: {}", req);
                val httpHeaders = new HttpHeaders();
                httpHeaders.add(HttpHeaders.CONTENT_TYPE, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
                val exchange = restTemplate.exchange(url, HttpMethod.PUT,
                        new HttpEntity<>(JsonUtil.writeValueAsBytes(req), httpHeaders), String.class);
                val responseBody = Optional.ofNullable(exchange.getBody()).orElse("");
                val responseStatus = exchange.getStatusCodeValue();
                if (responseStatus != HttpStatus.SC_OK) {
                    throw new KylinRuntimeException(
                            String.format(Locale.ROOT, BUILD_SNAPSHOT_ERROR_MESSAGE, project, tableIdentity));
                }
                val response = JsonUtil.readValue(responseBody, new TypeReference<RestResponse<JobInfoResponse>>() {
                });
                if (!StringUtils.equals(response.getCode(), KylinException.CODE_SUCCESS)) {
                    throw new KylinRuntimeException(
                            String.format(Locale.ROOT, BUILD_SNAPSHOT_ERROR_MESSAGE, project, tableIdentity));
                }
                val jobId = response.getData().getJobs().get(0).getJobId();
                saveSnapshotJobFile(false, "", jobId);
                log.info("Project[{}}] Snapshot[{}] buildSnapshot API success, response [{}]", project, tableIdentity,
                        response.getData());
            } catch (Exception e) {
                saveSnapshotJobFile(true, e.getMessage(), "");
                log.error("Project[{}] Snapshot[{}] buildSnapshot failed", project, tableIdentity);
                throw new KylinRuntimeException(e.getMessage(), e);
            }
        }
    }

    public Boolean checkSnapshotJobFile() {
        val snapshotJob = readSnapshotJobFile();
        val buildError = Boolean.parseBoolean(snapshotJob.getOrDefault("build_error", String.valueOf(true)));
        if (buildError) {
            return true;
        }
        val jobId = snapshotJob.get("job_id");
        if (StringUtils.isBlank(jobId)) {
            return true;
        }
        return !checkAutoRefreshJobSuccessOrRunning(jobId);
    }

    public Boolean checkAutoRefreshJobSuccessOrRunning(String jobId) {
        try {
            val executableManager = ExecutableManager.getInstance(config, project);
            val autoRefreshJob = executableManager.getJob(jobId);
            if (null == autoRefreshJob) {
                return false;
            }
            return autoRefreshJob.getStatus() == ExecutableState.SUCCEED || autoRefreshJob.getStatus().isRunning();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    public Map<String, String> readSnapshotJobFile() {
        val snapshotJob = Maps.<String, String> newHashMap();
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val snapshotJobPath = new Path(
                config.getSnapshotAutoRefreshDir(project) + SNAPSHOT_JOB + BACKSLASH + tableIdentity);
        try {
            if (fileSystem.exists(snapshotJobPath)) {
                try (FSDataInputStream inputStream = fileSystem.open(snapshotJobPath)) {
                    snapshotJob.putAll(JsonUtil.readValue(inputStream, new TypeReference<Map<String, String>>() {
                    }));
                }
            }
        } catch (IOException e) {
            log.error("read SnapshotSourceTableStats path[{}] to SourceTableStats has error", snapshotJobPath, e);
        }
        return snapshotJob;
    }

    public void saveSnapshotJobFile(Boolean buildError, String errorMessage, String jobId) {
        val snapshotJob = Maps.newHashMap();
        snapshotJob.put("build_error", buildError);
        snapshotJob.put("error_message", errorMessage);
        snapshotJob.put("job_id", jobId);
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val snapshotJobPath = new Path(
                config.getSnapshotAutoRefreshDir(project) + SNAPSHOT_JOB + BACKSLASH + tableIdentity);
        try (val out = fileSystem.create(snapshotJobPath, true)) {
            out.write(JsonUtil.writeValueAsBytes(snapshotJob));
        } catch (IOException e) {
            log.error("overwrite SourceTableStats to path[{}] failed!", snapshotJobPath, e);
        }
    }

    public Map<Object, Object> createRequestAndCheckRunningJob() {
        val req = Maps.newHashMap();
        req.put("need_refresh", needRefresh);
        req.put("project", project);
        req.put("tables", Sets.newHashSet(tableIdentity));
        val runningSnapshotJobs = getRunningSnapshotJobs();
        if (StringUtils.isNotBlank(partitionColumn)) {
            // partition table
            if (checkNeedBuildPartitionAndSetTableOption(req, runningSnapshotJobs)) {
                throw new KylinRuntimeException(String.format(Locale.ROOT,
                        "Project[%s] Snapshot[%s] buildSnapshot failed, because none partitions need build", project,
                        tableIdentity));
            }
        } else {
            if (CollectionUtils.isNotEmpty(runningSnapshotJobs)) {
                log.info("buildSnapshot: {} has running snapshot job", tableIdentity);
                throw new KylinRuntimeException(String.format(Locale.ROOT,
                        "Project[%s] Snapshot[%s] buildSnapshot failed, because has running snapshot job", project,
                        tableIdentity));
            }
        }
        return req;
    }

    private List<NSparkSnapshotJob> getRunningSnapshotJobs() {
        val execManager = ExecutableManager.getInstance(config, project);

        List<AbstractExecutable> executables = execManager.jobInfoToExecutable(execManager.fetchJobsByTypesAndStates(
                project, Lists.newArrayList(SNAPSHOT_BUILD.name(), SNAPSHOT_REFRESH.name()), null,
                ExecutableState.getNotFinalStates()));

        return executables.stream()
                .filter(executable -> StringUtils.equalsIgnoreCase(executable.getParam(NBatchConstants.P_TABLE_NAME),
                        tableIdentity))
                .filter(NSparkSnapshotJob.class::isInstance).map(NSparkSnapshotJob.class::cast)
                .collect(Collectors.toList());
    }

    public boolean checkNeedBuildPartitionAndSetTableOption(Map<Object, Object> req,
            List<NSparkSnapshotJob> runningSnapshotJobs) {
        if (CollectionUtils.isEmpty(needRefreshPartitionsValue)) {
            log.info("buildSnapshot: needRefreshPartitionsValue is empty");
            return true;
        }
        log.info("will build snapshot partitions value : {}", needRefreshPartitionsValue);
        val needBuildPartitionsValue = getNeedBuildPartitionsValue(runningSnapshotJobs);
        if (CollectionUtils.isEmpty(needBuildPartitionsValue)) {
            log.info("buildSnapshot: needBuildPartitionsValue is empty");
            return true;
        }
        val tableOption = Maps.newHashMap();
        val option = Maps.newHashMap();
        option.put("partition_col", partitionColumn);
        option.put("incremental_build", true);
        option.put("partitions_to_build", needBuildPartitionsValue);
        tableOption.put(tableIdentity, option);
        req.put("options", tableOption);
        return false;
    }

    private Sets.SetView<String> getNeedBuildPartitionsValue(List<NSparkSnapshotJob> runningSnapshotJobs) {
        val runningSnapshotJobsSelectPartitionsValue = runningSnapshotJobs.stream()
                .map(job -> job.getParam(NBatchConstants.P_SELECTED_PARTITION_VALUE)).map(selectedPartition -> {
                    try {
                        return JsonUtil.readValueAsSet(selectedPartition);
                    } catch (IOException ignore) {
                        return Sets.<String> newHashSet();
                    }
                }).flatMap(Collection::stream).collect(Collectors.toSet());
        return Sets.difference(needRefreshPartitionsValue, runningSnapshotJobsSelectPartitionsValue);
    }
}

@Getter
@Setter
@ToString
class JobInfoResponse {
    @JsonProperty("jobs")
    private List<JobInfo> jobs = Lists.newArrayList();
}

@Getter
@Setter
@ToString
class JobInfo {
    @JsonProperty("job_name")
    private String jobName;

    @JsonProperty("job_id")
    private String jobId;
}
