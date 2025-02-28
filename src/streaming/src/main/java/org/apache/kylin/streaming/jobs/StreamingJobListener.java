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

package org.apache.kylin.streaming.jobs;

import java.io.IOException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.guava30.shaded.common.eventbus.Subscribe;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.streaming.event.StreamingJobDropEvent;
import org.apache.kylin.streaming.event.StreamingJobKillEvent;
import org.apache.kylin.streaming.event.StreamingJobMetaCleanEvent;
import org.apache.kylin.streaming.jobs.scheduler.StreamingScheduler;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.apache.kylin.streaming.util.JobKiller;
import org.apache.kylin.streaming.util.MetaInfoUpdater;
import org.apache.spark.launcher.SparkAppHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class StreamingJobListener implements SparkAppHandle.Listener {
    private static final Logger logger = LoggerFactory.getLogger(StreamingJobListener.class);

    private String project;
    private String jobId;
    private String runnable;

    public StreamingJobListener() {

    }

    public StreamingJobListener(String project, String jobId) {
        this.project = project;
        this.jobId = jobId;
    }

    @Override
    public void stateChanged(SparkAppHandle handler) {
        if (handler.getState().isFinal()) {
            runnable = null;
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            val mgr = StreamingJobManager.getInstance(config, project);
            val jobMeta = mgr.getStreamingJobByUuid(jobId);
            if (isFailed(handler.getState()) && !jobMeta.isSkipListener()) {
                logger.warn("The streaming job {} has terminated unexpectedly…", jobId);
                handler.kill();
                JobKiller.killProcess(jobMeta);
                JobKiller.killApplication(jobId);
                MetaInfoUpdater.updateJobState(project, jobId,
                        Sets.newHashSet(JobStatusEnum.ERROR, JobStatusEnum.STOPPED), JobStatusEnum.ERROR);
            } else if (isFinished(handler.getState())) {
                handler.stop();
                JobKiller.killProcess(jobMeta);
                JobKiller.killApplication(jobId);
                MetaInfoUpdater.updateJobState(project, jobId,
                        Sets.newHashSet(JobStatusEnum.ERROR, JobStatusEnum.STOPPED), JobStatusEnum.STOPPED);
            }
        } else if (runnable == null && SparkAppHandle.State.RUNNING == handler.getState()) {
            runnable = "true";
            MetaInfoUpdater.updateJobState(project, jobId, JobStatusEnum.RUNNING);
        }
    }

    private boolean isFailed(SparkAppHandle.State state) {
        return SparkAppHandle.State.FAILED == state || SparkAppHandle.State.KILLED == state
                || SparkAppHandle.State.LOST == state;
    }

    private boolean isFinished(SparkAppHandle.State state) {
        return SparkAppHandle.State.FINISHED == state;
    }

    @Override
    public void infoChanged(SparkAppHandle handler) {
        // just Override
    }

    @Subscribe
    public void onStreamingJobKill(StreamingJobKillEvent streamingJobKillEvent) {
        val modelId = streamingJobKillEvent.getModelId();
        StreamingScheduler scheduler = StreamingScheduler.getInstance();
        scheduler.killJob(streamingJobKillEvent.getProject(), modelId, JobTypeEnum.STREAMING_MERGE,
                JobStatusEnum.STOPPED);
        scheduler.killJob(streamingJobKillEvent.getProject(), modelId, JobTypeEnum.STREAMING_BUILD,
                JobStatusEnum.STOPPED);
    }

    @Subscribe
    public void onStreamingJobDrop(StreamingJobDropEvent streamingJobDropEvent) {
        val modelId = streamingJobDropEvent.getModelId();
        val config = KylinConfig.getInstanceFromEnv();
        val mgr = StreamingJobManager.getInstance(config, streamingJobDropEvent.getProject());
        val buildJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.toString());
        val mergeJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.toString());
        mgr.deleteStreamingJob(buildJobId);
        mgr.deleteStreamingJob(mergeJobId);
    }

    @Subscribe
    public void onStreamingJobMetaCleanEvent(StreamingJobMetaCleanEvent streamingJobMetaCleanEvent) {
        val deletedPath = streamingJobMetaCleanEvent.getDeletedMetaPath();
        if (CollectionUtils.isEmpty(deletedPath)) {
            logger.debug("path list is empty, skip to delete.");
            return;
        }

        logger.info("begin to delete streaming meta path size:{}", deletedPath.size());
        deletedPath.forEach(path -> {
            try {
                val deleteSuccess = HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), path);
                logger.debug("delete streaming meta {} path:{}", deleteSuccess, path);
            } catch (IOException e) {
                logger.warn("delete streaming meta path:{} error", path, e);
            }
        });
    }
}
