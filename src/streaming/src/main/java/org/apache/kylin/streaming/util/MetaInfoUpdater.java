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

package org.apache.kylin.streaming.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.streaming.StreamingJobRecord;
import org.apache.kylin.metadata.streaming.StreamingJobRecordManager;
import org.apache.kylin.streaming.constants.StreamingConstants;
import org.apache.kylin.streaming.manager.StreamingJobManager;

import org.apache.kylin.guava30.shaded.common.collect.Sets;
import lombok.val;

public class MetaInfoUpdater {
    public static void update(String project, NDataSegment seg, NDataLayout layout) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowUpdate update = new NDataflowUpdate(seg.getDataflow().getUuid());
            update.setToAddOrUpdateLayouts(layout);
            NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateDataflow(update);
            return 0;
        }, project);
    }

    public static void updateJobState(String project, String jobId, JobStatusEnum state) {
        updateJobState(project, jobId, Sets.newHashSet(state), state);
    }

    public static void updateJobState(String project, String jobId, Set<JobStatusEnum> excludeStates,
            JobStatusEnum state) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            StreamingJobManager mgr = StreamingJobManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            mgr.updateStreamingJob(jobId, copyForWrite -> {
                if (!excludeStates.contains(copyForWrite.getCurrentStatus())) {
                    copyForWrite.setCurrentStatus(state);
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                            Locale.getDefault(Locale.Category.FORMAT));
                    Date date = new Date(System.currentTimeMillis());
                    copyForWrite.setLastUpdateTime(format.format(date));
                    val recordMgr = StreamingJobRecordManager.getInstance();
                    val record = new StreamingJobRecord();
                    record.setJobId(jobId);
                    record.setProject(project);
                    record.setCreateTime(System.currentTimeMillis());
                    switch (state) {
                    case STARTING:
                        if (!StringUtils.isEmpty(copyForWrite.getYarnAppUrl())) {
                            copyForWrite.setYarnAppId(StringUtils.EMPTY);
                            copyForWrite.setYarnAppUrl(StringUtils.EMPTY);
                        }
                        break;
                    case RUNNING:
                        copyForWrite.setLastStartTime(format.format(date));
                        copyForWrite.setSkipListener(false);
                        copyForWrite.setAction(StreamingConstants.ACTION_START);
                        record.setAction("START");
                        recordMgr.insert(record);
                        break;
                    case STOPPED:
                        record.setAction("STOP");
                        copyForWrite.setLastEndTime(format.format(date));
                        recordMgr.insert(record);
                        break;
                    case ERROR:
                        copyForWrite.setLastEndTime(format.format(date));
                        record.setAction("ERROR");
                        recordMgr.insert(record);
                        break;
                    default:
                    }
                }
            });
            return null;
        }, project);
    }

    public static void markGracefulShutdown(String project, String uuid) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val config = KylinConfig.getInstanceFromEnv();
            val mgr = StreamingJobManager.getInstance(config, project);
            mgr.updateStreamingJob(uuid, copyForWrite -> {
                if (copyForWrite != null) {
                    copyForWrite.setAction(StreamingConstants.ACTION_GRACEFUL_SHUTDOWN);
                }
            });
            return null;
        }, project);
    }
}
