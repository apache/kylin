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

package org.apache.kylin.job.metrics;

import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metrics.MetricsManager;
import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.apache.kylin.metrics.lib.impl.TimedRecordEvent;
import org.apache.kylin.metrics.property.JobPropertyEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobMetricsFacade {
    private static final Logger logger = LoggerFactory.getLogger(JobMetricsFacade.class);

    public static void updateMetrics(JobStatisticsResult jobStats) {
        if (!KylinConfig.getInstanceFromEnv().isKylinMetricsReporterForJobEnabled()) {
            return;
        }
        RecordEvent metricsEvent;
        if (jobStats.throwable == null) {
            metricsEvent = new TimedRecordEvent(KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectJob());
            setJobWrapper(metricsEvent, jobStats.user, jobStats.projectName, jobStats.cubeName, jobStats.jobId,
                    jobStats.jobType, jobStats.cubingType);
            setJobStats(metricsEvent, jobStats.tableSize, jobStats.cubeSize, jobStats.buildDuration,
                    jobStats.waitResourceTime, jobStats.perBytesTimeCost,
                    jobStats.dColumnDistinct, jobStats.dDictBuilding, jobStats.dCubingInmem, jobStats.dHfileConvert);
        } else {
            metricsEvent = new TimedRecordEvent(KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectJobException());
            setJobExceptionWrapper(metricsEvent, jobStats.user, jobStats.projectName, jobStats.cubeName, jobStats.jobId,
                    jobStats.jobType, jobStats.cubingType, //
                    jobStats.throwable);
        }
        MetricsManager.getInstance().update(metricsEvent);
    }

    private static void setJobWrapper(RecordEvent metricsEvent, String user, String projectName, String cubeName,
            String jobId, String jobType, String cubingType) {
        metricsEvent.put(JobPropertyEnum.USER.toString(), user);
        metricsEvent.put(JobPropertyEnum.PROJECT.toString(), projectName);
        metricsEvent.put(JobPropertyEnum.CUBE.toString(), cubeName);
        metricsEvent.put(JobPropertyEnum.ID_CODE.toString(), jobId);
        metricsEvent.put(JobPropertyEnum.TYPE.toString(), jobType);
        metricsEvent.put(JobPropertyEnum.ALGORITHM.toString(), cubingType);
    }

    private static void setJobStats(RecordEvent metricsEvent, long tableSize, long cubeSize, long buildDuration,
            long waitResourceTime, double perBytesTimeCost, long dColumnDistinct, long dDictBuilding, long dCubingInmem,
            long dHfileConvert) {
        metricsEvent.put(JobPropertyEnum.SOURCE_SIZE.toString(), tableSize);
        metricsEvent.put(JobPropertyEnum.CUBE_SIZE.toString(), cubeSize);
        metricsEvent.put(JobPropertyEnum.BUILD_DURATION.toString(), buildDuration);
        metricsEvent.put(JobPropertyEnum.WAIT_RESOURCE_TIME.toString(), waitResourceTime);
        metricsEvent.put(JobPropertyEnum.PER_BYTES_TIME_COST.toString(), perBytesTimeCost);
        metricsEvent.put(JobPropertyEnum.STEP_DURATION_DISTINCT_COLUMNS.toString(), dColumnDistinct);
        metricsEvent.put(JobPropertyEnum.STEP_DURATION_DICTIONARY.toString(), dDictBuilding);
        metricsEvent.put(JobPropertyEnum.STEP_DURATION_INMEM_CUBING.toString(), dCubingInmem);
        metricsEvent.put(JobPropertyEnum.STEP_DURATION_HFILE_CONVERT.toString(), dHfileConvert);
    }

    private static <T extends Throwable> void setJobExceptionWrapper(RecordEvent metricsEvent, String user,
            String projectName, String cubeName, String jobId, String jobType, String cubingType,
            Throwable throwable) {
        setJobWrapper(metricsEvent, user, projectName, cubeName, jobId, jobType, cubingType);
        metricsEvent.put(JobPropertyEnum.EXCEPTION.toString(), throwable.getClass().getName());
        metricsEvent.put(JobPropertyEnum.EXCEPTION_MSG.toString(), throwable.getMessage());
    }

    public static class JobStatisticsResult {
        // dimensions
        private String user;
        private String projectName;
        private String cubeName;
        private String jobId;
        private String jobType;
        private String cubingType;

        // statistics
        private long tableSize;
        private long cubeSize;
        private long buildDuration;
        private long waitResourceTime;
        private double perBytesTimeCost;

        // step statistics
        private long dColumnDistinct = 0L;
        private long dDictBuilding = 0L;
        private long dCubingInmem = 0L;
        private long dHfileConvert = 0L;

        // exception
        private Throwable throwable;

        public void setWrapper(String user, String projectName, String cubeName, String jobId, String jobType,
                String cubingType) {
            this.user = user;
            this.projectName = projectName == null ? null : projectName.toUpperCase(Locale.ROOT);
            this.cubeName = cubeName;
            this.jobId = jobId;
            this.jobType = jobType;
            this.cubingType = cubingType;
        }

        public void setJobStats(long tableSize, long cubeSize, long buildDuration, long waitResourceTime,
                double perBytesTimeCost) {
            this.tableSize = tableSize;
            this.cubeSize = cubeSize;
            this.buildDuration = buildDuration;
            this.waitResourceTime = waitResourceTime;
            this.perBytesTimeCost = perBytesTimeCost;
        }

        public void setJobStepStats(long dColumnDistinct, long dDictBuilding, long dCubingInmem, long dHfileConvert) {
            this.dColumnDistinct = dColumnDistinct;
            this.dDictBuilding = dDictBuilding;
            this.dCubingInmem = dCubingInmem;
            this.dHfileConvert = dHfileConvert;
        }

        public void setJobException(Throwable throwable) {
            this.throwable = throwable;
        }
    }
}
