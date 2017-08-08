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

package org.apache.kylin.metrics.job;

import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.apache.kylin.metrics.lib.impl.RecordEventWrapper;

public class JobRecordEventWrapper extends RecordEventWrapper {

    public static final long MIN_SOURCE_SIZE = 33554432L; //32MB per block created by the first step

    public JobRecordEventWrapper(RecordEvent metricsEvent) {
        super(metricsEvent);
        initStats();
    }

    public void initStats() {
        this.metricsEvent.put(JobPropertyEnum.SOURCE_SIZE.toString(), 0L);
        this.metricsEvent.put(JobPropertyEnum.CUBE_SIZE.toString(), 0L);
        this.metricsEvent.put(JobPropertyEnum.BUILD_DURATION.toString(), 0L);
        this.metricsEvent.put(JobPropertyEnum.WAIT_RESOURCE_TIME.toString(), 0L);
        this.metricsEvent.put(JobPropertyEnum.STEP_DURATION_DISTINCT_COLUMNS.toString(), 0L);
        this.metricsEvent.put(JobPropertyEnum.STEP_DURATION_DICTIONARY.toString(), 0L);
        this.metricsEvent.put(JobPropertyEnum.STEP_DURATION_INMEM_CUBING.toString(), 0L);
        this.metricsEvent.put(JobPropertyEnum.STEP_DURATION_HFILE_CONVERT.toString(), 0L);
        setDependentStats();
    }

    public void setWrapper(String projectName, String cubeName, String jobId, String jobType, String cubingType) {
        this.metricsEvent.put(JobPropertyEnum.PROJECT.toString(), projectName);
        this.metricsEvent.put(JobPropertyEnum.CUBE.toString(), cubeName);
        this.metricsEvent.put(JobPropertyEnum.ID_CODE.toString(), jobId);
        this.metricsEvent.put(JobPropertyEnum.TYPE.toString(), jobType);
        this.metricsEvent.put(JobPropertyEnum.ALGORITHM.toString(), cubingType);
    }

    public void setStats(long tableSize, long cubeSize, long buildDuration, long waitResourceTime) {
        this.metricsEvent.put(JobPropertyEnum.SOURCE_SIZE.toString(), tableSize);
        this.metricsEvent.put(JobPropertyEnum.CUBE_SIZE.toString(), cubeSize);
        this.metricsEvent.put(JobPropertyEnum.BUILD_DURATION.toString(), buildDuration);
        this.metricsEvent.put(JobPropertyEnum.WAIT_RESOURCE_TIME.toString(), waitResourceTime);
        setDependentStats();
    }

    public void setStepStats(long dColumnDistinct, long dDictBuilding, long dCubingInmem, long dHfileConvert) {
        this.metricsEvent.put(JobPropertyEnum.STEP_DURATION_DISTINCT_COLUMNS.toString(), dColumnDistinct);
        this.metricsEvent.put(JobPropertyEnum.STEP_DURATION_DICTIONARY.toString(), dDictBuilding);
        this.metricsEvent.put(JobPropertyEnum.STEP_DURATION_INMEM_CUBING.toString(), dCubingInmem);
        this.metricsEvent.put(JobPropertyEnum.STEP_DURATION_HFILE_CONVERT.toString(), dHfileConvert);
    }

    private void setDependentStats() {
        Long sourceSize = (Long) this.metricsEvent.get(JobPropertyEnum.SOURCE_SIZE.toString());
        if (sourceSize != null && sourceSize != 0) {
            if (sourceSize < MIN_SOURCE_SIZE) {
                sourceSize = MIN_SOURCE_SIZE;
            }
            this.metricsEvent.put(JobPropertyEnum.PER_BYTES_TIME_COST.toString(),
                    ((Long) this.metricsEvent.get(JobPropertyEnum.BUILD_DURATION.toString())
                            - (Long) this.metricsEvent.get(JobPropertyEnum.WAIT_RESOURCE_TIME.toString())) * 1.0
                            / sourceSize);
        } else {
            this.metricsEvent.put(JobPropertyEnum.PER_BYTES_TIME_COST.toString(), 0.0);
        }
    }

}