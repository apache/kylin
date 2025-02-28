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

package org.apache.kylin.common.scheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JobFinishedNotifier extends SchedulerEventNotifier {
    private String jobId;
    private long duration;
    private String jobState;
    private String jobType;
    private Set<String> segmentIds;
    private Set<Long> layoutIds;
    private long waitTime;
    private Map<String, Set<Long>> segmentPartitionsMap;
    private String jobClass;
    private String owner;
    private boolean isSucceed;
    private long startTime;
    private long endTime;
    private Object tag;
    private Throwable throwable;

    public JobFinishedNotifier(String jobId, String project, String subject, long duration, String jobState,
            String jobType, Set<String> segmentIds, Set<Long> layoutIds, Set<Long> partitionIds, long waitTime,
            String jobClass, String owner, boolean result, long startTime, long endTime, Object tag,
            Throwable throwable) {
        setProject(project);
        setSubject(subject);
        this.jobId = jobId;
        this.duration = duration;
        this.jobState = jobState;
        this.jobType = jobType;
        this.segmentIds = segmentIds;
        this.layoutIds = layoutIds;
        this.waitTime = waitTime;
        if (partitionIds != null) {
            this.segmentPartitionsMap = new HashMap<>();
            if (segmentIds != null) {
                for (String segmentId : segmentIds) {
                    segmentPartitionsMap.put(segmentId, partitionIds);
                }
            }
        }
        this.jobClass = jobClass;
        this.owner = owner;
        this.isSucceed = result;
        this.startTime = startTime;
        this.endTime = endTime;
        this.tag = tag;
        this.throwable = throwable;
    }

}
