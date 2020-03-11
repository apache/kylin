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

package org.apache.kylin.stream.core.query;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.gridtable.StorageSideBehavior;

public class StreamingQueryProfile {
    private static ThreadLocal<StreamingQueryProfile> threadProfile = new ThreadLocal<>();
    private String queryId;
    private boolean enableDetailProfile;
    private long queryStartTime;
    private long requestSendTime;
    private int totalScanFiles = 0;
    private long totalScanFileSize = 0;
    private AtomicLong scanRows = new AtomicLong(0);
    private AtomicLong filterRows= new AtomicLong(0);
    private long finalRows;

    private StorageSideBehavior storageBehavior = StorageSideBehavior.SCAN_FILTER_AGGR_CHECKMEM;
    private List<String> includeSegments;
    private List<String> skippedSegments;
    private List<ProfileStep> profileSteps;
    private Map<String, ProfileStep> stepMap;

    public StreamingQueryProfile(String queryId, long requestSendTime) {
        this.queryId = queryId;
        this.includeSegments = Lists.newArrayList();
        this.skippedSegments = Lists.newArrayList();
        this.profileSteps = Lists.newArrayList();
        this.stepMap = Maps.newHashMap();
        this.requestSendTime = requestSendTime;
        this.queryStartTime = System.currentTimeMillis();
    }

    public static StreamingQueryProfile get() {
        return threadProfile.get();
    }

    public static void set(StreamingQueryProfile profile) {
        threadProfile.set(profile);
    }

    public boolean isDetailProfileEnable() {
        return enableDetailProfile;
    }

    public void enableDetailProfile() {
        this.enableDetailProfile = true;
    }

    public void includeSegment(String segmentName) {
        includeSegments.add(segmentName);
    }

    public void skipSegment(String segmentName) {
        skippedSegments.add(segmentName);
    }

    public void incScanFile(int fileSize) {
        totalScanFiles++;
        totalScanFileSize += fileSize;
    }

    public ProfileStep startStep(String stepName) {
        long startTime = System.currentTimeMillis();
        ProfileStep step = new ProfileStep(stepName, startTime);
        profileSteps.add(step);
        stepMap.put(stepName, step);
        return step;
    }

    public ProfileStep finishStep(String stepName) {
        ProfileStep step = stepMap.get(stepName);
        if (step != null) {
            step.duration = System.currentTimeMillis() - step.startTime;
        }
        return step;
    }

    public void addStepInfo(String stepName, String key, String val) {
        ProfileStep step = stepMap.get(stepName);
        if (step != null) {
            step.stepInfo(key, val);
        }
    }

    public String getQueryId() {
        return queryId;
    }

    public void incScanRows(long n) {
        scanRows.addAndGet(n);
    }

    public void incFilterRows(long n) {
        filterRows.addAndGet(n);
    }

    public void setFinalRows(long n) {
        this.finalRows = n;
    }

    public StorageSideBehavior getStorageBehavior() {
        return storageBehavior;
    }

    public void setStorageBehavior(StorageSideBehavior storageBehavior) {
        this.storageBehavior = storageBehavior;
    }

    public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.println();
        pw.println("start:          " + (queryStartTime - requestSendTime));
        pw.println("segments num:   " + includeSegments.size());
        pw.println("segments:       " + includeSegments);
        pw.println("skip segments:  " + skippedSegments);
        pw.println("total files:    " + totalScanFiles);
        pw.println("total file size:" + totalScanFileSize);
        pw.println("scan rows:      " + scanRows);
        pw.println("filter rows:    " + filterRows);
        pw.println("final rows:      " + finalRows);
        pw.println();
        if (enableDetailProfile) {
            pw.println("details:");
            for (ProfileStep profileStep : profileSteps) {
                pw.println("  " + profileStep.toString());
            }
        }
        return sw.toString();
    }

    public class ProfileStep {
        String name;
        Map<String, String> properties;
        long startTime;
        long duration;

        ProfileStep(String name, long startTime) {
            this.name = name;
            this.properties = Maps.newHashMap();
            this.startTime = startTime;
        }

        public ProfileStep stepInfo(String key, String val) {
            this.properties.put(key, val);
            return this;
        }

        public long getDuration() {
            return duration;
        }

        @Override
        public String toString() {
            String start = String.valueOf(startTime - queryStartTime);
            StringBuilder builder = new StringBuilder();
            builder.append(start);
            builder.append("    ");
            builder.append(name);
            if (!properties.isEmpty()) {
                builder.append(properties.toString());
            }
            builder.append("    ");
            builder.append(duration);
            builder.append("ms");
            return builder.toString();
        }
    }
}
