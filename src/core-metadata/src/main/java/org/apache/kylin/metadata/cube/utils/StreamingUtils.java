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

package org.apache.kylin.metadata.cube.utils;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.STREAMING_TABLE_REFRESH_INTERVAL_UNIT_ERROR;

import java.lang.management.ManagementFactory;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore;
import org.apache.kylin.common.persistence.metadata.PersistException;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;

import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingUtils {

    private static int MAX_RETRY = 3;

    public static Set<LayoutEntity> getToBuildLayouts(NDataflow df) {
        Set<LayoutEntity> layouts = Sets.newHashSet();
        Segments<NDataSegment> readySegments = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);

        if (CollectionUtils.isEmpty(readySegments)) {
            if (CollectionUtils.isNotEmpty(df.getIndexPlan().getAllIndexes())) {
                layouts.addAll(df.getIndexPlan().getAllLayouts());
            }
            log.trace("added {} layouts according to model {}'s index plan", layouts.size(),
                    df.getIndexPlan().getModel().getAlias());
        } else {
            NDataSegment latestReadySegment = readySegments.getLatestReadySegment();
            for (Map.Entry<Long, NDataLayout> cuboid : latestReadySegment.getLayoutsMap().entrySet()) {
                layouts.add(cuboid.getValue().getLayout());
            }
            log.trace("added {} layouts according to model {}'s latest ready segment {}", layouts.size(),
                    df.getIndexPlan().getModel().getAlias(), latestReadySegment.getName());
        }
        return layouts;
    }

    public static String getJobId(String modelId, String jobType) {
        return modelId + "_" + jobType.toLowerCase(Locale.ROOT).substring(10);
    }

    public static String getModelId(String jobId) {
        return jobId.substring(0, jobId.lastIndexOf("_"));
    }

    public static String parseStreamingDuration(String duration) {
        if (duration == null || "".equals(duration.trim())) {
            return "30";
        } else {
            return duration;
        }
    }

    public static Long parseSize(String inputSize) {
        if (inputSize == null || "".equals(inputSize.trim())) {
            inputSize = "32m";
        }
        var size = 0L;
        if (inputSize.endsWith("b")) {
            size = Long.parseLong(inputSize.substring(0, inputSize.length() - 2));
        } else {
            size = Long.parseLong(inputSize.substring(0, inputSize.length() - 1));
        }
        if (inputSize.endsWith("k") || inputSize.endsWith("kb")) {
            return size * 1024;
        } else if (inputSize.endsWith("m") || inputSize.endsWith("mb")) {
            return size * 1024 * 1024;
        } else if (inputSize.endsWith("g") || inputSize.endsWith("gb")) {
            return size * 1024 * 1024 * 1024;
        } else {
            throw new IllegalArgumentException("Size unit must be k/kb, m/mb or g/gb...");
        }
    }

    public static Long parseTableRefreshInterval(String inputInterval) {
        if (inputInterval == null || "".equals(inputInterval.trim())) {
            return TimeUtil.timeStringAs("-1m", TimeUnit.MINUTES);
        }
        if (inputInterval.endsWith("m")) {
            return TimeUtil.timeStringAs(inputInterval, TimeUnit.MINUTES);
        } else if (inputInterval.endsWith("h")) {
            return 60 * TimeUtil.timeStringAs(inputInterval, TimeUnit.HOURS);
        } else if (inputInterval.endsWith("d")) {
            return 24 * 60 * TimeUtil.timeStringAs(inputInterval, TimeUnit.DAYS);
        } else {
            throw new KylinException(STREAMING_TABLE_REFRESH_INTERVAL_UNIT_ERROR);
        }
    }

    public static boolean isLocalMode() {
        return "true".equals(System.getProperty("streaming.local"));
    }

    public static void replayAuditlog() {
        int retry = 0;
        Exception err = new Exception("catch error");

        while (retry++ < MAX_RETRY) {
            KylinConfig conf = KylinConfig.getInstanceFromEnv();
            try {
                ResourceStore store = ResourceStore.getKylinMetaStore(conf);
                val auditLogStore = store.getAuditLogStore();
                if (auditLogStore instanceof JdbcAuditLogStore) {
                    ((JdbcAuditLogStore) auditLogStore).catchupWithMaxTimeout();
                } else {
                    auditLogStore.catchupWithTimeout();
                }
                return;
            } catch (Exception e) {
                err = e;
                log.warn("catch error, begin to retry");
            }
        }
        throw new PersistException(err.getMessage(), err);
    }

    public static String getProcessId() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        return name.split("@")[0];
    }

    public static boolean isJobOnCluster(KylinConfig config) {
        return !StreamingUtils.isLocalMode() && !config.isUTEnv();
    }

    public static void sleep(long times) {
        try {
            Thread.sleep(times);
        } catch (InterruptedException e) {
            log.error("Thread is interrupted while sleeping");
            Thread.currentThread().interrupt();
        }
    }
}
