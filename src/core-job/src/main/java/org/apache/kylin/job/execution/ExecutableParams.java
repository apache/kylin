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
package org.apache.kylin.job.execution;

import static org.apache.kylin.job.execution.AbstractExecutable.NOTIFY_LIST;
import static org.apache.kylin.job.execution.AbstractExecutable.PARENT_ID;
import static org.apache.kylin.job.execution.AbstractExecutable.SPARK_YARN_QUEUE;
import static org.apache.kylin.job.execution.AbstractExecutable.SUBMITTER;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_DATA_RANGE_END;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_DATA_RANGE_START;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.MailHelper;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.job.JobBucket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ExecutableParams {

    @Getter
    private final Map<String, String> params = Maps.newHashMap();

    public final String getParam(String key) {
        return this.params.get(key);
    }

    public final void setParam(String key, String value) {
        this.params.put(key, value);
    }

    public final void setParams(Map<String, String> params) {
        if (MapUtils.isNotEmpty(params)) {
            this.params.putAll(params);
        }
    }

    public final void setParentId(String parentId) {
        setParam(PARENT_ID, parentId);
    }

    public final void setSubmitter(String submitter) {
        setParam(SUBMITTER, submitter);
    }

    public final void setParent(AbstractExecutable parent) {
        setParentId(parent.getId());
    }

    public final List<String> getNotifyList() {
        final String str = getParam(NOTIFY_LIST);
        if (str != null) {
            return Lists.newArrayList(StringUtils.split(str, ","));
        } else {
            return Collections.emptyList();
        }
    }

    public final void setNotifyList(String notifications) {
        setParam(NOTIFY_LIST, notifications);
    }

    public final void setNotifyList(List<String> notifications) {
        setNotifyList(StringUtils.join(notifications, ","));
    }

    public List<String> getAllNotifyUsers(KylinConfig kylinConfig) {
        final String[] adminDls = kylinConfig.getAdminDls();
        List<String> users = MailHelper.getAllNotifyUserList(adminDls);
        users.addAll(getNotifyList());
        return users;
    }

    public final String getParentId() {
        return getParam(PARENT_ID);
    }

    public final String getSubmitter() {
        return getParam(SUBMITTER);
    }

    public long getDataRangeEnd() {
        return Optional.ofNullable(getParam(P_DATA_RANGE_END)).map(Long::parseLong).orElse(0L);
    }

    public long getDataRangeStart() {
        return Optional.ofNullable(getParam(P_DATA_RANGE_START)).map(Long::parseLong).orElse(0L);
    }

    public Set<Long> getToBeDeletedLayoutIds() {
        Set<Long> layoutIdList = new LinkedHashSet<>();
        String idStr = getParam(NBatchConstants.P_LAYOUT_IDS);
        if (StringUtils.isNotBlank(idStr)) {
            for (String id : idStr.split(",")) {
                layoutIdList.add(Long.parseLong(id));
            }
        }

        return layoutIdList;
    }

    public Set<Long> getLayoutIds() {
        if (getParam(NBatchConstants.P_LAYOUT_IDS) != null) {
            return Sets
                    .newHashSet(org.apache.commons.lang3.StringUtils.split(getParam(NBatchConstants.P_LAYOUT_IDS), ","))
                    .stream().map(Long::parseLong).collect(Collectors.toSet());
        } else {
            return null;
        }
    }

    public Set<String> getSegmentIds() {
        if (getParam(NBatchConstants.P_SEGMENT_IDS) != null) {
            return Sets.newHashSet(
                    org.apache.commons.lang3.StringUtils.split(getParam(NBatchConstants.P_SEGMENT_IDS), ","));
        } else {
            return null;
        }
    }

    public void setSparkYarnQueue(String queue) {
        this.setParam(SPARK_YARN_QUEUE, queue);
    }

    public String getSparkYarnQueue() {
        return getParam(SPARK_YARN_QUEUE);
    }

    /**
     * Compression bucket param to reduce storage volume in database.
     * pattern: {segmentId:[ [layoutId:[{partitionId, bucketId},{partitionId, bucketId},...], [layoutId:[{partId, bucketId},{partId, bucketId},...], }
     */
    public static String toBucketParam(Set<JobBucket> buckets) {
        String param = "";
        try {
            HashMap<String, Map<Long, Map<Long, Long>>> bucketParams = Maps.newHashMap();
            buckets.forEach(bucket -> {
                bucketParams.computeIfAbsent(bucket.getSegmentId(), k -> Maps.newHashMap());
                bucketParams.get(bucket.getSegmentId()).computeIfAbsent(bucket.getLayoutId(), k -> Maps.newHashMap());
                bucketParams.get(bucket.getSegmentId()).get(bucket.getLayoutId()).put(bucket.getPartitionId(),
                        bucket.getBucketId());
            });
            param = JsonUtil.writeValueAsString(bucketParams);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return param;
    }

    public static Set<JobBucket> getBuckets(String content) {
        final Set<JobBucket> buckets = Sets.newHashSet();
        try {
            val bucketParams = JsonUtil.readValue(content,
                    new TypeReference<HashMap<String, Map<Long, Map<Long, Long>>>>() {
                    });
            bucketParams.forEach((segment, layouts) -> {
                layouts.forEach((layoutId, partitions) -> {
                    partitions.forEach((partitionId, bucketId) -> {
                        buckets.add(new JobBucket(segment, layoutId, bucketId, partitionId));
                    });
                });
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buckets;
    }

    // segmentId -> target partitions
    public final Map<String, Set<Long>> getPartitionsBySegment() {
        Set<JobBucket> buckets = getBuckets(getParam(NBatchConstants.P_BUCKETS));
        HashMap<String, Set<Long>> partitions = Maps.newHashMap();
        buckets.forEach(bucket -> {
            partitions.putIfAbsent(bucket.getSegmentId(), Sets.newHashSet());
            partitions.get(bucket.getSegmentId()).add(bucket.getPartitionId());
        });
        return partitions;
    }
}
