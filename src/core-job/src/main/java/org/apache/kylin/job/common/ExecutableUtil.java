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

package org.apache.kylin.job.common;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_INDEX_FAIL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY;
import static org.apache.kylin.job.constant.ExecutableConstants.COLUMNAR_SHUFFLE_MANAGER;
import static org.apache.kylin.job.constant.ExecutableConstants.GLUTEN_PLUGIN;
import static org.apache.kylin.job.constant.ExecutableConstants.GLUTEN_PREFIX;
import static org.apache.kylin.job.constant.ExecutableConstants.SPARK_PLUGINS;
import static org.apache.kylin.job.constant.ExecutableConstants.SPARK_SHUFFLE_MANAGER;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.code.ErrorCodeProducer;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataSegmentManager;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.job.JobBucket;
import org.apache.kylin.metadata.model.NDataModelManager;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public abstract class ExecutableUtil {

    private static final ConcurrentMap<JobTypeEnum, ExecutableUtil> implementations = new ConcurrentHashMap<>();

    protected static void registerImplementation(JobTypeEnum type, ExecutableUtil child) {
        implementations.put(type, child);
    }

    private static void registerDefaultImplementations() {
        registerImplementation(JobTypeEnum.INDEX_BUILD, new IndexBuildJobUtil());
        registerImplementation(JobTypeEnum.INDEX_MERGE, new MergeJobUtil());
        registerImplementation(JobTypeEnum.INDEX_REFRESH, new RefreshJobUtil());
        registerImplementation(JobTypeEnum.INC_BUILD, new SegmentBuildJobUtil());
        registerImplementation(JobTypeEnum.SUB_PARTITION_REFRESH, new RefreshJobUtil());
        registerImplementation(JobTypeEnum.SUB_PARTITION_BUILD, new PartitionBuildJobUtil());
        registerImplementation(JobTypeEnum.LAYOUT_DATA_OPTIMIZE, new LayoutOptimizeJobUtil());
    }

    public static ExecutableUtil getImplementation(JobTypeEnum type) {
        // Double-Checked Locking
        ExecutableUtil implementation = implementations.get(type);
        if (implementation == null) {
            synchronized (ExecutableUtil.class) {
                implementation = implementations.get(type);
                if (implementation == null) {
                    registerDefaultImplementations();
                    implementation = implementations.get(type);
                }
            }
        }
        return implementation;
    }

    public static void computeParams(JobParam jobParam) {
        val model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getDataModelDesc(jobParam.getModel());
        if (model != null && model.isMultiPartitionModel()) {
            jobParam.getCondition().put(JobParam.ConditionConstant.MULTI_PARTITION_JOB, true);
        }
        ExecutableUtil paramUtil = getImplementation(jobParam.getJobTypeEnum());
        if (paramUtil != null) {
            paramUtil.computeLayout(jobParam);
            if (jobParam.isMultiPartitionJob()) {
                paramUtil.computePartitions(jobParam);
            }
        }
    }

    public static void computeJobBucket(JobParam jobParam) {
        if (!jobParam.isMultiPartitionJob()) {
            return;
        }
        if (CollectionUtils.isEmpty(jobParam.getTargetPartitions())) {
            throw new KylinException(JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY);
        }
        Set<JobBucket> buckets = Sets.newHashSet();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dfm = NDataflowManager.getInstance(config, jobParam.getProject());
        NDataflow df = dfm.getDataflow(jobParam.getModel());
        NDataSegmentManager segmentManager = config.getManager(jobParam.getProject(), NDataSegmentManager.class);

        for (String targetSegment : jobParam.getTargetSegments()) {
            NDataSegment segment = df.getSegment(targetSegment);
            val bucketStart = new AtomicLong(segment.getMaxBucketId());
            Set<Long> partitions;
            // Different segments with different partitions will only happen in index build job.
            if (JobTypeEnum.INDEX_BUILD == jobParam.getJobTypeEnum()) {
                partitions = segment.getAllPartitionIds();
            } else {
                partitions = jobParam.getTargetPartitions();
            }
            jobParam.getProcessLayouts().forEach(layout -> partitions.forEach(partition -> buckets
                    .add(new JobBucket(segment.getId(), layout.getId(), bucketStart.incrementAndGet(), partition))));
            segmentManager.update(segment.getUuid(), copyForWrite -> copyForWrite.setMaxBucketId(bucketStart.get()));
        }
        jobParam.setTargetBuckets(buckets);
    }

    public void checkLayoutsNotEmpty(JobParam jobParam) {
        String enableAutoIndexPlan = jobParam.getExtParams().get(NBatchConstants.P_PLANNER_AUTO_APPROVE_ENABLED);
        if (Boolean.parseBoolean(enableAutoIndexPlan)) {
            return;
        }
        if (CollectionUtils.isEmpty(jobParam.getProcessLayouts())) {
            log.warn("JobParam {} is no longer valid because no layout awaits building", jobParam);
            throw new KylinException(getCheckIndexErrorCode());
        }
    }

    protected Set<LayoutEntity> filterTobeDelete(HashSet<LayoutEntity> layouts) {
        return layouts.stream().filter(layout -> !layout.isToBeDeleted()).collect(Collectors.toSet());
    }

    public ErrorCodeProducer getCheckIndexErrorCode() {
        return JOB_CREATE_CHECK_INDEX_FAIL;
    }

    public void computeLayout(JobParam jobParam) {
    }

    /**
     * Only multi partition model
     */
    public void computePartitions(JobParam jobParam) {
    }

    public static Map<String, String> removeGultenParams(Map<String, String> params) {
        params.computeIfPresent(SPARK_PLUGINS, (pluginKey, pluginValue) -> {
            String tempPluginValue = pluginValue;
            String comma = ",";
            if (StringUtils.contains(pluginValue, GLUTEN_PLUGIN)) {
                tempPluginValue = Arrays.stream(tempPluginValue.split(comma))
                        .filter(p -> !StringUtils.equals(p, GLUTEN_PLUGIN)).collect(Collectors.joining(comma));
            }
            return tempPluginValue;
        });
        params.computeIfPresent(SPARK_SHUFFLE_MANAGER, (pluginKey, pluginValue) -> {
            String tempPluginValue = pluginValue;
            if (StringUtils.equals(pluginValue, COLUMNAR_SHUFFLE_MANAGER)) {
                tempPluginValue = "sort";
            }
            return tempPluginValue;
        });
        return params.entrySet().stream().filter(e -> !e.getKey().startsWith(GLUTEN_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
