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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_SEGMENT_READY_FAIL;

import java.util.HashSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;

import com.google.common.collect.Sets;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public class IndexBuildJobUtil extends ExecutableUtil {
    @Override
    public void computeLayout(JobParam jobParam) {
        NDataflow df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getDataflow(jobParam.getModel());
        IndexPlan indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getIndexPlan(jobParam.getModel());
        final HashSet<LayoutEntity> toBeProcessedLayouts = Sets.newLinkedHashSet();
        final HashSet<LayoutEntity> toBeDeletedLayouts = Sets.newLinkedHashSet();

        var readySegs = new Segments<>(df.getSegments(jobParam.getTargetSegments()));
        if (readySegs.isEmpty()) {
            log.warn("JobParam {} is no longer valid because no ready segment exists in target index_plan {}", jobParam,
                    jobParam.getModel());
            throw new KylinException(JOB_CREATE_CHECK_SEGMENT_READY_FAIL);
        }
        val mixLayouts = SegmentUtil.intersectionLayouts(readySegs);
        var allLayouts = indexPlan.getAllLayouts();
        val targetLayouts = jobParam.getTargetLayouts();

        if (targetLayouts.isEmpty()) {
            allLayouts.forEach(layout -> {
                if (layout.isToBeDeleted()) {
                    toBeDeletedLayouts.add(layout);
                } else if (!mixLayouts.contains(layout.getId())) {
                    toBeProcessedLayouts.add(layout);
                }
            });
        } else {
            allLayouts.forEach(layout -> {
                long layoutId = layout.getId();
                if (targetLayouts.contains(layoutId) && !mixLayouts.contains(layoutId)) {
                    toBeProcessedLayouts.add(layout);
                }
            });
        }
        jobParam.setProcessLayouts(filterTobeDelete(toBeProcessedLayouts));
        jobParam.setDeleteLayouts(toBeDeletedLayouts);
    }

    @Override
    public void computePartitions(JobParam jobParam) {
        NDataflowManager dfm = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject());
        val df = dfm.getDataflow(jobParam.getModel()).copy();
        val segments = df.getSegments(jobParam.getTargetSegments());
        val partitionIds = Sets.<Long> newHashSet();
        segments.forEach(segment -> {
            if (CollectionUtils.isEmpty(segment.getAllPartitionIds())) {
                throw new KylinException(JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY);
            }
            partitionIds.addAll(segment.getAllPartitionIds());
        });
        jobParam.setTargetPartitions(partitionIds);
    }
}
