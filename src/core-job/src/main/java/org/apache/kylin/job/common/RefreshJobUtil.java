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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_SEGMENT_READY_FAIL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_REFRESH_CHECK_INDEX_FAIL;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.code.ErrorCodeProducer;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.SegmentPartition;

import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public class RefreshJobUtil extends ExecutableUtil {
    @Override
    public void computeLayout(JobParam jobParam) {
        NDataflow df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getDataflow(jobParam.getModel());
        NDataSegment newSeg = df.getSegment(jobParam.getSegment());
        List<NDataSegment> segments = df.getSegments().stream()
                .filter(segment -> segment.getSegRange().startStartMatch(newSeg.getSegRange()))
                .filter(segment -> segment.getSegRange().endEndMatch(newSeg.getSegRange()))
                .filter(segment -> SegmentStatusEnum.READY == segment.getStatus()
                        || SegmentStatusEnum.WARNING == segment.getStatus())
                .collect(Collectors.toList());
        if (segments.size() != 1) {
            throw new KylinException(JOB_CREATE_CHECK_SEGMENT_READY_FAIL);
        }
        val indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getIndexPlan(jobParam.getModel());
        val targetLayouts = jobParam.getTargetLayouts();
        HashSet<LayoutEntity> layouts = Sets.newHashSet();
        if (targetLayouts.isEmpty()) {
            val refreshAll = (Boolean) jobParam.getCondition().get(JobParam.ConditionConstant.REFRESH_ALL_LAYOUTS);
            if (refreshAll) {
                layouts.addAll(indexPlan.getAllLayouts());
            } else if (segments.get(0).getLayoutsMap().isEmpty() && !KylinConfig.getInstanceFromEnv().isUTEnv()) {
                throw new KylinException(JOB_CREATE_CHECK_INDEX_FAIL);
            } else {
                segments.get(0).getLayoutsMap().values().forEach(layout -> layouts.add(layout.getLayout()));
            }
        } else {
            indexPlan.getAllLayouts().forEach(layout -> {
                if (targetLayouts.contains(layout.getId())) {
                    layouts.add(layout);
                }
            });
        }
        jobParam.setProcessLayouts(filterTobeDelete(layouts));
        checkLayoutsNotEmpty(jobParam);
    }

    @Override
    public ErrorCodeProducer getCheckIndexErrorCode() {
        return JOB_REFRESH_CHECK_INDEX_FAIL;
    }

    @Override
    public void computePartitions(JobParam jobParam) {
        NDataflowManager dfm = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject());
        val df = dfm.getDataflow(jobParam.getModel()).copy();
        val segment = df.getSegment(jobParam.getSegment());
        if (JobTypeEnum.INDEX_REFRESH == jobParam.getJobTypeEnum()) {
            jobParam.setTargetPartitions(segment.getMultiPartitions().stream().map(SegmentPartition::getPartitionId)
                    .collect(Collectors.toSet()));
        }
    }
}
