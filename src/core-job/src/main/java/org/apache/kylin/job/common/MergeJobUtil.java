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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_EXCEPTION;

import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.SegmentPartition;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public class MergeJobUtil extends ExecutableUtil {

    static {
        registerImplementation(JobTypeEnum.INDEX_MERGE, new MergeJobUtil());
    }

    @Override
    public void computeLayout(JobParam jobParam) {
        NDataflow df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getDataflow(jobParam.getModel());
        NDataSegment newSeg = df.getSegment(jobParam.getSegment());
        HashSet<LayoutEntity> layouts = Sets.newHashSet();
        val oldSegs = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING).stream()
                .filter(seg -> seg.getSegRange().overlaps(newSeg.getSegRange())).collect(Collectors.toList());
        if (oldSegs.isEmpty()) {
            log.warn("JobParam {} is no longer valid because no old segment ready", jobParam);
            throw new KylinException(JOB_CREATE_EXCEPTION);
        }

        for (Map.Entry<Long, NDataLayout> cuboid : oldSegs.get(0).getLayoutsMap().entrySet()) {
            layouts.add(cuboid.getValue().getLayout());
        }
        if (layouts.isEmpty() && !KylinConfig.getInstanceFromEnv().isUTEnv()) {
            log.warn("JobParam {} is no longer valid because no layout awaits building", jobParam);
            throw new KylinException(JOB_CREATE_EXCEPTION);
        }
        jobParam.setProcessLayouts(layouts);
    }

    @Override
    public void computePartitions(JobParam jobParam) {
        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getDataflow(jobParam.getModel());
        NDataSegment newSeg = df.getSegment(jobParam.getSegment());
        jobParam.setTargetPartitions(
                newSeg.getMultiPartitions().stream().map(SegmentPartition::getPartitionId).collect(Collectors.toSet()));
    }

}
