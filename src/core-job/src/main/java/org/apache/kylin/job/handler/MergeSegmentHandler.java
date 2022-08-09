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

package org.apache.kylin.job.handler;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_SEGMENT_FAIL;
import static org.apache.kylin.job.factory.JobFactoryConstant.MERGE_JOB_FACTORY;

import java.util.HashSet;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public class MergeSegmentHandler extends AbstractJobHandler {
    @Override
    protected AbstractExecutable createJob(JobParam jobParam) {
        NDataflow df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getDataflow(jobParam.getModel());
        NDataSegment newSeg = df.getSegment(jobParam.getSegment());
        Set<NDataSegment> mergeSegment = new HashSet<>();
        mergeSegment.add(newSeg);
        return JobFactory.createJob(MERGE_JOB_FACTORY,
                new JobFactory.JobBuildParams(mergeSegment, jobParam.getProcessLayouts(), jobParam.getOwner(),
                        JobTypeEnum.INDEX_MERGE, jobParam.getJobId(), null, jobParam.getIgnoredSnapshotTables(),
                        jobParam.getTargetPartitions(), jobParam.getTargetBuckets(), jobParam.getExtParams()));
    }

    /**
     * Merge is abandoned when segment index is not aligned.
     */
    @Override
    protected void checkBeforeHandle(JobParam jobParam) {
        super.checkBeforeHandle(jobParam);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataflow df = NDataflowManager.getInstance(kylinConfig, jobParam.getProject())
                .getDataflow(jobParam.getModel());
        val segments = df.getSegments();
        NDataSegment newSegment = df.getSegment(jobParam.getSegment());
        Set<Long> layoutIds = null;
        for (val seg : segments) {
            if (seg.getId().equals(newSegment.getId())) {
                continue;
            }
            if (seg.getSegRange().overlaps(newSegment.getSegRange()) && null == layoutIds) {
                layoutIds = seg.getLayoutsMap().keySet();
            }
            if (seg.getSegRange().overlaps(newSegment.getSegRange())
                    && !seg.getLayoutsMap().keySet().equals(layoutIds)) {
                log.warn("Segment's layout is not matched,segID:{}, {} -> {}", seg.getId(), layoutIds,
                        seg.getLayoutsMap().keySet());
                throw new KylinException(JOB_CREATE_CHECK_SEGMENT_FAIL);
            }
        }
    }
}
