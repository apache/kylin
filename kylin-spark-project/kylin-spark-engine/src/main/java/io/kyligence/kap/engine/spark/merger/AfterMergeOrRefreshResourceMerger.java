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

package io.kyligence.kap.engine.spark.merger;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.clearspring.analytics.util.Lists;

import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import lombok.val;

public class AfterMergeOrRefreshResourceMerger extends SparkJobMetadataMerger {

    public AfterMergeOrRefreshResourceMerger(KylinConfig config, String project) {
        super(config, project);
    }

    @Override
    public NDataLayout[] merge(String dataflowId, Set<String> segmentIds, Set<Long> layoutIds,
            ResourceStore remoteResourceStore, JobTypeEnum jobType) {

        NDataflowManager mgr = NDataflowManager.getInstance(getConfig(), getProject());
        NDataflowUpdate update = new NDataflowUpdate(dataflowId);

        NDataflowManager distMgr = NDataflowManager.getInstance(remoteResourceStore.getConfig(), getProject());
        NDataflow distDataflow = distMgr.getDataflow(update.getDataflowId()).copy(); // avoid changing cached objects

        List<NDataSegment> toUpdateSegments = Lists.newArrayList();
        List<NDataLayout> toUpdateCuboids = Lists.newArrayList();

        NDataSegment mergedSegment = distDataflow.getSegment(segmentIds.iterator().next());

        if (mergedSegment.getStatus() == SegmentStatusEnum.NEW)
            mergedSegment.setStatus(SegmentStatusEnum.READY);

        toUpdateSegments.add(mergedSegment);
        if (JobTypeEnum.INDEX_REFRESH.equals(jobType)) {
            updateSnapshotTableIfNeed(mergedSegment);
        }
        // only add layouts which still in segments, others maybe deleted by user
        List<NDataSegment> toRemoveSegments = distMgr.getToRemoveSegs(distDataflow, mergedSegment);
        if (JobTypeEnum.INDEX_MERGE.equals(jobType)) {
            Optional<Long> reduce = toRemoveSegments.stream()
                    .map(NDataSegment::getSourceBytesSize)
                    .filter(size -> size != -1)
                    .reduce(Long::sum);
            if (reduce.isPresent()) {
                long totalSourceSize = reduce.get();
                mergedSegment.setSourceBytesSize(totalSourceSize);
                mergedSegment.setLastBuildTime(System.currentTimeMillis());
            }
        }
        val livedLayouts = mgr.getDataflow(dataflowId).getLatestReadySegment().getLayoutsMap().values().stream()
                .map(NDataLayout::getLayoutId).collect(Collectors.toSet());
        toUpdateCuboids.addAll(mergedSegment.getSegDetails().getLayouts().stream()
                .filter(c -> livedLayouts.contains(c.getLayoutId())).collect(Collectors.toList()));

        update.setToAddOrUpdateLayouts(toUpdateCuboids.toArray(new NDataLayout[0]));
        update.setToRemoveSegs(toRemoveSegments.toArray(new NDataSegment[0]));
        update.setToUpdateSegs(toUpdateSegments.toArray(new NDataSegment[0]));

        mgr.updateDataflow(update);

        return update.getToAddOrUpdateLayouts();
    }

    @Override
    public void merge(AbstractExecutable abstractExecutable) {
        try (val buildResourceStore = ExecutableUtils.getRemoteStore(this.getConfig(), abstractExecutable)) {
            val dataFlowId = ExecutableUtils.getDataflowId(abstractExecutable);
            val segmentIds = ExecutableUtils.getSegmentIds(abstractExecutable);
            val layoutIds = ExecutableUtils.getLayoutIds(abstractExecutable);
            NDataLayout[] nDataLayouts = merge(dataFlowId, segmentIds, layoutIds, buildResourceStore,
                    abstractExecutable.getJobType());
            recordDownJobStats(abstractExecutable, nDataLayouts);
            abstractExecutable.notifyUserIfNecessary(nDataLayouts);
        }
    }

}
