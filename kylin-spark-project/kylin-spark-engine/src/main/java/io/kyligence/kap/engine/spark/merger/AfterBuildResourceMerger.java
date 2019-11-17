/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.merger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.SegmentUtils;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

@Slf4j
public class AfterBuildResourceMerger extends SparkJobMetadataMerger {


    public AfterBuildResourceMerger(KylinConfig config, String project) {
        super(config, project);
    }

    @Override
    public NDataLayout[] merge(String dataflowId, Set<String> segmentId, Set<Long> layoutIds, ResourceStore remoteResourceStore, JobTypeEnum jobType) {
        switch (jobType) {
            case INDEX_BUILD:
                return mergeAfterCatchup(dataflowId, segmentId, layoutIds, remoteResourceStore);
            case INC_BUILD:
                Preconditions.checkArgument(segmentId.size() == 1);
                return mergeAfterIncrement(dataflowId, segmentId.iterator().next(), layoutIds, remoteResourceStore);
            default:
                throw new UnsupportedOperationException("Error job type: " + jobType);
        }
    }

    @Override
    public void merge(AbstractExecutable abstractExecutable) {
        try (val buildResourceStore = ExecutableUtils.getRemoteStore(this.getConfig(), abstractExecutable)) {
            val dataFlowId = ExecutableUtils.getDataflowId(abstractExecutable);
            val segmentIds = ExecutableUtils.getSegmentIds(abstractExecutable);
            val layoutIds = ExecutableUtils.getLayoutIds(abstractExecutable);
            NDataLayout[] nDataLayouts = merge(dataFlowId, segmentIds, layoutIds, buildResourceStore, abstractExecutable.getJobType());
            recordDownJobStats(abstractExecutable, nDataLayouts);
            abstractExecutable.notifyUserIfNecessary(nDataLayouts);
        }
    }

    public NDataLayout[] mergeAfterIncrement(String flowName, String segmentId, Set<Long> layoutIds,
                                             ResourceStore remoteStore) {
        val localDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        val localDataflow = localDataflowManager.getDataflow(flowName);
        val remoteDataflowManager = NDataflowManager.getInstance(remoteStore.getConfig(), getProject());
        val remoteDataflow = remoteDataflowManager.getDataflow(flowName).copy();

        val dfUpdate = new NDataflowUpdate(flowName);
        val availableLayoutIds = intersectionWithLastSegment(localDataflow, layoutIds);
        val theSeg = remoteDataflow.getSegment(segmentId);
        updateSnapshotTableIfNeed(theSeg);
        theSeg.setStatus(SegmentStatusEnum.READY);
        dfUpdate.setToUpdateSegs(theSeg);
        dfUpdate.setToAddOrUpdateLayouts(theSeg.getSegDetails().getLayouts().stream()
                .filter(c -> availableLayoutIds.contains(c.getLayoutId())).toArray(NDataLayout[]::new));

        localDataflowManager.updateDataflow(dfUpdate);

        return dfUpdate.getToAddOrUpdateLayouts();
    }

    public NDataLayout[] mergeAfterCatchup(String flowName, Set<String> segmentIds, Set<Long> layoutIds,
                                           ResourceStore remoteStore) {
        val localDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        val localDataflow = localDataflowManager.getDataflow(flowName);
        val remoteDataflowManager = NDataflowManager.getInstance(remoteStore.getConfig(), getProject());
        val remoteDataflow = remoteDataflowManager.getDataflow(flowName).copy();

        val dataflow = localDataflowManager.getDataflow(flowName);
        val dfUpdate = new NDataflowUpdate(flowName);
        val addCuboids = Lists.<NDataLayout>newArrayList();

        val layoutInCubeIds = dataflow.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId)
                .collect(Collectors.toList());
        val availableLayoutIds = layoutIds.stream().filter(layoutInCubeIds::contains).collect(Collectors.toSet());
        for (String segId : segmentIds) {
            val localSeg = localDataflow.getSegment(segId);
            val remoteSeg = remoteDataflow.getSegment(segId);
            // ignore if local segment is not ready
            if (localSeg == null || localSeg.getStatus() != SegmentStatusEnum.READY) {
                continue;
            }
            updateSnapshotTableIfNeed(remoteSeg);
            for (long layoutId : availableLayoutIds) {
                NDataLayout dataCuboid = remoteSeg.getLayout(layoutId);
                Preconditions.checkNotNull(dataCuboid);
                addCuboids.add(dataCuboid);
            }
            dfUpdate.setToUpdateSegs(remoteSeg);
        }
        dfUpdate.setToAddOrUpdateLayouts(addCuboids.toArray(new NDataLayout[0]));

        localDataflowManager.updateDataflow(dfUpdate);

        return dfUpdate.getToAddOrUpdateLayouts();
    }


    private Set<Long> intersectionWithLastSegment(NDataflow dataflow, Collection<Long> layoutIds) {
        val layoutInSegmentIds = SegmentUtils.getToBuildLayouts(dataflow).stream().map(LayoutEntity::getId)
                .collect(Collectors.toSet());
        return layoutIds.stream().filter(layoutInSegmentIds::contains).collect(Collectors.toSet());
    }

}
                                            