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

package org.apache.kylin.streaming.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.engine.spark.builder.NBuildSourceInfo;
import org.apache.kylin.engine.spark.job.BuildJobInfos;
import org.apache.kylin.engine.spark.job.DFBuildJob;
import org.apache.kylin.metadata.cube.cuboid.NSpanningTree;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.streaming.common.BuildJobEntry;
import org.apache.kylin.streaming.metadata.BuildLayoutWithRestUpdate;
import org.apache.kylin.streaming.request.StreamingSegmentRequest;
import org.apache.kylin.streaming.rest.RestSupport;
import org.apache.kylin.streaming.util.JobExecutionIdHolder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.val;

public class StreamingDFBuildJob extends DFBuildJob {

    private Map<Long, Dataset<Row>> cuboidDatasetMap;

    public StreamingDFBuildJob(String project) {
        buildLayoutWithUpdate = new BuildLayoutWithRestUpdate(JobTypeEnum.STREAMING_BUILD);
        config = KylinConfig.getInstanceFromEnv();
        dfMgr = NDataflowManager.getInstance(config, project);
        this.project = project;
    }

    public void streamBuild(BuildJobEntry buildJobEntry) throws IOException {

        if (this.ss == null) {
            this.ss = buildJobEntry.spark();
            ss.sparkContext().setLocalProperty("spark.sql.execution.id", null);
        }

        this.jobId = RandomUtil.randomUUIDStr();
        if (this.infos == null) {
            this.infos = new BuildJobInfos();
        }

        if (cuboidDatasetMap == null) {
            cuboidDatasetMap = new ConcurrentHashMap<>();
        }

        setParam(NBatchConstants.P_DATAFLOW_ID, buildJobEntry.dataflowId());

        Preconditions.checkState(buildJobEntry.toBuildTree().getRootIndexEntities().size() != 0,
                "streaming mast have one root index");

        val theRootLevelBuildInfos = new NBuildSourceInfo();
        theRootLevelBuildInfos.setFlattableDS(buildJobEntry.streamingFlatDS());
        theRootLevelBuildInfos.setSparkSession(ss);
        theRootLevelBuildInfos.setToBuildCuboids(buildJobEntry.toBuildTree().getRootIndexEntities());
        build(Sets.newHashSet(theRootLevelBuildInfos), buildJobEntry.batchSegment().getId(),
                buildJobEntry.toBuildTree());

        logger.info("start update segment");
        if (config.isUTEnv()) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                NDataflow newDF = dfMgr.getDataflow(buildJobEntry.dataflowId()).copy();
                NDataSegment segUpdate = newDF.getSegment(buildJobEntry.batchSegment().getId());
                segUpdate.setStatus(SegmentStatusEnum.READY);
                segUpdate.setSourceCount(buildJobEntry.flatTableCount());
                val dfUpdate = new NDataflowUpdate(buildJobEntry.dataflowId());
                dfUpdate.setToUpdateSegs(segUpdate);
                dfUpdate.setStatus(RealizationStatusEnum.ONLINE);
                dfMgr.updateDataflow(dfUpdate);
                return 0;
            }, project);
        } else {
            updateSegment(buildJobEntry);
        }
        this.infos.clear();
        cuboidDatasetMap.clear();
    }

    public void updateSegment(BuildJobEntry buildJobEntry) {
        String url = "/streaming_jobs/dataflow/segment";
        StreamingSegmentRequest req = new StreamingSegmentRequest(project, buildJobEntry.dataflowId(),
                buildJobEntry.flatTableCount());
        req.setNewSegId(buildJobEntry.batchSegment().getId());
        req.setStatus(RealizationStatusEnum.ONLINE.name());
        req.setJobType(JobTypeEnum.STREAMING_BUILD.name());
        val jobId = StreamingUtils.getJobId(buildJobEntry.dataflowId(), req.getJobType());
        req.setJobExecutionId(JobExecutionIdHolder.getJobExecutionId(jobId));
        try (RestSupport rest = createRestSupport()) {
            rest.execute(rest.createHttpPut(url), req);
        }
        StreamingUtils.replayAuditlog();
    }

    public RestSupport createRestSupport() {
        return new RestSupport(config);
    }

    @Override
    protected List<NBuildSourceInfo> constructTheNextLayerBuildInfos(//
            NSpanningTree st, //
            NDataSegment seg, //
            Collection<IndexEntity> allIndexesInCurrentLayer) { //
        val childrenBuildSourceInfos = new ArrayList<NBuildSourceInfo>();
        for (IndexEntity index : allIndexesInCurrentLayer) {
            val children = st.getChildrenByIndexPlan(index);
            if (!children.isEmpty()) {
                val theRootLevelBuildInfos = new NBuildSourceInfo();
                theRootLevelBuildInfos.setSparkSession(ss);
                LayoutEntity layout = new ArrayList<>(st.getLayouts(index)).get(0);
                val parentDataset = cuboidDatasetMap.get(layout.getId());
                theRootLevelBuildInfos.setLayoutId(layout.getId());
                theRootLevelBuildInfos.setToBuildCuboids(children);
                theRootLevelBuildInfos.setFlattableDS(parentDataset);
                childrenBuildSourceInfos.add(theRootLevelBuildInfos);
            }
        }
        // return the next to be built layer.
        return childrenBuildSourceInfos;
    }

    @Override
    protected NDataLayout saveAndUpdateLayout(Dataset<Row> dataset, NDataSegment seg, LayoutEntity layout)
            throws IOException {
        cuboidDatasetMap.put(layout.getId(), dataset);
        return super.saveAndUpdateLayout(dataset, seg, layout);
    }

    public NDataSegment getSegment(String segId) {
        // ensure the seg is the latest.
        StreamingUtils.replayAuditlog();
        return super.getSegment(segId);
    }

    public void shutdown() {
        if (buildLayoutWithUpdate != null) {
            buildLayoutWithUpdate.shutDown();
        }
    }
}
