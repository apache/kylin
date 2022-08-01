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

import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.streaming.common.MergeJobEntry;
import org.apache.kylin.streaming.request.StreamingSegmentRequest;
import org.apache.kylin.streaming.rest.RestSupport;
import org.apache.kylin.streaming.util.JobExecutionIdHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class SyncMerger {
    private static final Logger logger = LoggerFactory.getLogger(SyncMerger.class);

    private MergeJobEntry mergeJobEntry;

    public SyncMerger(MergeJobEntry mergeJobEntry) {
        this.mergeJobEntry = mergeJobEntry;
    }

    public void run(StreamingDFMergeJob merger) {
        logger.info("start merge streaming segment");
        logger.info(mergeJobEntry.toString());

        val start = System.currentTimeMillis();
        try {
            merger.streamingMergeSegment(mergeJobEntry);
            logger.info("merge segment cost {}", System.currentTimeMillis() - start);
            logger.info("delete merged segment and change the status");
            mergeJobEntry.globalMergeTime().set(System.currentTimeMillis() - start);

            val config = KylinConfig.getInstanceFromEnv();
            if (config.isUTEnv()) {
                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(),
                            mergeJobEntry.project());
                    NDataflow copy = dfMgr.getDataflow(mergeJobEntry.dataflowId()).copy();
                    val seg = copy.getSegment(mergeJobEntry.afterMergeSegment().getId());
                    seg.setStatus(SegmentStatusEnum.READY);
                    seg.setSourceCount(mergeJobEntry.afterMergeSegmentSourceCount());
                    val dfUpdate = new NDataflowUpdate(mergeJobEntry.dataflowId());
                    dfUpdate.setToUpdateSegs(seg);
                    List<NDataSegment> toRemoveSegs = mergeJobEntry.unMergedSegments();
                    dfUpdate.setToRemoveSegs(toRemoveSegs.toArray(new NDataSegment[0]));
                    dfMgr.updateDataflow(dfUpdate);
                    return 0;
                }, mergeJobEntry.project());
            } else {
                String url = "/streaming_jobs/dataflow/segment";
                StreamingSegmentRequest req = new StreamingSegmentRequest(mergeJobEntry.project(),
                        mergeJobEntry.dataflowId(), mergeJobEntry.afterMergeSegmentSourceCount());
                req.setRemoveSegment(mergeJobEntry.unMergedSegments());
                req.setNewSegId(mergeJobEntry.afterMergeSegment().getId());
                req.setJobType(JobTypeEnum.STREAMING_MERGE.name());
                val jobId = StreamingUtils.getJobId(mergeJobEntry.dataflowId(), req.getJobType());
                req.setJobExecutionId(JobExecutionIdHolder.getJobExecutionId(jobId));
                try (RestSupport rest = new RestSupport(config)) {
                    rest.execute(rest.createHttpPut(url), req);
                }
                StreamingUtils.replayAuditlog();
            }
        } catch (Exception e) {
            logger.info("merge failed reason: {} stackTrace is: {}", e.toString(), e.getStackTrace());
            val config = KylinConfig.getInstanceFromEnv();
            if (!config.isUTEnv()) {
                String url = "/streaming_jobs/dataflow/segment/deletion";
                StreamingSegmentRequest req = new StreamingSegmentRequest(mergeJobEntry.project(),
                        mergeJobEntry.dataflowId());
                req.setRemoveSegment(Collections.singletonList(mergeJobEntry.afterMergeSegment()));
                req.setJobType(JobTypeEnum.STREAMING_MERGE.name());
                val jobId = StreamingUtils.getJobId(mergeJobEntry.dataflowId(), req.getJobType());
                req.setJobExecutionId(JobExecutionIdHolder.getJobExecutionId(jobId));
                try (RestSupport rest = new RestSupport(config)) {
                    rest.execute(rest.createHttpPost(url), req);
                }
                StreamingUtils.replayAuditlog();
            }
            throw new KylinException(ServerErrorCode.SEGMENT_MERGE_FAILURE, mergeJobEntry.afterMergeSegment().getId());
        }
    }

}
