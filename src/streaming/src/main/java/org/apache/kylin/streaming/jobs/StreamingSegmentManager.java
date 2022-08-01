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

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.streaming.request.StreamingSegmentRequest;
import org.apache.kylin.streaming.rest.RestSupport;
import org.apache.kylin.streaming.util.JobExecutionIdHolder;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.kafka010.OffsetRangeManager;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingSegmentManager {

    private static final String SEGMENT_POST_URL = "/streaming_jobs/dataflow/segment";

    public static NDataSegment allocateSegment(SparkSession ss, String dataflowId, String project, Long batchMinTime,
            Long batchMaxTime) {
        return allocateSegment(ss, null, dataflowId, project, batchMinTime, batchMaxTime);
    }

    public static NDataSegment allocateSegment(SparkSession ss, SegmentRange.KafkaOffsetPartitionedSegmentRange sr,
            String dataflowId, String project, Long batchMinTime, Long batchMaxTime) {
        if (sr == null) {
            val offsetRange = OffsetRangeManager.currentOffsetRange(ss);
            sr = new SegmentRange.KafkaOffsetPartitionedSegmentRange(batchMinTime, batchMaxTime,
                    OffsetRangeManager.partitionOffsets(offsetRange._1),
                    OffsetRangeManager.partitionOffsets(offsetRange._2));
        }
        NDataSegment newSegment;
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        if (config.isUTEnv()) {
            val segRange = sr;
            newSegment = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                return dfMgr.appendSegmentForStreaming(dfMgr.getDataflow(dataflowId), segRange);
            }, project);
            return newSegment;
        } else {
            StreamingSegmentRequest req = new StreamingSegmentRequest(project, dataflowId);
            req.setSegmentRange(sr);
            req.setNewSegId(RandomUtil.randomUUIDStr());
            req.setJobType(JobTypeEnum.STREAMING_BUILD.name());
            val jobId = StreamingUtils.getJobId(dataflowId, req.getJobType());
            req.setJobExecutionId(JobExecutionIdHolder.getJobExecutionId(jobId));
            try(RestSupport rest = new RestSupport(config)) {
                RestResponse<String> restResponse = rest.execute(rest.createHttpPost(SEGMENT_POST_URL), req);
                String newSegId = restResponse.getData();
                StreamingUtils.replayAuditlog();
                if (!StringUtils.isEmpty(newSegId)) {
                    NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
                    newSegment = dfMgr.getDataflow(dataflowId).getSegment(newSegId);
                    for (LayoutEntity layout : newSegment.getIndexPlan().getAllLayouts()) {
                        NDataLayout ly = NDataLayout.newDataLayout(newSegment.getDataflow(), newSegment.getId(),
                                layout.getId());
                        newSegment.getLayoutsMap().put(ly.getLayoutId(), ly);
                    }
                    return newSegment;
                } else {
                    val empSeg = NDataSegment.empty();
                    empSeg.setId(StringUtils.EMPTY);
                    return empSeg;
                }
            }
        }
    }

}
