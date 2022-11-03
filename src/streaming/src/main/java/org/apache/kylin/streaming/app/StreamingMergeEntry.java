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

package org.apache.kylin.streaming.app;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.streaming.common.MergeJobEntry;
import org.apache.kylin.streaming.constants.StreamingConstants;
import org.apache.kylin.streaming.jobs.StreamingDFMergeJob;
import org.apache.kylin.streaming.jobs.SyncMerger;
import org.apache.kylin.streaming.merge.CatchupMergePolicy;
import org.apache.kylin.streaming.merge.MergePolicy;
import org.apache.kylin.streaming.merge.NormalMergePolicy;
import org.apache.kylin.streaming.merge.PeakMergePolicy;
import org.apache.kylin.streaming.request.StreamingSegmentRequest;
import org.apache.kylin.streaming.rest.RestSupport;
import org.apache.kylin.streaming.util.JobExecutionIdHolder;
import org.apache.kylin.streaming.util.JobKiller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import lombok.val;
import lombok.var;

public class StreamingMergeEntry extends StreamingMergeApplication {
    private static final Logger logger = LoggerFactory.getLogger(StreamingMergeEntry.class);
    private static final AtomicLong globalMergeTime = new AtomicLong(0);
    private static AtomicBoolean gracefulStop = new AtomicBoolean(false);
    private static CountDownLatch latch = new CountDownLatch(1);

    private StreamingDFMergeJob merger = new StreamingDFMergeJob();
    private CatchupMergePolicy catchupMergePolicy = new CatchupMergePolicy();
    private NormalMergePolicy normalMergePolicy = new NormalMergePolicy();
    private PeakMergePolicy peakMergePolicy = new PeakMergePolicy();

    private AtomicLong hdfsFileScanStartTime = new AtomicLong(System.currentTimeMillis());

    public static void main(String[] args) {
        StreamingMergeEntry entry = new StreamingMergeEntry();
        entry.execute(args);
    }

    public static void stop() {
        gracefulStop.set(true);
    }

    public void doExecute() throws ExecuteException {
        setStopFlag(false);
        logger.info("StreamingMergeEntry:{},{},{},{},{}", project, dataflowId, thresholdOfSegSize, numberOfSeg,
                distMetaUrl);

        var isError = false;
        try {
            while (isRunning()) {
                process(project, dataflowId);
                if (!isGracefulShutdown(project, jobId)) {
                    StreamingUtils.sleep(kylinConfig.getStreamingSegmentMergeInterval() * 1000);
                } else {
                    setStopFlag(true);
                    logger.info("begin to shutdown streaming merge job ({}:{})", project, dataflowId);
                }
            }
            closeSparkSession();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            isError = true;
            JobKiller.killApplication(jobId);
            throw new ExecuteException("streaming merging segment error occured: ", e);
        } finally {
            close(isError);
        }
    }

    @Override
    public boolean getStopFlag() {
        return gracefulStop.get();
    }

    @Override
    public void setStopFlag(boolean stopFlag) {
        gracefulStop.set(stopFlag);
    }

    public void process(String project, String dataflowId) {
        // Step1. catch up metadata
        StreamingUtils.replayAuditlog();
        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataflow dataflow = dfMgr.getDataflow(dataflowId);
        Segments<NDataSegment> segments = dataflow.getSegments().getSegments(SegmentStatusEnum.READY,
                SegmentStatusEnum.WARNING);
        // step2. sort segment
        Collections.sort(segments);
        removeLastL0Segment(segments);

        final AtomicInteger currLayer = new AtomicInteger(0);
        // step3. choose a merge policy
        MergePolicy policy = selectPolicy(segments, currLayer.get());

        // step4. start merge action
        while (policy != null && policy.matchMergeCondition(thresholdOfSegSize)) {
            // step4.1 get candidate list
            List<NDataSegment> segList = policy.getMatchSegList();
            // step4.2 merge target segments
            NDataSegment afterMergeSeg = mergeSegments(project, dataflowId, segList, currLayer.get());
            policy.next(currLayer);
            dataflow = dfMgr.getDataflow(dataflowId);
            segments = dataflow.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING, SegmentStatusEnum.NEW);
            removeLastL0Segment(segments);
            NDataSegment seg = getSegment(segments, afterMergeSeg, project, dataflowId);
            if (seg.getStatus() == SegmentStatusEnum.NEW) {
                break;
            }
            if (seg.getStorageBytesSize() > thresholdOfSegSize) {
                logger.info("SegmentId={} size ({}) exceeds threshold", seg.getId(), seg.getStorageBytesSize());
                break;
            } else {
                for (NDataSegment item : segList) {
                    putHdfsFile(item.getId(),
                            new Pair<>(dataflow.getSegmentHdfsPath(item.getId()), System.currentTimeMillis()));
                }
            }
            policy = selectPolicy(segments, currLayer.get());
            clearHdfsFiles(dfMgr.getDataflow(dataflowId), hdfsFileScanStartTime);
        }
    }

    public NDataSegment mergeSegments(String project, String dataflowId, List<NDataSegment> retainSegments,
            int currLayer) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        int retry = 0;
        while (retry++ < 3) {
            try {
                return allocateSegment(project, dataflowId, retainSegments, currLayer);
            } catch (KylinException e) {
                logger.error(e.getMessage(), e);
                StreamingUtils.sleep(config.getStreamingSegmentMergeInterval() * 1000 * retry);
            }
        }
        throw new KylinException(ServerErrorCode.SEGMENT_MERGE_FAILURE, project + "/" + dataflowId);
    }

    public NDataSegment getSegment(Segments<NDataSegment> segments, NDataSegment afterMergeSeg, String project,
            String dataflowId) {
        NDataSegment seg = segments.getSegment(afterMergeSeg.getName(), SegmentStatusEnum.READY);
        if (seg == null) {
            seg = segments.getSegment(afterMergeSeg.getName(), SegmentStatusEnum.WARNING);
        }
        if (seg == null) {
            seg = segments.getSegment(afterMergeSeg.getName(), SegmentStatusEnum.NEW);
            val config = KylinConfig.getInstanceFromEnv();
            if (seg != null && !config.isUTEnv()) {
                removeSegment(project, dataflowId, seg);
            }
        }
        if (seg == null) {
            throw new KylinException(ServerErrorCode.SEGMENT_MERGE_FAILURE, "segment is null");
        }
        return seg;
    }

    public void removeSegment(String project, String dataflowId, NDataSegment seg) {
        String url = "/streaming_jobs/dataflow/segment/deletion";
        StreamingSegmentRequest req = new StreamingSegmentRequest(project, dataflowId);
        req.setRemoveSegment(Arrays.asList(seg));
        req.setJobType(jobType.name());
        req.setJobExecutionId(JobExecutionIdHolder.getJobExecutionId(jobId));
        try (RestSupport rest = createRestSupport(KylinConfig.getInstanceFromEnv())) {
            rest.execute(rest.createHttpPost(url), req);
        }
        StreamingUtils.replayAuditlog();
    }

    public NDataSegment allocateSegment(String project, String dataflowId, List<NDataSegment> retainSegments,
            int currLayer) {
        // step1. getSegmentRange to merge
        // get partition & offset from target segments and get min/max time
        Map<Integer, Long> sourcePartitionOffsetStart = Maps.newHashMap();
        Map<Integer, Long> sourcePartitionOffsetEnd = Maps.newHashMap();
        AtomicLong minTime = new AtomicLong(Long.MAX_VALUE);
        AtomicLong maxTime = new AtomicLong(0L);

        retainSegments.forEach(seg -> {
            val range = seg.getKSRange();
            if (range.getStart() != null && range.getStart() < minTime.get()) {
                minTime.set(range.getStart());
            }
            if (range.getEnd() != null && range.getEnd() > maxTime.get()) {
                maxTime.set(range.getEnd());
            }

            range.getSourcePartitionOffsetStart().forEach((partition, offset) -> {
                if (!sourcePartitionOffsetStart.containsKey(partition)
                        || sourcePartitionOffsetStart.get(partition) > offset) {
                    sourcePartitionOffsetStart.put(partition, offset);
                }
            });
            range.getSourcePartitionOffsetEnd().forEach((partition, offset) -> {
                if (!sourcePartitionOffsetEnd.containsKey(partition)
                        || sourcePartitionOffsetEnd.get(partition) < offset) {
                    sourcePartitionOffsetEnd.put(partition, offset);
                }
            });
        });

        val rangeToMerge = new SegmentRange.KafkaOffsetPartitionedSegmentRange(minTime.get(), maxTime.get(),
                sourcePartitionOffsetStart, sourcePartitionOffsetEnd);

        KylinConfig config = KylinConfig.getInstanceFromEnv();

        // step2. create new segment(after merge segment) and update metadata
        NDataSegment afterMergeSeg;
        if (config.isUTEnv()) {
            afterMergeSeg = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                return dfMgr.mergeSegments(dfMgr.getDataflow(dataflowId), rangeToMerge, true, currLayer + 1, null);
            }, project);
        } else {
            afterMergeSeg = doMergeStreamingSegment(project, dataflowId, rangeToMerge, currLayer);
        }

        // step3. execute merge segment action!
        logger.info("start sync thread for merge");

        List<NDataSegment> updatedSegments = null;
        val dfMgr = NDataflowManager.getInstance(config, project);
        final NDataflow df = dfMgr.getDataflow(dataflowId);
        updatedSegments = retainSegments.stream().map(seg -> df.getSegment(seg.getId())).collect(Collectors.toList());
        long afterMergeSegSourceCount = retainSegments.stream().mapToLong(NDataSegment::getSourceCount).sum();
        logger.info("afterMergeSegment[{}] layer={}  from {}", afterMergeSeg, currLayer, updatedSegments);

        val mergeJobEntry = new MergeJobEntry(ss, project, dataflowId, afterMergeSegSourceCount, globalMergeTime,
                updatedSegments, afterMergeSeg);
        SyncMerger syncMerge = new SyncMerger(mergeJobEntry);
        syncMerge.run(merger);
        return afterMergeSeg;
    }

    public NDataSegment doMergeStreamingSegment(String project, String dataflowId,
            SegmentRange.KafkaOffsetPartitionedSegmentRange rangeToMerge, int currLayer) {
        val config = KylinConfig.getInstanceFromEnv();
        String url = "/streaming_jobs/dataflow/segment";
        StreamingSegmentRequest req = new StreamingSegmentRequest(project, dataflowId);
        req.setSegmentRange(rangeToMerge);
        req.setLayer(String.valueOf(currLayer));
        req.setNewSegId(RandomUtil.randomUUIDStr());
        req.setJobType(jobType.name());
        req.setJobExecutionId(JobExecutionIdHolder.getJobExecutionId(jobId));

        try (RestSupport rest = createRestSupport(config)) {
            RestResponse<String> restResponse = rest.execute(rest.createHttpPost(url), req);
            String newSegId = restResponse.getData();
            // catch up local metadata
            StreamingUtils.replayAuditlog();
            NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            return dfMgr.getDataflow(dataflowId).getSegment(newSegId);
        }
    }

    private MergePolicy selectPolicy(Segments<NDataSegment> segments, int layer) {
        Collections.sort(segments);
        if (!peakMergePolicy.selectMatchedSegList(segments, layer, thresholdOfSegSize, numberOfSeg).isEmpty()) {
            return peakMergePolicy;
        }
        if (!catchupMergePolicy.selectMatchedSegList(segments, layer, thresholdOfSegSize, numberOfSeg).isEmpty()) {
            return catchupMergePolicy;
        }
        if (!normalMergePolicy.selectMatchedSegList(segments, layer, thresholdOfSegSize, numberOfSeg).isEmpty()) {
            return normalMergePolicy;
        }
        return null;
    }

    private void removeLastL0Segment(Segments<NDataSegment> segments) {
        if (!segments.isEmpty()) {
            val additionInfo = segments.get(segments.size() - 1).getAdditionalInfo();
            if (additionInfo != null && !additionInfo.containsKey(StreamingConstants.FILE_LAYER))
                segments.remove(segments.size() - 1);
        }
    }

    private void close(boolean isError) {
        merger.shutdown();
        latch.countDown();
        closeAuditLogStore(ss);
        if (!isError) {
            systemExit(0);
        }
    }
}
