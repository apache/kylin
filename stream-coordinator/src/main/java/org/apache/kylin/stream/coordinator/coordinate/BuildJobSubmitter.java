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
package org.apache.kylin.stream.coordinator.coordinate;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.StreamingCubingEngine;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.stream.coordinator.StreamingCubeInfo;
import org.apache.kylin.stream.coordinator.coordinate.annotations.NonSideEffect;
import org.apache.kylin.stream.coordinator.coordinate.annotations.NotAtomicIdempotent;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.SegmentBuildState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * <pre>
 * The main responsibility of BuildJobSubmitter including:
 *  1. Try to find candidate segment which ready to submit a build job
 *  2. Trace the status of candidate segment's build job and promote segment if it is has met requirements
 *
 * The candidate segments are those segment what can be saw/perceived by coordinator,
 *  candidate segment could be divided into following state/queue:
 *  1. segment which data are uploaded PARTLY
 *  2. segment which data are uploaded completely and WAITING to build
 *  3. segment which in BUILDING state, job's state should be one of (NEW/RUNNING/ERROR/DISCARD)
 *  4. segment which built succeed and wait to deliver to historical part (and to be deleted in realtime part)
 *  5. segment which in historical part(HBase Ready Segment)
 *
 * By design, segment should transfer to next queue in sequential way(shouldn't jump the queue), do not break this.
 * </pre>
 */
public class BuildJobSubmitter implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BuildJobSubmitter.class);

    private ConcurrentMap<String, ConcurrentSkipListSet<SegmentJobBuildInfo>> segmentBuildJobCheckList = Maps
            .newConcurrentMap();
    private Set<String> cubeCheckList = new ConcurrentSkipListSet<>();
    private StreamingCoordinator coordinator;

    private long checkTimes = 0;

    public BuildJobSubmitter(StreamingCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    void restore() {
        logger.info("Restore job submitter");
        List<String> cubes = coordinator.getStreamMetadataStore().getCubes();
        for (String cube : cubes) {
            List<SegmentBuildState> segmentBuildStates = coordinator.getStreamMetadataStore()
                    .getSegmentBuildStates(cube);
            Collections.sort(segmentBuildStates);
            for (SegmentBuildState segmentBuildState : segmentBuildStates) {
                if (segmentBuildState.isInBuilding()) {
                    SegmentJobBuildInfo jobBuildInfo = new SegmentJobBuildInfo(cube, segmentBuildState.getSegmentName(),
                            segmentBuildState.getState().getJobId());
                    this.addToJobTrackList(jobBuildInfo);
                }
            }
        }
    }

    void addToJobTrackList(SegmentJobBuildInfo segmentBuildJob) {
        ConcurrentSkipListSet<SegmentJobBuildInfo> buildInfos = segmentBuildJobCheckList.get(segmentBuildJob.cubeName);
        if (buildInfos == null) {
            buildInfos = new ConcurrentSkipListSet<>();
            ConcurrentSkipListSet<SegmentJobBuildInfo> previousValue = segmentBuildJobCheckList
                    .putIfAbsent(segmentBuildJob.cubeName, buildInfos);
            if (previousValue != null) {
                buildInfos = previousValue;
            }
        }
        buildInfos.add(segmentBuildJob);
    }

    void addToCheckList(String cubeName) {
        cubeCheckList.add(cubeName);
    }

    void clearCheckList(String cubeName) {
        cubeCheckList.remove(cubeName);
        segmentBuildJobCheckList.remove(cubeName);
    }

    @Override
    public void run() {
        try {
            if (coordinator.isLead()) {
                doRun();
            }
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        }
    }

    void doRun() {
        checkTimes++;
        logger.info("\n----------------------------------------------------------- {}", checkTimes);
        List<SegmentJobBuildInfo> successJobs = traceEarliestSegmentBuildJob();

        for (SegmentJobBuildInfo successJob : successJobs) {
            ConcurrentSkipListSet<SegmentJobBuildInfo> submittedBuildJobs = segmentBuildJobCheckList
                    .get(successJob.cubeName);
            submittedBuildJobs.remove(successJob);
        }

        findSegmentReadyToBuild();

        if (checkTimes % 100 == 1) {
            logger.info("Force traverse all cubes periodically.");
            for (StreamingCubeInfo cubeInfo : coordinator.getEnableStreamingCubes()) {
                List<String> segmentList = checkSegmentBuidJobFromMetadata(cubeInfo.getCubeName());
                for (String segmentName : segmentList) {
                    submitSegmentBuildJob(cubeInfo.getCubeName(), segmentName);
                }
            }
        }
    }

    /**
     * <pre>
     * Trace the state of build job for the earliest(NOT ALL) segment for each streaming cube, and
     *  1. try to promote into Ready HBase Segment if job's state is succeed
     *  2. try to resume the build job if job's state is error
     * </pre>
     * 
     * @return all succeed building job
     */
    @NonSideEffect
    List<SegmentJobBuildInfo> traceEarliestSegmentBuildJob() {
        List<SegmentJobBuildInfo> successJobs = Lists.newArrayList();
        for (ConcurrentSkipListSet<SegmentJobBuildInfo> buildInfos : segmentBuildJobCheckList.values()) {
            if (buildInfos.isEmpty()) {
                continue;
            }

            // find the earliest segment build job and try to promote
            SegmentJobBuildInfo segmentBuildJob = buildInfos.first();
            logger.debug("Check the cube:{} segment:{} build status.", segmentBuildJob.cubeName,
                    segmentBuildJob.segmentName);
            try {
                CubingJob cubingJob = (CubingJob) coordinator.getExecutableManager().getJob(segmentBuildJob.jobID);
                if (cubingJob == null) {
                    logger.error("Cannot find metadata of current job.");
                    continue;
                }
                ExecutableState jobState = cubingJob.getStatus();
                if (ExecutableState.SUCCEED.equals(jobState)) {
                    CubeManager cubeManager = coordinator.getCubeManager();
                    CubeInstance cubeInstance = cubeManager.getCube(segmentBuildJob.cubeName).latestCopyForWrite();
                    CubeSegment cubeSegment = cubeInstance.getSegment(segmentBuildJob.segmentName, null);
                    logger.info("The cube:{} segment:{} is ready to be promoted.", segmentBuildJob.cubeName,
                            segmentBuildJob.segmentName);
                    coordinator.getClusterManager().segmentBuildComplete(cubingJob, cubeInstance, cubeSegment,
                            segmentBuildJob);
                    addToCheckList(cubeInstance.getName());
                    successJobs.add(segmentBuildJob);
                } else if (ExecutableState.ERROR.equals(jobState)) {
                    if (segmentBuildJob.retryCnt < 5) {
                        logger.info("Job:{} is error, resume the job.", segmentBuildJob);
                        coordinator.getExecutableManager().resumeJob(segmentBuildJob.jobID);
                        segmentBuildJob.retryCnt++;
                    } else {
                        logger.warn("Job:{} is error, exceed max retry. Kylin admin could resume it or discard it"
                                + "(to let new building job be sumbitted) .", segmentBuildJob);
                    }
                } else {
                    logger.debug("Current job state {}", jobState);
                }
            } catch (Exception e) {
                logger.error("Error when check streaming segment job build state:" + segmentBuildJob, e);
            }
        }
        return successJobs;
    }

    @NonSideEffect
    void findSegmentReadyToBuild() {
        Iterator<String> iterator = cubeCheckList.iterator();
        while (iterator.hasNext()) {
            String cubeName = iterator.next();
            List<String> segmentList = checkSegmentBuidJobFromMetadata(cubeName);
            boolean allSubmited = true;
            for (String segmentName : segmentList) {
                boolean ok = submitSegmentBuildJob(cubeName, segmentName);
                allSubmited = allSubmited && ok;
                if (!ok) {
                    logger.debug("Failed to submit building job.");
                }
            }
            if (allSubmited) {
                iterator.remove();
                logger.debug("Removed {} from check list.", cubeName);
            }
        }
    }

    // ==========================================================================================
    // ==========================================================================================

    /**
     * @return list of segment which could be submitted a segment build job
     */
    @NonSideEffect
    List<String> checkSegmentBuidJobFromMetadata(String cubeName) {
        List<String> result = Lists.newArrayList();
        CubeInstance cubeInstance = coordinator.getCubeManager().getCube(cubeName);
        CubeSegment latestHistoryReadySegment = cubeInstance.getLatestReadySegment();

        long minSegmentStart = -1;
        if (latestHistoryReadySegment != null) {
            minSegmentStart = latestHistoryReadySegment.getTSRange().end.v;
        }
        int allowMaxBuildingSegments = cubeInstance.getConfig().getMaxBuildingSegments();

        CubeAssignment assignments = coordinator.getStreamMetadataStore().getAssignmentsByCube(cubeName);
        Set<Integer> cubeAssignedReplicaSets = assignments.getReplicaSetIDs();

        List<SegmentBuildState> segmentStates = coordinator.getStreamMetadataStore().getSegmentBuildStates(cubeName);
        int inBuildingSegments = cubeInstance.getBuildingSegments().size();
        int leftQuota = allowMaxBuildingSegments - inBuildingSegments;

        // Sort it so we can iterate segments from eariler one to newer one
        Collections.sort(segmentStates);

        for (int i = 0; i < segmentStates.size(); i++) {

            if (leftQuota <= 0) {
                logger.info("No left quota to build segments for cube:{}", cubeName);
                break;
            }

            SegmentBuildState segmentState = segmentStates.get(i);
            Pair<Long, Long> segmentRange = CubeSegment.parseSegmentName(segmentState.getSegmentName());

            // If we have a exist historcial segment, we should not let new realtime segment overwrite it, it is so dangrous,
            // we just delete the entry to ignore the segment which should not exist
            if (segmentRange.getFirst() < minSegmentStart) {
                logger.warn(
                        "The cube segment state is not correct because it belongs to historcial part, cube:{} segment:{}, clear it.",
                        cubeName, segmentState.getSegmentName());
                coordinator.getStreamMetadataStore().removeSegmentBuildState(cubeName, segmentState.getSegmentName());
                continue;
            }

            // We already have a building job for current segment
            if (segmentState.isInBuilding()) {
                boolean needRebuild = checkSegmentBuildingJob(segmentState, cubeName, cubeInstance);
                if (!needRebuild)
                    continue;
            } else if (segmentState.isInWaiting()) {
                // The data has not been uploaded to remote completely, or job is discard
                // These two case should be submit a building job, just let go through it
            }

            boolean readyToBuild = checkSegmentIsReadyToBuild(segmentStates, i, cubeAssignedReplicaSets);
            if (!readyToBuild) {
                logger.debug("Segment {} {} is not ready to submit a building job.", cubeName, segmentState);
                // use break instead continue here, because we should transfer to next queue in sequential way (no jump the queue)
                break;
            } else {
                result.add(segmentState.getSegmentName());
                leftQuota--;
            }
        }
        if (logger.isDebugEnabled() && result.isEmpty()) {
            logger.debug("Candidate {} : {}.", cubeName, String.join(", ", result));
        }
        return result;
    }

    /**
     * Submit a build job for streaming segment
     *
     * @return true if submit succeed ; else false
     */
    @NotAtomicIdempotent
    boolean submitSegmentBuildJob(String cubeName, String segmentName) {
        logger.info("Try submit streaming segment build job, cube:{} segment:{}", cubeName, segmentName);
        CubeInstance cubeInstance = coordinator.getCubeManager().getCube(cubeName);
        try {
            // Step 1. create a new segment if not exists
            CubeSegment newSeg = null;
            Pair<Long, Long> segmentRange = CubeSegment.parseSegmentName(segmentName);
            boolean segmentExists = false;
            for (CubeSegment segment : cubeInstance.getSegments()) {
                SegmentRange.TSRange tsRange = segment.getTSRange();
                if (tsRange.start.v.equals(segmentRange.getFirst()) && segmentRange.getSecond().equals(tsRange.end.v)) {
                    segmentExists = true;
                    newSeg = segment;
                }
            }

            if (!segmentExists) {
                logger.debug("Create segment for {} {} .", cubeName, segmentName);
                newSeg = coordinator.getCubeManager().appendSegment(cubeInstance,
                        new SegmentRange.TSRange(segmentRange.getFirst(), segmentRange.getSecond()));
            } else {
                logger.info("Segment {} exists.", segmentName);
            }

            // Step 2. create and submit new build job
            DefaultChainedExecutable executable = getStreamingCubingJob(newSeg);
            coordinator.getExecutableManager().addJob(executable);
            String jobId = executable.getId();
            newSeg.setLastBuildJobID(jobId);

            // Step 3. add it to job trigger list
            SegmentJobBuildInfo segmentJobBuildInfo = new SegmentJobBuildInfo(cubeName, segmentName, jobId);
            addToJobTrackList(segmentJobBuildInfo);

            // Step 4. add job to stream metadata in case of current node dead
            SegmentBuildState.BuildState state = new SegmentBuildState.BuildState();
            state.setBuildStartTime(System.currentTimeMillis());
            state.setState(SegmentBuildState.BuildState.State.BUILDING);
            state.setJobId(jobId);
            logger.debug("Commit building job {} for {} {} .", jobId, cubeName, segmentName);
            coordinator.getStreamMetadataStore().updateSegmentBuildState(cubeName, segmentName, state);
            return true;
        } catch (Exception e) {
            logger.error("Streaming job submit fail, cubeName:" + cubeName + " segment:" + segmentName, e);
            return false;
        }
    }

    /**
     * Check segment which in building state
     *
     * @return true if we need to resubmit a new build job, else false
     */
    boolean checkSegmentBuildingJob(SegmentBuildState segmentState, String cubeName, CubeInstance cubeInstance) {
        String jobId = segmentState.getState().getJobId();
        logger.debug("There is segment in building, cube:{} segment:{} jobId:{}", cubeName,
                segmentState.getSegmentName(), jobId);
        long buildStartTime = segmentState.getState().getBuildStartTime();
        if (buildStartTime != 0 && jobId != null) {
            long buildDuration = System.currentTimeMillis() - buildStartTime;

            // Check build state after 15 minutes
            if (buildDuration < 15 * 60 * 1000) {
                return false;
            }
            CubingJob cubingJob = (CubingJob) coordinator.getExecutableManager().getJob(jobId);
            Preconditions.checkNotNull(cubingJob, "CubingJob should not be null.");
            ExecutableState jobState = cubingJob.getStatus();

            // If job is already succeed and HBase segment in ready state, remove the build state
            if (ExecutableState.SUCCEED.equals(jobState)) {
                CubeSegment cubeSegment = cubeInstance.getSegment(segmentState.getSegmentName(), null);
                if (cubeSegment != null && SegmentStatusEnum.READY == cubeSegment.getStatus()) {
                    logger.info("Job:{} is already succeed, and segment:{} is ready, remove segment build state", jobId,
                            segmentState.getSegmentName());
                    coordinator.getStreamMetadataStore().removeSegmentBuildState(cubeName,
                            segmentState.getSegmentName());
                }
                return false;
            }

            // If a job is in error state, just retry it
            if (ExecutableState.ERROR.equals(jobState)) {
                logger.info("Job:{} is error, resume the job.", jobId);
                coordinator.getExecutableManager().resumeJob(jobId);
                return false;
            }

            // If a job is discard, we will try to resumbit it later.
            if (ExecutableState.DISCARDED.equals(jobState)) {
                logger.info("Job:{} is discard, resubmit it later.", jobId);
                return true;
            } else {
                logger.info("Job:{} is in running, job state: {}.", jobId, jobState);
            }
        } else {
            logger.info("Unknown state {}", segmentState);
        }
        return false;
    }

    /**
     * <pre>
     *     When all replica sets have uploaded their local segment cache to deep storage, (that is to say all required data is uploaded), we can mark
     *     this segment as ready to submit a MapReduce job to build into HBase.
     *
     *     Note the special situation, when some replica set didn't upload any data in some segment duration for lack
     *     of entered kafka event, we still try to check the newer segment duration, if found some newer segment have data
     *     uploaded for current miss replica set, we marked local segment cache has been uploaded for that replica for current segment.
     *     This workround will prevent job-submit queue from blocking by no data for some topic partition.
     * </pre>
     *
     * @return true if current segment is ready to submit a build job, else false
     */
    boolean checkSegmentIsReadyToBuild(List<SegmentBuildState> allSegmentStates, int checkedSegmentIdx,
            Set<Integer> cubeAssignedReplicaSets) {
        SegmentBuildState checkedSegmentState = allSegmentStates.get(checkedSegmentIdx);
        Set<Integer> notCompleteReplicaSets = Sets
                .newHashSet(Sets.difference(cubeAssignedReplicaSets, checkedSegmentState.getCompleteReplicaSets()));
        if (notCompleteReplicaSets.isEmpty()) {
            return true;
        } else {
            for (int i = checkedSegmentIdx + 1; i < allSegmentStates.size(); i++) {
                SegmentBuildState segmentBuildState = allSegmentStates.get(i);
                Set<Integer> completeReplicaSetsForNext = segmentBuildState.getCompleteReplicaSets();
                Iterator<Integer> notCompleteRSItr = notCompleteReplicaSets.iterator();
                while (notCompleteRSItr.hasNext()) {
                    Integer rsID = notCompleteRSItr.next();
                    if (completeReplicaSetsForNext.contains(rsID)) {
                        logger.info(
                                "the replica set:{} doesn't have data for segment:{}, but have data for later segment:{}",
                                rsID, checkedSegmentState.getSegmentName(), segmentBuildState.getSegmentName());
                        notCompleteRSItr.remove();
                    }
                }
            }
            return notCompleteReplicaSets.isEmpty();
        }
    }

    public Set<String> getCubeCheckList() {
        return cubeCheckList;
    }

    public DefaultChainedExecutable getStreamingCubingJob(CubeSegment segment){
        return new StreamingCubingEngine().createStreamingCubingJob(segment, "SYSTEM");
    }
}
