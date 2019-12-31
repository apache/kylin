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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.stream.coordinator.exception.StoreException;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BuildJobSubmitterTest extends StreamingTestBase {

    // shared stub object
    CubeManager cubeManager;
    StreamingCoordinator streamingCoordinator;
    ReceiverClusterManager clusterManager;
    ExecutableManager executableManager;
    KylinConfig config = stubKylinConfig();

    void beforeTestTraceEarliestSegmentBuildJob() {
        // prepare dependency
        CubeSegment cubeSegment = stubCubSegment(SegmentStatusEnum.NEW, 100L, 200L);
        CubeInstance cubeInstance = stubCubeInstance(cubeSegment);

        cubeManager = stubCubeManager(cubeInstance, false);
        config = stubKylinConfig();

        Map<String, CubingJob> cubingJobMap = new HashMap<>();
        cubingJobMap.put(mockBuildJob1, stubCubingJob(ExecutableState.SUCCEED));
        cubingJobMap.put(mockBuildJob2, stubCubingJob(ExecutableState.DISCARDED));
        cubingJobMap.put(mockBuildJob3, stubCubingJob(ExecutableState.ERROR));
        executableManager = stubExecutableManager(cubingJobMap);
        streamingCoordinator = stubStreamingCoordinator(config, cubeManager, executableManager);
        clusterManager = stubReceiverClusterManager(streamingCoordinator);
        when(streamingCoordinator.getClusterManager()).thenReturn(clusterManager);
    }

    @Test
    public void testTraceEarliestSegmentBuildJob() {
        beforeTestTraceEarliestSegmentBuildJob();
        BuildJobSubmitter buildJobSubmitter = new BuildJobSubmitter(streamingCoordinator);
        buildJobSubmitter.restore();
        List<SegmentJobBuildInfo> jobList = buildJobSubmitter.traceEarliestSegmentBuildJob();
        assertEquals(1, jobList.size());
        assertThat(jobList.stream().map(x -> x.jobID).collect(Collectors.toSet()), hasItem(mockBuildJob1));
        assertEquals(1, buildJobSubmitter.getCubeCheckList().size());
    }

    @Test(expected = StoreException.class)
    @SuppressWarnings("unchecked")
    public void testTraceEarliestSegmentBuildJob2() {
        beforeTestTraceEarliestSegmentBuildJob();
        when(clusterManager.segmentBuildComplete(isA(CubingJob.class), isA(CubeInstance.class), isA(CubeSegment.class),
                isA(SegmentJobBuildInfo.class))).thenThrow(StoreException.class);
        BuildJobSubmitter buildJobSubmitter = new BuildJobSubmitter(streamingCoordinator);
        buildJobSubmitter.restore();
        List<SegmentJobBuildInfo> jobList = buildJobSubmitter.traceEarliestSegmentBuildJob();
        assertEquals(0, jobList.size());
        assertEquals(0, buildJobSubmitter.getCubeCheckList().size());
    }

    void prepareTestCheckSegmentBuildJobFromMetadata() {
        CubeSegment cubeSegment = stubCubSegment(SegmentStatusEnum.NEW, 100L, 200L);
        CubeInstance cubeInstance = stubCubeInstance(cubeSegment);
        config = stubKylinConfig();
        when(cubeInstance.getConfig()).thenReturn(config);

        cubeManager = stubCubeManager(cubeInstance, false);

        Map<String, CubingJob> cubingJobMap = new HashMap<>();
        cubingJobMap.put(mockBuildJob1, stubCubingJob(ExecutableState.SUCCEED));
        cubingJobMap.put(mockBuildJob2, stubCubingJob(ExecutableState.DISCARDED));
        cubingJobMap.put(mockBuildJob3, stubCubingJob(ExecutableState.DISCARDED));
        cubingJobMap.put(mockBuildJob4, stubCubingJob(ExecutableState.ERROR));

        executableManager = stubExecutableManager(cubingJobMap);
        streamingCoordinator = stubStreamingCoordinator(config, cubeManager, executableManager);
        clusterManager = stubReceiverClusterManager(streamingCoordinator);
        when(streamingCoordinator.getClusterManager()).thenReturn(clusterManager);
    }

    @Test
    public void testCheckSegmentBuildJobFromMetadata() {
        prepareTestCheckSegmentBuildJobFromMetadata();
        BuildJobSubmitter buildJobSubmitter = new BuildJobSubmitter(streamingCoordinator);
        buildJobSubmitter.restore();
        List<String> segmentReadyList = buildJobSubmitter.checkSegmentBuildJobFromMetadata(cubeName2);
        assertEquals(1, segmentReadyList.size());

        segmentReadyList = buildJobSubmitter.checkSegmentBuildJobFromMetadata(cubeName3);
        assertEquals(1, segmentReadyList.size());
    }

    @Test
    public void testCheckSegmentBuildJobFromMetadata1() {
        prepareTestCheckSegmentBuildJobFromMetadata();
        BuildJobSubmitter buildJobSubmitter = new BuildJobSubmitter(streamingCoordinator);
        buildJobSubmitter.restore();

        List<String> segmentReadyList = buildJobSubmitter.checkSegmentBuildJobFromMetadata(cubeName4);
        verify(executableManager, times(1)).resumeJob(eq(mockBuildJob4));
        assertEquals(0, segmentReadyList.size());
    }

    @Test
    public void testSubmitSegmentBuildJob() throws IOException {
        CubeSegment cubeSegment1 = stubCubSegment(SegmentStatusEnum.NEW, 100L, 200L);
        CubeSegment cubeSegment2 = stubCubSegment(SegmentStatusEnum.NEW, 1559390400000L, 1559394000000L);

        CubeInstance cubeInstance = stubCubeInstance(cubeSegment1);
        @SuppressWarnings("unchecked")
        Iterator<CubeSegment> cubeSegmentIterable = mock(Iterator.class);
        when(cubeSegmentIterable.hasNext()).thenReturn(true, false);
        @SuppressWarnings("unchecked")
        Segments<CubeSegment> segmentSegments = mock(Segments.class, RETURNS_DEEP_STUBS);
        when(cubeSegmentIterable.next()).thenReturn(cubeSegment1, cubeSegment2);
        when(segmentSegments.iterator()).thenReturn(cubeSegmentIterable);
        when(cubeInstance.getSegments()).thenReturn(segmentSegments);

        config = stubKylinConfig();
        when(cubeInstance.getConfig()).thenReturn(config);

        cubeManager = stubCubeManager(cubeInstance, false);

        Map<String, CubingJob> cubingJobMap = new HashMap<>();
        cubingJobMap.put(mockBuildJob1, stubCubingJob(ExecutableState.SUCCEED));
        cubingJobMap.put(mockBuildJob2, stubCubingJob(ExecutableState.DISCARDED));
        cubingJobMap.put(mockBuildJob3, stubCubingJob(ExecutableState.DISCARDED));
        cubingJobMap.put(mockBuildJob4, stubCubingJob(ExecutableState.ERROR));

        executableManager = stubExecutableManager(cubingJobMap);
        streamingCoordinator = stubStreamingCoordinator(config, cubeManager, executableManager);
        clusterManager = stubReceiverClusterManager(streamingCoordinator);
        when(streamingCoordinator.getClusterManager()).thenReturn(clusterManager);

        when(cubeManager.appendSegment(any(CubeInstance.class), any(SegmentRange.TSRange.class)))
                .thenReturn(cubeSegment1, cubeSegment2);

        BuildJobSubmitter buildJobSubmitter = new BuildJobSubmitter(streamingCoordinator);
        buildJobSubmitter = spy(buildJobSubmitter);

        DefaultChainedExecutable cubingJob = mock(DefaultChainedExecutable.class);
        when(cubingJob.getId()).thenReturn(mockBuildJob4);
        doReturn(cubingJob).when(buildJobSubmitter).getStreamingCubingJob(any(CubeSegment.class));

        buildJobSubmitter.restore();
        boolean submitSuccess = buildJobSubmitter.submitSegmentBuildJob(cubeName1, segment1);
        assertTrue(submitSuccess);
    }
}