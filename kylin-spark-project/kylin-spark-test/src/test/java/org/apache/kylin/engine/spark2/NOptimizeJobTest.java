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

package org.apache.kylin.engine.spark2;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.job.NSparkBatchOptimizeJobCheckpointBuilder;
import org.apache.kylin.engine.spark.job.NSparkOptimizingJob;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.CheckpointExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class NOptimizeJobTest extends LocalWithSparkSessionTest {
    protected KylinConfig config;
    protected CubeManager cubeMgr;
    protected ExecutableManager execMgr;

    private final String CUBE_NAME = "ci_left_join_cube";
    private final long CUBOID_ADD = 1048575L;
    private final long CUBOID_DELETE = 14336L;

    @Override
    public void setup() throws SchedulerException {
        super.setup();
        overwriteSystemProp("kylin.env", "UT");
        overwriteSystemProp("isDeveloperMode", "true");
        overwriteSystemProp("kylin.engine.segment-statistics-enabled", "true");
        Map<RealizationType, Integer> priorities = Maps.newHashMap();
        priorities.put(RealizationType.HYBRID, 0);
        priorities.put(RealizationType.CUBE, 0);
        Candidate.setPriorities(priorities);
        config = KylinConfig.getInstanceFromEnv();
        cubeMgr = CubeManager.getInstance(config);
        execMgr = ExecutableManager.getInstance(config);
    }

    @Override
    public void after() {
        super.after();
    }

    @Test
    public void verifyOptimizeJob() throws Exception {
        CubeInstance cube = cubeMgr.reloadCube(CUBE_NAME);
        Set<Long> recommendCuboids = new HashSet<>();
        recommendCuboids.addAll(cube.getCuboidScheduler().getAllCuboidIds());
        recommendCuboids.add(CUBOID_ADD);
        recommendCuboids.remove(CUBOID_DELETE);
        // 1. Build two segments
        buildSegments(CUBE_NAME, new SegmentRange.TSRange(dateToLong("2012-01-01"), dateToLong("2012-02-01")),
                new SegmentRange.TSRange(dateToLong("2012-02-01"), dateToLong("2012-03-01")));

        // 2. Optimize Segment
        CubeSegment[] optimizeSegments = cubeMgr.optimizeSegments(cube, recommendCuboids);
        for (CubeSegment segment : optimizeSegments) {
            ExecutableState result = optimizeSegment(segment);
            Assert.assertEquals(ExecutableState.SUCCEED, result);
        }

        cube = cubeMgr.reloadCube(CUBE_NAME);

        Assert.assertEquals(4, cube.getSegments().size());
        Assert.assertEquals(2, cube.getSegments(SegmentStatusEnum.READY_PENDING).size());
        Assert.assertEquals(2, cube.getSegments(SegmentStatusEnum.READY).size());

        // 3. CheckPoint Job
        executeCheckPoint(cube);

        cube = cubeMgr.reloadCube(CUBE_NAME);

        // 4. Check cube status and cuboid list
        Assert.assertEquals(2, cube.getSegments().size());
        Assert.assertEquals(2, cube.getSegments(SegmentStatusEnum.READY).size());
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        for (CubeSegment segment : cube.getSegments()) {
            Assert.assertEquals(SegmentStatusEnum.READY, segment.getStatus());
            CubeStatsReader segStatsReader = new CubeStatsReader(segment, config);
            Assert.assertEquals(recommendCuboids, segStatsReader.getCuboidRowHLLCounters().keySet());
            String cuboidPath = PathManager.getSegmentParquetStoragePath(cube, segment.getName(), segment.getStorageLocationIdentifier());
            Assert.assertTrue(fs.exists(new Path(cuboidPath)));
            Assert.assertTrue(fs.exists(new Path(cuboidPath + "/" + CUBOID_ADD)));
            Assert.assertFalse(fs.exists(new Path(cuboidPath + "/" + CUBOID_DELETE)));

        }
        Assert.assertEquals(recommendCuboids, cube.getCuboidScheduler().getAllCuboidIds());
    }

    public void buildSegments(String cubeName, SegmentRange.TSRange... toBuildRanges) throws Exception {
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        cleanupSegments(cubeName);

        ExecutableState state;
        for (SegmentRange.TSRange toBuildRange : toBuildRanges) {
            state = buildCuboid(cubeName, toBuildRange);
            Assert.assertEquals(ExecutableState.SUCCEED, state);
        }
    }

    protected ExecutableState optimizeSegment(CubeSegment segment) throws Exception {
        NSparkOptimizingJob optimizeJob = NSparkOptimizingJob.optimize(segment, "ADMIN");
        execMgr.addJob(optimizeJob);
        ExecutableState result = wait(optimizeJob);
        checkJobTmpPathDeleted(config, optimizeJob);
        return result;
    }

    protected ExecutableState executeCheckPoint(CubeInstance cubeInstance) throws Exception {
        CheckpointExecutable checkPointJob = new NSparkBatchOptimizeJobCheckpointBuilder(cubeInstance, "ADMIN").build();
        execMgr.addJob(checkPointJob);
        ExecutableState result = wait(checkPointJob);
        return result;
    }
}
