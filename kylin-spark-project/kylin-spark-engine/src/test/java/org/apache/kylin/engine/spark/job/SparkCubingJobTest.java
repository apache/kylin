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

package org.apache.kylin.engine.spark.job;

import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.engine.spark.job.NSparkCubingStep;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.engine.spark.storage.ParquetStorage;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.metadata.cube.model.Cube;
import org.apache.kylin.engine.spark.metadata.cube.model.CuboidLayoutChooser;
import org.apache.kylin.engine.spark.metadata.cube.model.DataLayout;
import org.apache.kylin.engine.spark.metadata.cube.model.DataSegDetails;
import org.apache.kylin.engine.spark.metadata.cube.model.DataSegment;
import org.apache.kylin.engine.spark.metadata.cube.model.IndexEntity;
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity;
import org.apache.kylin.engine.spark.metadata.cube.model.SegmentRange;
import org.apache.kylin.engine.spark.metadata.cube.model.SpanningTree;
import org.apache.kylin.engine.spark.metadata.cube.model.SpanningTreeFactory;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.spark_project.guava.collect.Sets;

import java.util.ArrayList;
import java.util.List;

public class SparkCubingJobTest extends LocalWithSparkSessionTest {

    private KylinConfig config;

    @Before
    public void setup() {
        ss.sparkContext().setLogLevel("ERROR");
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        System.setProperty("kap.engine.persist-flattable-threshold", "0");

        /*NDefaultScheduler.destroyInstance();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(getTestConfig()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }*/

        config = getTestConfig();
    }

    @After
    public void after() {
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
        System.clearProperty("kap.engine.persist-flattable-threshold");
    }

    @Test
    public void testBuildJob() throws Exception {
        Cube cube = Cube.getInstance(config);
        ExecutableManager execMgr = ExecutableManager.getInstance(config);

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // ready cube, segment, cuboid layout
        DataSegment oneSeg = cube.appendSegment(SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<LayoutEntity> round1 = new ArrayList<>();
        round1.add(cube.getCuboidLayout(20_000_020_001L));
        round1.add(cube.getCuboidLayout(1_000_001L));
        round1.add(cube.getCuboidLayout(30001L));
        round1.add(cube.getCuboidLayout(10002L));

        SpanningTree nSpanningTree = SpanningTreeFactory.fromLayouts(round1, cube.getUuid());
        for (IndexEntity rootCuboid : nSpanningTree.getRootIndexEntities()) {
            LayoutEntity layout = CuboidLayoutChooser.selectLayoutForBuild(oneSeg, rootCuboid);
            Assert.assertNull(layout);
        }

        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round1), "ADMIN");
        NSparkCubingStep sparkStep = job.getSparkCubingStep();
        StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        execMgr.addJob(job);

        // wait job done
        ExecutableState status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        /**
         * Round2. Build new layouts, should reuse the data from already existing cuboid.
         * Notice: After round1 the segment has been updated, need to refresh the cache before use the old one.
         */
        List<LayoutEntity> round2 = new ArrayList<>();
        round2.add(cube.getCuboidLayout(1L));
        round2.add(cube.getCuboidLayout(20_000_000_001L));
        round2.add(cube.getCuboidLayout(20001L));
        round2.add(cube.getCuboidLayout(10001L));

        nSpanningTree = SpanningTreeFactory.fromLayouts(round2, cube.getUuid());
        for (IndexEntity rootCuboid : nSpanningTree.getRootIndexEntities()) {
            LayoutEntity layout = CuboidLayoutChooser.selectLayoutForBuild(oneSeg, rootCuboid);
            Assert.assertNotNull(layout);
        }

        job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round2), "ADMIN");
        execMgr.addJob(job);

        // wait job done
        status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        validateCube(cube.getSegments().get(0).getId());
        validateTableIndex(cube.getSegments().get(0).getId());
    }

    private void validateCube(String segmentId) {
        Cube cube = Cube.getInstance(config, "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DataSegment seg = cube.getSegment(segmentId);

        // check row count in NDataSegDetails
        Assert.assertEquals(10000, seg.getLayout(1).getRows());
        Assert.assertEquals(10000, seg.getLayout(10001).getRows());
        Assert.assertEquals(10000, seg.getLayout(10002).getRows());
    }

    private void validateTableIndex(String segmentId) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Cube cube = Cube.getInstance(config, "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DataSegment seg = cube.getSegment(segmentId);
        DataSegDetails segCuboids = seg.getSegDetails();
        DataLayout dataCuboid = DataLayout.newDataLayout(segCuboids, 20000000001L);
        LayoutEntity layout = dataCuboid.getLayout();
        Assert.assertEquals(10000, seg.getLayout(20000000001L).getRows());

        ParquetStorage storage = new ParquetStorage();
        Dataset<Row> ret = storage.getFrom(NSparkCubingUtil.getStoragePath(dataCuboid), ss);
        List<Row> rows = ret.collectAsList();
        Assert.assertEquals("Ebay", rows.get(0).apply(1).toString());
        Assert.assertEquals("Ebaymotors", rows.get(1).apply(1).toString());
        Assert.assertEquals("Ebay", rows.get(9998).apply(1).toString());
        Assert.assertEquals("英国", rows.get(9999).apply(1).toString());
    }
}
