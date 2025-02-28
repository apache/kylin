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

package org.apache.kylin.newten;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import lombok.val;

public class NMultiPartitionJobTest extends NLocalWithSparkSessionTest {
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        overwriteSystemProp("kylin.model.multi-partition-enabled", "true");
        setOverlay("src/test/resources/ut_meta/multi_partition");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/multi_partition" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        super.tearDown();
    }

    @Override
    public String getProject() {
        return "multi_partition";
    }

    @Test
    public void testConstantComputeColumn() throws Exception {
        String dfID = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dfManager.getDataflow(dfID);
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).updateDataflow(update);
            return null;
        }, getProject());

        val layouts = df.getIndexPlan().getAllLayouts();
        long startTime = SegmentRange.dateToLong("2020-11-05");
        long endTime = SegmentRange.dateToLong("2020-11-06");
        val segmentRange = new SegmentRange.TimePartitionedSegmentRange(startTime, endTime);

        val buildPartitions = Lists.<String[]> newArrayList();
        buildPartitions.add(new String[] { "usa" });
        buildPartitions.add(new String[] { "cn" });
        buildPartitions.add(new String[] { "africa" });

        indexDataConstructor.buildIndex(dfID, segmentRange, Sets.newLinkedHashSet(layouts), true, buildPartitions);
        String sqlHitCube = " select count(1) from TEST_BANK_INCOME t1 inner join TEST_BANK_LOCATION t2 on t1. COUNTRY = t2. COUNTRY "
                + " where  t1.dt = '2020-11-05' ";
        List<String> hitCubeResult = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube).collectAsList()
                .stream().map(Row::toString).collect(Collectors.toList());
        Assert.assertEquals(1, hitCubeResult.size());

        // will auto offline
        overwriteSystemProp("kylin.model.multi-partition-enabled", "false");
        long startTime2 = SegmentRange.dateToLong("2020-11-06");
        long endTime2 = SegmentRange.dateToLong("2020-11-07");
        val segmentRange2 = new SegmentRange.TimePartitionedSegmentRange(startTime2, endTime2);
        indexDataConstructor.buildIndex(dfID, segmentRange2, Sets.newLinkedHashSet(layouts), true, buildPartitions);
        NDataflow df2 = dfManager.getDataflow(dfID);
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, df2.getStatus());
    }

    @Test
    public void testGlobalDict() throws Exception {
        String dfID = "0080e4e4-69af-449e-b09f-05c90dfa04b6";
        NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dfManager.getDataflow(dfID);
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).updateDataflow(update);
            return null;
        }, getProject());

        val layouts = df.getIndexPlan().getAllLayouts();
        long startTime = SegmentRange.dateToLong("2020-11-01");
        long endTime = SegmentRange.dateToLong("2020-11-06");
        val segmentRange = new SegmentRange.TimePartitionedSegmentRange(startTime, endTime);

        val segmentId = indexDataConstructor.buildIndex(dfID, segmentRange, Sets.newLinkedHashSet(layouts), true,
                Lists.<String[]> newArrayList(new String[] { "un" }));
        String sqlHitCube = " select count(distinct INCOME) from TEST_BANK_INCOME t1 "
                + "where t1.dt < '2020-11-06' and t1.dt >= '2020-11-01'";
        String whereSql = " and COUNTRY in ('un')";
        List<Row> result = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube + whereSql).collectAsList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).size());
        Assert.assertEquals(2, result.get(0).getLong(0));

        whereSql = " and COUNTRY in ('un', 'usa')";
        indexDataConstructor.buildMultiPartition(dfID, segmentId, Sets.newLinkedHashSet(layouts), true,
                Lists.<String[]> newArrayList(new String[] { "usa" }));
        result = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube + whereSql).collectAsList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).size());
        Assert.assertEquals(5, result.get(0).getLong(0));

        whereSql = " and COUNTRY in ('un', 'usa', 'cn')";
        indexDataConstructor.buildMultiPartition(dfID, segmentId, Sets.newLinkedHashSet(layouts), true,
                Lists.<String[]> newArrayList(new String[] { "cn" }));
        result = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube + whereSql).collectAsList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).size());
        Assert.assertEquals(9, result.get(0).getLong(0));

        whereSql = " and COUNTRY in ('un')";
        result = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube + whereSql).collectAsList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).size());
        Assert.assertEquals(2, result.get(0).getLong(0));

        whereSql = " and COUNTRY in ('usa')";
        result = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube + whereSql).collectAsList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).size());
        Assert.assertEquals(5, result.get(0).getLong(0));

        whereSql = " and COUNTRY in ('cn')";
        result = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube + whereSql).collectAsList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).size());
        Assert.assertEquals(6, result.get(0).getLong(0));

        // will auto offline
        overwriteSystemProp("kylin.model.multi-partition-enabled", "false");
        long startTime2 = SegmentRange.dateToLong("2020-11-06");
        long endTime2 = SegmentRange.dateToLong("2020-11-07");
        val segmentRange2 = new SegmentRange.TimePartitionedSegmentRange(startTime2, endTime2);
        indexDataConstructor.buildIndex(dfID, segmentRange2, Sets.newLinkedHashSet(layouts), true,
                Lists.<String[]> newArrayList(new String[] { "africa" }));
        NDataflow df2 = dfManager.getDataflow(dfID);
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, df2.getStatus());
    }

    @Test
    public void testSanityCheck() throws Exception {
        String dataflowId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dfManager.getDataflow(dataflowId);
        val update = new NDataflowUpdate(dataflowId);
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).updateDataflow(update);
            return null;
        }, getProject());

        val layoutList = df.getIndexPlan().getAllLayouts();
        long startTime = SegmentRange.dateToLong("2020-11-05");
        long endTime = SegmentRange.dateToLong("2020-11-06");
        val segmentRange = new SegmentRange.TimePartitionedSegmentRange(startTime, endTime);

        val buildPartitions = Lists.<String[]> newArrayList();
        buildPartitions.add(new String[] { "usa" });
        buildPartitions.add(new String[] { "cn" });
        buildPartitions.add(new String[] { "africa" });

        val partitioned = layoutList.stream().collect(Collectors.partitioningBy(l -> l.getIndex().getId() == 110000L));
        val batch1 = partitioned.get(Boolean.TRUE);
        val batch2 = partitioned.get(Boolean.FALSE);
        String segmentId = indexDataConstructor.buildIndex(dataflowId, segmentRange, Sets.newLinkedHashSet(batch1),
                true, buildPartitions);

        NDataSegment segment = dfManager.getDataflow(dataflowId).getSegment(segmentId);
        indexDataConstructor.buildSegment(dataflowId, segment, Sets.newLinkedHashSet(batch2), false, buildPartitions);

        NDataSegment segment1 = dfManager.getDataflow(dataflowId).getSegment(segmentId);
        Assert.assertEquals(layoutList.size(), segment1.getSegDetails().getLayouts().size());
    }
}
