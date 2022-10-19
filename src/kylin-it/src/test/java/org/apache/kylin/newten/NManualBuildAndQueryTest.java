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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.job.NSparkMergingJob;
import org.apache.kylin.engine.spark.merger.AfterMergeOrRefreshResourceMerger;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.guava.collect.Sets;

import com.google.common.collect.Maps;

import lombok.val;

@Ignore("see io.kyligence.kap.ut.TestQueryAndBuild")
@SuppressWarnings("serial")
public class NManualBuildAndQueryTest extends NLocalWithSparkSessionTest {

    private static final Logger logger = LoggerFactory.getLogger(NManualBuildAndQueryTest.class);

    @Before
    public void setup() throws Exception {
        super.init();
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
    }

    public void buildCubes() throws Exception {
        if (Boolean.parseBoolean(System.getProperty("noBuild", "false"))) {
            System.out.println("Direct query");
        } else if (Boolean.parseBoolean(System.getProperty("isDeveloperMode", "false"))) {
            fullBuild("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            fullBuild("741ca86a-1f13-46da-a59f-95fb68615e3a");
        } else {
            buildAndMergeCube("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            buildAndMergeCube("741ca86a-1f13-46da-a59f-95fb68615e3a");
        }
    }

    private void buildAndMergeCube(String dfName) throws Exception {
        if (dfName.equals("89af4ee2-2cdb-4b07-b39e-4c29856309aa")) {
            buildFourSegementAndMerge(dfName);
        }
        if (dfName.equals("741ca86a-1f13-46da-a59f-95fb68615e3a")) {
            buildTwoSegementAndMerge(dfName);
        }
    }

    private void buildTwoSegementAndMerge(String dfName) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());

        NDataflow df = dsMgr.getDataflow(dfName);
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        /**
         * Round1. Build 4 segment
         */
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2013-01-01");
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);
        start = SegmentRange.dateToLong("2013-01-01");
        end = SegmentRange.dateToLong("2015-01-01");
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);

        /**
         * Round2. Merge two segments
         */
        df = dsMgr.getDataflow(dfName);
        NDataSegment firstMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2010-01-01"), SegmentRange.dateToLong("2015-01-01")), false);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN",
                RandomUtil.randomUUIDStr());
        execMgr.addJob(firstMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, IndexDataConstructor.wait(firstMergeJob));
        val merger = new AfterMergeOrRefreshResourceMerger(config, getProject());
        merger.merge(firstMergeJob.getSparkMergingStep());

        /**
         * validate cube segment info
         */
        NDataSegment firstSegment = dsMgr.getDataflow(dfName).getSegments().get(0);

        if (getProject().equals("default") && dfName.equals("741ca86a-1f13-46da-a59f-95fb68615e3a")) {
            Map<Long, NDataLayout> cuboidsMap1 = firstSegment.getLayoutsMap();
            Map<Long, Long[]> compareTuples1 = Maps.newHashMap();
            compareTuples1.put(1L, new Long[] { 9896L, 9896L });
            compareTuples1.put(10001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(10002L, new Long[] { 9896L, 9896L });
            compareTuples1.put(20001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(30001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(1000001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(1010001L, new Long[] { 731L, 9163L });
            compareTuples1.put(1020001L, new Long[] { 302L, 9163L });
            compareTuples1.put(1030001L, new Long[] { 44L, 210L });
            compareTuples1.put(1040001L, new Long[] { 9163L, 9896L });
            compareTuples1.put(1050001L, new Long[] { 9421L, 9896L });
            compareTuples1.put(1060001L, new Long[] { 105L, 276L });
            compareTuples1.put(1070001L, new Long[] { 143L, 9421L });
            compareTuples1.put(1080001L, new Long[] { 138L, 286L });
            compareTuples1.put(1090001L, new Long[] { 9880L, 9896L });
            compareTuples1.put(1100001L, new Long[] { 9833L, 9896L });
            compareTuples1.put(20000000001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(20000010001L, new Long[] { 9896L, 9896L });
            compareTuples1.put(20000020001L, new Long[] { 9896L, 9896L });
            verifyCuboidMetrics(cuboidsMap1, compareTuples1);
        }

        Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2010-01-01"),
                SegmentRange.dateToLong("2015-01-01")), firstSegment.getSegRange());
        //Assert.assertEquals(27, firstSegment.getDictionaries().size());

        getLookTables(df).forEach(table -> Assert.assertNotNull(table.getLastSnapshotPath()));
    }

    private Set<TableDesc> getLookTables(NDataflow df) {
        return df.getModel().getLookupTables().stream().map(tableRef -> tableRef.getTableDesc())
                .collect(Collectors.toSet());
    }

    private void buildFourSegementAndMerge(String dfName) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());

        NDataflow df = dsMgr.getDataflow(dfName);
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        /**
         * Round1. Build 4 segment
         */
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2012-06-01");
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);

        start = SegmentRange.dateToLong("2012-06-01");
        end = SegmentRange.dateToLong("2013-01-01");
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);

        start = SegmentRange.dateToLong("2013-01-01");
        end = SegmentRange.dateToLong("2013-06-01");
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);

        start = SegmentRange.dateToLong("2013-06-01");
        end = SegmentRange.dateToLong("2015-01-01");
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);

        /**
         * Round2. Merge two segments
         */
        df = dsMgr.getDataflow(dfName);
        NDataSegment firstMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2010-01-01"), SegmentRange.dateToLong("2013-01-01")), false);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN",
                RandomUtil.randomUUIDStr());
        execMgr.addJob(firstMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, IndexDataConstructor.wait(firstMergeJob));
        val merger = new AfterMergeOrRefreshResourceMerger(config, getProject());
        merger.merge(firstMergeJob.getSparkMergingStep());

        df = dsMgr.getDataflow(dfName);

        NDataSegment secondMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2013-01-01"), SegmentRange.dateToLong("2015-06-01")), false);
        NSparkMergingJob secondMergeJob = NSparkMergingJob.merge(secondMergeSeg, Sets.newLinkedHashSet(layouts),
                "ADMIN", RandomUtil.randomUUIDStr());
        execMgr.addJob(secondMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, IndexDataConstructor.wait(secondMergeJob));
        merger.merge(secondMergeJob.getSparkMergingStep());

        /**
         * validate cube segment info
         */
        NDataSegment firstSegment = dsMgr.getDataflow(dfName).getSegments().get(0);
        NDataSegment secondSegment = dsMgr.getDataflow(dfName).getSegments().get(1);

        if (getProject().equals("default") && dfName.equals("89af4ee2-2cdb-4b07-b39e-4c29856309aa")) {
            Map<Long, NDataLayout> cuboidsMap1 = firstSegment.getLayoutsMap();
            Map<Long, Long[]> compareTuples1 = Maps.newHashMap();
            compareTuples1.put(1L, new Long[] { 4903L, 4903L });
            compareTuples1.put(10001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(10002L, new Long[] { 4903L, 4903L });
            compareTuples1.put(20001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(30001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(1000001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(20000000001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(20000010001L, new Long[] { 4903L, 4903L });
            compareTuples1.put(20000020001L, new Long[] { 4903L, 4903L });
            verifyCuboidMetrics(cuboidsMap1, compareTuples1);

            Map<Long, NDataLayout> cuboidsMap2 = secondSegment.getLayoutsMap();
            Map<Long, Long[]> compareTuples2 = Maps.newHashMap();
            compareTuples2.put(1L, new Long[] { 5097L, 5097L });
            compareTuples2.put(10001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(10002L, new Long[] { 5097L, 5097L });
            compareTuples2.put(20001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(30001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(1000001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(20000000001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(20000010001L, new Long[] { 5097L, 5097L });
            compareTuples2.put(20000020001L, new Long[] { 5097L, 5097L });
            verifyCuboidMetrics(cuboidsMap2, compareTuples2);
        }

        Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2010-01-01"),
                SegmentRange.dateToLong("2013-01-01")), firstSegment.getSegRange());
        Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2013-01-01"),
                SegmentRange.dateToLong("2015-01-01")), secondSegment.getSegRange());
        //Assert.assertEquals(31, firstSegment.getDictionaries().size());
        //Assert.assertEquals(31, secondSegment.getDictionaries().size());
        getLookTables(df).stream().forEach(table -> Assert.assertNotNull(table.getLastSnapshotPath()));

    }

    private void verifyCuboidMetrics(Map<Long, NDataLayout> cuboidsMap, Map<Long, Long[]> compareTuples) {
        compareTuples.forEach((key, value) -> {
            Assert.assertEquals(value[0], (Long) cuboidsMap.get(key).getRows());
            Assert.assertEquals(value[1], (Long) cuboidsMap.get(key).getSourceRows());
        });
    }
}
