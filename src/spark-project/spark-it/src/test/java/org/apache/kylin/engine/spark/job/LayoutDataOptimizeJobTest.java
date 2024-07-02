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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.common.ExecutableUtil;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.handler.LayoutDataOptimizeJobHandler.LayoutDataOptimizeJobParam;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.cuboid.NCuboidLayoutChooser;
import org.apache.kylin.metadata.cube.cuboid.NSpanningTree;
import org.apache.kylin.metadata.cube.cuboid.NSpanningTreeFactory;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayoutDetailsManager;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import io.delta.tables.DeltaTable;

public class LayoutDataOptimizeJobTest extends NLocalWithSparkSessionTest {

    private KylinConfig config;

    @Override
    public String getProject() {
        return "storage_v3_test";
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ss.sparkContext().setLogLevel("ERROR");
        overwriteSystemProp("kylin.engine.persist-flattable-threshold", "0");
        overwriteSystemProp("kylin.engine.persist-flatview", "true");

        config = getTestConfig();
        JobContextUtil.getJobContext(config);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        super.tearDown();
    }

    @Test
    public void testV3LayoutDataOptimizeJob() throws InterruptedException {
        NDataSegment segment = buildV3Data();
        NDataflow df = segment.getDataflow();
        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());
        Set<Long> optimizeLayoutIds = Sets.newHashSet();
        // repartition
        LayoutEntity repartitionLayout = df.getIndexPlan().getAllLayouts().get(0);
        optimizeLayoutIds.add(repartitionLayout.getId());
        NDataLayoutDetailsManager manager = NDataLayoutDetailsManager.getInstance(config, getProject());
        long repartitionLayoutId = repartitionLayout.getId();
        String repartitionColName = repartitionLayout.getOrderedDimensions().get(repartitionLayout.getDimsIds().get(0))
                .getIdentity();
        String repartitionLayoutTablePath = df.getSegmentHdfsPath(segment.getId()) + "/" + repartitionLayoutId;
        DeltaTable repartitionLayoutTable = DeltaTable.forPath(repartitionLayoutTablePath);
        List<Object> partitionColumns = repartitionLayoutTable.detail().select("partitionColumns").collectAsList()
                .get(0).getList(0);
        Assert.assertEquals(0, partitionColumns.size());
        manager.updateLayoutDetails(df.getId(), repartitionLayoutId, (copy) -> {
            copy.setPartitionColumns(Lists.newArrayList(repartitionColName));
        });

        // compaction
        LayoutEntity compactionLayoutEntity = df.getIndexPlan().getAllLayouts().get(1);
        long compactionLayoutId = compactionLayoutEntity.getId();
        optimizeLayoutIds.add(compactionLayoutId);
        String compactionLayoutTablePath = df.getSegmentHdfsPath(segment.getId()) + "/" + compactionLayoutId;
        DeltaTable compactionLayoutTable = DeltaTable.forPath(compactionLayoutTablePath);
        // mock two file layout
        compactionLayoutTable.toDF().write().format("delta").mode("append").save(compactionLayoutTablePath);
        long mockFileNums = compactionLayoutTable.detail().select("numFiles").collectAsList().get(0).getLong(0);
        Assert.assertEquals(2, mockFileNums);

        // zorder
        LayoutEntity zorderLayout = df.getIndexPlan().getAllLayouts().get(2);
        long zorderLayoutId = zorderLayout.getId();
        optimizeLayoutIds.add(zorderLayoutId);
        List<String> zorderColIds = zorderLayout.getDimsIds().subList(0, 1).stream()
                .map(colId -> zorderLayout.getOrderedDimensions().get(colId).getIdentity())
                .collect(Collectors.toList());
        manager.updateLayoutDetails(df.getId(), zorderLayoutId, (copy) -> {
            copy.setZorderByColumns(zorderColIds);
            copy.setMinCompactionFileSizeInBytes(1);
            copy.setMaxCompactionFileSizeInBytes(10000);
        });

        String jobId = "layout-data-optimize-job";
        JobParam jobParam = new JobParam().withProject(getProject()).withJobTypeEnum(JobTypeEnum.LAYOUT_DATA_OPTIMIZE)
                .withTargetLayouts(optimizeLayoutIds);
        jobParam.setJobId(jobId);
        jobParam.setModel(df.getId());
        ExecutableUtil.computeParams(jobParam);
        LayoutDataOptimizeJobParam params = new LayoutDataOptimizeJobParam(jobParam);
        NSparkLayoutDataOptimizeJob job = NSparkLayoutDataOptimizeJob.create(params);
        execMgr.addJob(job);
        // wait job done
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);
        // check repartition
        List<String> optimizedPartitionColumns = repartitionLayoutTable.detail().select("partitionColumns")
                .collectAsList().get(0).getList(0);
        Assert.assertEquals(1, optimizedPartitionColumns.size());
        Assert.assertEquals(Integer.toString(df.getModel().getColumnIdByColumnName(repartitionColName)),
                optimizedPartitionColumns.get(0));
        // check compaction
        long optimizedFileNums = compactionLayoutTable.detail().select("numFiles").collectAsList().get(0).getLong(0);
        Assert.assertEquals(1, optimizedFileNums);
    }

    private NDataSegment buildV3Data() throws InterruptedException {
        String dfName = "7d840904-7b34-4edd-aabd-79df992ef32e";
        String project = "storage_v3_test";
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, project);
        ExecutableManager execMgr = ExecutableManager.getInstance(config, project);

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        cleanupSegments(dfName);
        NDataflow df = dsMgr.getDataflow(dfName);

        NDataSegment oneSeg = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager m = NDataflowManager.getInstance(getTestConfig(), getProject());
            return m.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange("2012-01-01", "2012-02-01"));
        }, project);
        NSpanningTree nSpanningTree = NSpanningTreeFactory.fromLayouts(df.getIndexPlan().getAllLayouts(), df.getUuid());
        for (IndexEntity rootCuboid : nSpanningTree.getRootIndexEntities()) {
            LayoutEntity layout = NCuboidLayoutChooser.selectLayoutForBuild(oneSeg, rootCuboid);
            Assert.assertNull(layout);
        }

        // Round1. Build new segment
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg),
                Sets.newLinkedHashSet(df.getIndexPlan().getAllLayouts()), "ADMIN", JobTypeEnum.INC_BUILD,
                RandomUtil.randomUUIDStr(), null, null, null);
        NSparkCubingStep sparkStep = job.getSparkCubingStep();
        StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // launch the job
        execMgr.addJob(job);

        // wait job done
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);
        return oneSeg;
    }

    private void cleanupSegments(String dfName) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager m = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
            NDataflow df = m.getDataflow(dfName);
            NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
            update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
            return m.updateDataflow(update);
        }, getProject());
    }

}
