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
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.job.NSparkSnapshotJob;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class NBuildAndQuerySnapshotTest extends NLocalWithSparkSessionTest {

    private KylinConfig config;
    private NDataflowManager dsMgr;

    @Before
    public void setUp() throws Exception {
        super.init();
        config = KylinConfig.getInstanceFromEnv();
        dsMgr = NDataflowManager.getInstance(config, getProject());
        indexDataConstructor = new IndexDataConstructor(getProject());
    }

    @After
    public void cleanup() throws Exception {
        JobContextUtil.cleanUp();
        super.cleanupTestMetadata();
    }

    @Test
    public void testBasic() throws Exception {
        String dataflowName = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        cleanUpSegmentsAndIndexPlan(dataflowName);

        // before build snapshot
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, getProject());
        Assert.assertNull(tableMetadataManager.getTableDesc("DEFAULT.TEST_COUNTRY").getLastSnapshotPath());

        // build
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());
        buildCube(dataflowName, SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("2012-02-01"));

        // after build
        String lastSnapshotPath = tableMetadataManager.getTableDesc("DEFAULT.TEST_COUNTRY").getLastSnapshotPath();
        Assert.assertNotNull(lastSnapshotPath);
        Dataset dataset = ExecAndComp.queryModelWithoutCompute(getProject(), "select NAME from TEST_COUNTRY");
        Assert.assertEquals(244, dataset.collectAsList().size());
    }

    private void cleanUpSegmentsAndIndexPlan(String dfName) {
        NIndexPlanManager ipMgr = NIndexPlanManager.getInstance(config, getProject());
        String cubeId = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getIndexPlan().getUuid();
        IndexPlan cube = ipMgr.getIndexPlan(cubeId);
        Set<Long> tobeRemovedLayouts = cube.getAllLayouts().stream().filter(layout -> layout.getId() != 10001L)
                .map(LayoutEntity::getId).collect(Collectors.toSet());

        UnitOfWork.doInTransactionWithRetry(() -> {
            NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).updateIndexPlan(
                    dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getIndexPlan().getUuid(),
                    copyForWrite -> copyForWrite.removeLayouts(tobeRemovedLayouts, true, true));
            return null;
        }, getProject());

        NDataflow df = dsMgr.getDataflow(dfName);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        UnitOfWork.doInTransactionWithRetry(() -> {
            NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).updateDataflow(update);
            return null;
        }, getProject());
    }

    private void buildCube(String dfName, long start, long end) throws Exception {
        NDataflow df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);
    }

    @Test
    public void testQueryPartitionSnapshot() throws Exception {
        String tableName = "EDW.TEST_SELLER_TYPE_DIM";
        String partitionCol = "SELLER_TYPE_CD";
        Set<String> partitions = ImmutableSet.of("5", "16");
        UnitOfWork.doInTransactionWithRetry(() -> {
            NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, getProject());
            TableDesc table = tableManager.getTableDesc(tableName);
            table.setSelectedSnapshotPartitionCol(partitionCol);
            table.setPartitionColumn(partitionCol);
            tableManager.updateTableDesc(table);
            return null;
        }, getProject());

        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, getProject());
        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());
        NSparkSnapshotJob job = NSparkSnapshotJob.create(tableManager.getTableDesc(tableName), "ADMIN",
                JobTypeEnum.SNAPSHOT_BUILD, RandomUtil.randomUUIDStr(), partitionCol, "false", null);
        setPartitions(job, partitions);
        execMgr.addJob(job);

        // wait job done
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        String sql = "select * from EDW.TEST_SELLER_TYPE_DIM";
        QueryExec queryExec = new QueryExec(getProject(), KylinConfig.getInstanceFromEnv());
        val resultSet = queryExec.executeQuery(sql);
        Assert.assertEquals(2, resultSet.getRows().size());
    }

    private void setPartitions(NSparkSnapshotJob job, Set<String> partitions) {
        job.setParam("partitions", String.join(",", partitions));
        job.getSnapshotBuildingStep().setParam("partitions", String.join(",", partitions));
    }

}
