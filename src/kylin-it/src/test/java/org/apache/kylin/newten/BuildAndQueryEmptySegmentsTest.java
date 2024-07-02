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
import org.apache.kylin.engine.spark.job.NSparkMergingJob;
import org.apache.kylin.engine.spark.merger.AfterMergeOrRefreshResourceMerger;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BuildAndQueryEmptySegmentsTest extends NLocalWithSparkSessionTest {

    private static final String DF_NAME1 = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
    private static final String DF_NAME2 = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";

    private static final String SQL = "select\n" + " count(1) as TRANS_CNT \n" + " from test_kylin_fact \n"
            + " group by trans_id";

    private static final String SQL_DERIVED = "SELECT \n" + "test_cal_dt.season_beg_dt\n"
            + "FROM test_kylin_fact LEFT JOIN edw.test_cal_dt as test_cal_dt \n"
            + "ON test_kylin_fact.cal_dt=test_cal_dt.cal_dt \n"
            + "WHERE test_kylin_fact.cal_dt>'2009-06-01' and test_kylin_fact.cal_dt<'2013-01-01' \n"
            + "GROUP BY test_cal_dt.season_beg_dt";

    private static final String SQL_DERIVED_AGG = "select count(*) from (SELECT \n" + "test_cal_dt.season_beg_dt\n"
            + "FROM test_kylin_fact LEFT JOIN edw.test_cal_dt as test_cal_dt \n"
            + "ON test_kylin_fact.cal_dt=test_cal_dt.cal_dt \n"
            + "WHERE test_kylin_fact.cal_dt>'2009-06-01' and test_kylin_fact.cal_dt<'2013-01-01' \n"
            + "GROUP BY test_cal_dt.season_beg_dt)";

    private KylinConfig config;
    private NDataflowManager dsMgr;
    private ExecutableManager execMgr;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        config = KylinConfig.getInstanceFromEnv();
        dsMgr = NDataflowManager.getInstance(config, getProject());
        execMgr = ExecutableManager.getInstance(config, getProject());
        NIndexPlanManager ipMgr = NIndexPlanManager.getInstance(config, getProject());
        String cubeId = dsMgr.getDataflow(DF_NAME1).getIndexPlan().getUuid();
        IndexPlan cube = ipMgr.getIndexPlan(cubeId);
        Set<Long> tobeRemovedLayouts = cube.getAllLayouts().stream() //
                .map(LayoutEntity::getId).filter(id -> id != 10001L).collect(Collectors.toSet());

        UnitOfWork.doInTransactionWithRetry(() -> {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            IndexPlan indexPlan = NIndexPlanManager.getInstance(kylinConfig, getProject())
                    .updateIndexPlan(
                            NDataflowManager.getInstance(kylinConfig, getProject()).getDataflow(DF_NAME1).getIndexPlan()
                                    .getUuid(),
                            copyForWrite -> copyForWrite.removeLayouts(tobeRemovedLayouts, true, true));
            System.out.println("Init success, the remaining layouts are:" + indexPlan.getAllLayouts());
            return null;
        }, getProject());
    }

    @After
    public void cleanup() throws Exception {
        JobContextUtil.cleanUp();
        super.cleanupTestMetadata();
    }

    @Test
    public void testEmptySegments() throws Exception {
        UnitOfWork.doInTransactionWithRetry(() -> {
            KylinConfig testConfig = KylinConfig.getInstanceFromEnv();
            NDataflowManager dataflowManager = NDataflowManager.getInstance(testConfig, getProject());
            dataflowManager.updateDataflowStatus(DF_NAME2, RealizationStatusEnum.OFFLINE);
            cleanupSegments(DF_NAME1, dataflowManager);
            populateSSWithCSVData(testConfig, getProject(), SparderEnv.getSparkSession());
            return null;
        }, getProject());

        buildCube(DF_NAME1, SegmentRange.dateToLong("2009-01-01"), SegmentRange.dateToLong("2009-06-01"));
        Assert.assertEquals(0, dsMgr.getDataflow(DF_NAME1).getSegments().get(0).getSegDetails().getTotalRowCount());

        testQueryUnequal(SQL);
        testQueryUnequal(SQL_DERIVED);
        testQuery(SQL_DERIVED_AGG);

        buildCube(DF_NAME1, SegmentRange.dateToLong("2009-06-01"), SegmentRange.dateToLong("2010-01-01"));
        Assert.assertEquals(0, dsMgr.getDataflow(DF_NAME1).getSegments().get(1).getSegDetails().getTotalRowCount());
        buildCube(DF_NAME1, SegmentRange.dateToLong("2010-01-01"), SegmentRange.dateToLong("2012-01-01"));
        Assert.assertEquals(0, dsMgr.getDataflow(DF_NAME1).getSegments().get(2).getSegDetails().getTotalRowCount());
        buildCube(DF_NAME1, SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("2015-01-01"));
        Assert.assertNotEquals(0, dsMgr.getDataflow(DF_NAME1).getSegments().get(3).getSegDetails().getTotalRowCount());

        mergeSegments("2009-01-01", "2010-01-01", true);
        mergeSegments("2010-01-01", "2015-01-01", true);

        testQuery(SQL);
        testQuery(SQL_DERIVED);
        testQuery(SQL_DERIVED_AGG);
        UnitOfWork.doInTransactionWithRetry(() -> {
            NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    getProject());
            dataflowManager.updateDataflowStatus(DF_NAME2, RealizationStatusEnum.ONLINE);
            return null;
        }, getProject());

    }

    private void cleanupSegments(String dfName, NDataflowManager dsMgr) {
        NDataflow df = dsMgr.getDataflow(dfName);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
    }

    private void buildCube(String dfName, long start, long end) throws Exception {
        NDataflow df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);
    }

    private void mergeSegments(String start, String end, boolean force) throws Exception {
        NDataflow df = dsMgr.getDataflow(DF_NAME1);
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        NDataSegment emptyMergeSeg = UnitOfWork.doInTransactionWithRetry(() -> {
            NDataflowManager manager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
            return manager.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                    SegmentRange.dateToLong(start), SegmentRange.dateToLong(end)), force);
        }, getProject());

        NSparkMergingJob emptyMergeJob = NSparkMergingJob.merge(emptyMergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN",
                RandomUtil.randomUUIDStr());
        execMgr.addJob(emptyMergeJob);
        Assert.assertEquals(ExecutableState.SUCCEED, IndexDataConstructor.wait(emptyMergeJob));
        UnitOfWork.doInTransactionWithRetry(() -> {
            AfterMergeOrRefreshResourceMerger merger = new AfterMergeOrRefreshResourceMerger(
                    KylinConfig.getInstanceFromEnv(), getProject());
            merger.merge(emptyMergeJob.getSparkMergingStep());
            return null;
        }, getProject());

    }

    private void testQuery(String sqlStr) {
        Dataset dsFromCube = ExecAndComp.queryModelWithoutCompute(getProject(), sqlStr);
        Assert.assertNotEquals(0L, dsFromCube.count());
        String sql = convertToSparkSQL(sqlStr);
        Dataset dsFromSpark = ExecAndComp.querySparkSql(sql);
        Assert.assertEquals(dsFromCube.count(), dsFromSpark.count());
    }

    private void testQueryUnequal(String sqlStr) {

        Dataset dsFromCube = ExecAndComp.queryModelWithoutCompute(getProject(), sqlStr);
        if (dsFromCube != null) {
            Assert.assertEquals(0L, dsFromCube.count());
            String sql = convertToSparkSQL(sqlStr);
            Dataset dsFromSpark = ExecAndComp.querySparkSql(sql);
            Assert.assertNotEquals(dsFromCube.count(), dsFromSpark.count());
        }
    }

    private String convertToSparkSQL(String sqlStr) {
        return sqlStr.replaceAll("edw\\.", "");
    }

}
