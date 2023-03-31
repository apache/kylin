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

package org.apache.kylin.query.routing;

import java.util.List;
import java.util.Map;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.engine.TypeSystem;
import org.apache.kylin.query.engine.meta.SimpleDataContext;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.util.OlapContextUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

public class SegmentPruningRuleTest extends NLocalWithSparkSessionTest {

    @BeforeClass
    public static void initSpark() {
        if (Shell.MAC) {
            // for snappy
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");
        }
        if (ss != null && !ss.sparkContext().isStopped()) {
            ss.stop();
        }
        sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set("spark.memory.fraction", "0.1");
        // opt memory
        sparkConf.set("spark.shuffle.detectCorrupt", "false");
        // For sinai_poc/query03, enable implicit cross join conversion
        sparkConf.set("spark.sql.crossJoin.enabled", "true");
        sparkConf.set("spark.sql.adaptive.enabled", "true");
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");
        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);

    }

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/multi_partition_date_type");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    private List<NDataSegment> startRealizationPruner(NDataflowManager dataflowManager, String dataflowId, String sql,
            String project, KylinConfig kylinConfig) throws Exception {
        NDataflow dataflow = dataflowManager.getDataflow(dataflowId);
        List<OLAPContext> olapContexts = OlapContextUtil.getOlapContexts(getProject(), sql);
        OLAPContext context = olapContexts.get(0);
        CalciteSchema rootSchema = new QueryExec(project, kylinConfig).getRootSchema();
        SimpleDataContext dataContext = new SimpleDataContext(rootSchema.plus(), TypeSystem.javaTypeFactory(),
                kylinConfig);
        context.firstTableScan.getCluster().getPlanner().setExecutor(new RexExecutorImpl(dataContext));
        Map<String, String> map = RealizationChooser.matchJoins(dataflow.getModel(), context, false, false);
        context.fixModel(dataflow.getModel(), map);
        return new SegmentPruningRule().pruneSegments(dataflow, context);
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    @Test
    public void testSegmentPruningPartitionDateColumnFilter() throws Exception {
        val dataflowId = "3718b614-5191-2254-77e9-f4c5ca64e309";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT * FROM TEST_DB.TEST_FACT_13_10W WHERE DATE_6 >= '2021-10-28' AND DATE_6 < '2021-11-05'";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.TEST_FACT_13_10W WHERE DATE_6 >= CAST('2021-10-28' AS DATE) AND DATE_6 < '2021-11-05'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.TEST_FACT_13_10W WHERE DATE_6 >= CAST('2021-10-28' AS DATE) AND DATE_6 < CAST('2021-11-05' AS DATE)";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
    }

    @Test
    public void testSegmentPruningPartitionDateStrColumnFilter() throws Exception {
        val dataflowId = "00a91916-d31e-ed40-b1ba-4a86765072f6";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT * FROM TEST_DB.TEST_FACT_24_2W WHERE STRING_DATE_20 >= '2021-12-20' AND STRING_DATE_20 < '2021-12-26'";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(6, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.TEST_FACT_24_2W WHERE STRING_DATE_20 >= CAST('2021-12-20' AS DATE) AND STRING_DATE_20 < '2021-12-26'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(6, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.TEST_FACT_24_2W WHERE STRING_DATE_20 >= CAST('2021-12-20' AS DATE) AND STRING_DATE_20 < CAST('2021-12-26' AS DATE)";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(6, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.TEST_FACT_24_2W WHERE STRING_DATE_20 >= CAST('2021-12-20' AS TIMESTAMP) AND STRING_DATE_20 < CAST('2021-12-26' AS TIMESTAMP)";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(6, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.TEST_FACT_24_2W WHERE CAST(STRING_DATE_20 AS TIMESTAMP)  >= '2021-12-20' AND CAST(STRING_DATE_20 AS TIMESTAMP) < '2021-12-26'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(6, selectSegmentList.size());
    }

    @Test
    public void testSegmentPruningPartitionDateStr2ColumnFilter() throws Exception {
        val dataflowId = "cdf17c7b-18e3-9a09-23d1-4e82b7bc9123";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT * FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= '20211220' AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= '20211220' AND STRING_DATE2_24 < '20211224'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
    }

    @Test
    public void testSegmentPruningDateType2Timestamp() throws Exception {
        val dataflowId = "3718b614-5191-2254-77e9-f4c5ca64e310";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE DATE_6 >= '2021-10-28' AND DATE_6 < '2021-11-05'";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE DATE_6 >= CAST('2021-10-28' AS DATE) AND DATE_6 < '2021-11-05'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE DATE_6 >= CAST('2021-10-28' AS DATE) AND DATE_6 < CAST('2021-11-05' AS DATE)";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());

        DataType dateType = new DataType("date", 0, 0);
        ReflectionTestUtils.invokeMethod(SegmentPruningRule.class, "checkAndReformatDateType", "yyyy-MM-dd HH:mm:ss",
                Long.parseLong("1633928400000"), dateType);
    }

    @Test
    public void testSegmentPruningTimestampType2DateType() throws Exception {
        val dataflowId = "3718b614-5191-2254-77e9-f4c5ca64e311";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE TIMESTAMP_10 >= '2021-10-28' AND TIMESTAMP_10 < '2021-11-05'";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(5, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE TIMESTAMP_10 >= CAST('2021-10-28' AS DATE) AND TIMESTAMP_10 < '2021-11-05'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(5, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE TIMESTAMP_10 >= CAST('2021-10-28' AS DATE) AND TIMESTAMP_10 < CAST('2021-11-05' AS DATE)";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(5, selectSegmentList.size());

        DataType dateType = new DataType("date", 0, 0);
        ReflectionTestUtils.invokeMethod(SegmentPruningRule.class, "checkAndReformatDateType", "yyyy-MM-dd HH:mm:ss",
                Long.parseLong("1633928400000"), dateType);
    }

    @Test
    public void testSegmentPruningTimestampType() throws Exception {
        val dataflowId = "3718b614-5191-2254-77e9-f4c5ca64e312";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE TIMESTAMP_10 >= '2021-10-28' AND TIMESTAMP_10 < '2021-11-05'";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(5, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE TIMESTAMP_10 >= CAST('2021-10-28' AS DATE) AND TIMESTAMP_10 < '2021-11-05'";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(5, selectSegmentList.size());
        sql = "SELECT * FROM TEST_DB.DATE_TIMESTAMP_TABLE WHERE TIMESTAMP_10 >= CAST('2021-10-28' AS DATE) AND TIMESTAMP_10 < CAST('2021-11-05' AS DATE)";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(5, selectSegmentList.size());

        DataType dateType = new DataType("date", 0, 0);
        ReflectionTestUtils.invokeMethod(SegmentPruningRule.class, "checkAndReformatDateType", "yyyy-MM-dd HH:mm:ss",
                Long.parseLong("1633928400000"), dateType);
    }

    @Test
    public void testSegmentPruningMaxMeasureBeforeDaysSuccess() throws Exception {
        overwriteSystemProp("kylin.query.calcite.extras-props.conformance", "DEFAULT");
        val dataflowId = "cdf17c7b-18e3-9a09-23d1-4e82b7bc9123";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        String sql = "SELECT MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());

        overwriteSystemProp("kylin.query.max-measure-segment-pruner-before-days", "0");
        sql = "SELECT MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(0, selectSegmentList.size());

        overwriteSystemProp("kylin.query.max-measure-segment-pruner-before-days", "2");
        sql = "SELECT MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());

        sql = "SELECT MAX(STRING_DATE2_24), MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());

        sql = "SELECT MAX(STRING_DATE2_24), COUNT(*) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());

        sql = "SELECT MAX(STRING_DATE2_24), COUNT(1) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());

        sql = "SELECT MAX(STRING_DATE2_24), MIN(1) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());

        sql = "SELECT 1, MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());

        sql = "SELECT 1, MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224 GROUP BY 1";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());

        sql = "SELECT STRING_DATE2_24, MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224 GROUP BY STRING_DATE2_24";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());

        sql = "SELECT SUM(1), COUNT(DISTINCT 1), MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224 GROUP BY STRING_DATE2_24";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(2, selectSegmentList.size());
    }

    @Test
    public void testSegmentPruningMaxMeasureBeforeDaysFail() throws Exception {
        overwriteSystemProp("kylin.query.calcite.extras-props.conformance", "DEFAULT");
        val dataflowId = "cdf17c7b-18e3-9a09-23d1-4e82b7bc9123";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);

        // test non-max
        overwriteSystemProp("kylin.query.max-measure-segment-pruner-before-days", "2");
        String sql = "SELECT MIN(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());

        // test non time partition column
        sql = "SELECT MAX(STRING_DATE_20) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());

        // test multi aggregations
        sql = "SELECT MAX(STRING_DATE2_24), MIN(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());

        // test no aggregations
        sql = "SELECT STRING_DATE2_24 FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());

        // test multi group by column
        sql = "SELECT MAX(STRING_DATE2_24),STRING_DATE_20 FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224 GROUP BY STRING_DATE_20";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(4, selectSegmentList.size());

        // test dataformat is null
        dataflowManager.getDataflow(dataflowId).getModel().getPartitionDesc().setPartitionDateFormat(null);
        sql = "SELECT MAX(STRING_DATE2_24) FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 >= 20211220 AND STRING_DATE2_24 < 20211224";
        selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project, kylinConfig);
        Assert.assertEquals(18, selectSegmentList.size());
    }

    @Test
    public void testSegmentPruningFilterAlwaysFalse() throws Exception {
        overwriteSystemProp("kylin.query.calcite.extras-props.conformance", "DEFAULT");
        val dataflowId = "cdf17c7b-18e3-9a09-23d1-4e82b7bc9123";
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);

        String sql = "SELECT * FROM (SELECT STRING_DATE2_24,STRING_DATE_20 FROM TEST_DB.TEST_FACT_30_3W WHERE STRING_DATE2_24 = 20211221) AS T1 WHERE STRING_DATE2_24 = 20211220";
        List<NDataSegment> selectSegmentList = startRealizationPruner(dataflowManager, dataflowId, sql, project,
                kylinConfig);
        Assert.assertEquals(0, selectSegmentList.size());
    }

    @Override
    public String getProject() {
        return "multi_partition_date_type";
    }

}
