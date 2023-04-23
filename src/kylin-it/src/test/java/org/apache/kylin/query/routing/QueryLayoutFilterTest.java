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
import org.apache.kylin.metadata.query.NativeQueryRealization;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.engine.TypeSystem;
import org.apache.kylin.query.engine.meta.SimpleDataContext;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.util.OlapContextTestUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryLayoutFilterTest extends NLocalWithSparkSessionTest {

    @BeforeClass
    public static void initSpark() {
        if (Shell.MAC)
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy
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
        this.createTestMetadata();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    @Test
    public void testQueryWithFilterCondAlwaysFalse() throws Exception {
        String dataflowId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        KylinConfig kylinConfig = getTestConfig();
        String project = "default";
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        NDataflow dataflow = dataflowManager.getDataflow(dataflowId);
        String sql = "SELECT COUNT(*) FROM TEST_BANK_INCOME inner join TEST_BANK_LOCATION "
                + "on TEST_BANK_INCOME.COUNTRY = TEST_BANK_LOCATION.COUNTRY \n" //
                + "WHERE 1 = 1\n" //
                + "and TEST_BANK_INCOME.DT = '2021-11-02'\n" //
                + "and TEST_BANK_INCOME.COUNTRY in ('INDONESIA')\n" //
                + "and TEST_BANK_INCOME.COUNTRY in ('KENYA')";
        List<OLAPContext> contexts = OlapContextTestUtil.getOlapContexts(project, sql);
        OLAPContext context = contexts.get(0);

        CalciteSchema rootSchema = new QueryExec(project, kylinConfig).getRootSchema();
        SimpleDataContext dataContext = new SimpleDataContext(rootSchema.plus(), TypeSystem.javaTypeFactory(),
                kylinConfig);
        context.firstTableScan.getCluster().getPlanner().setExecutor(new RexExecutorImpl(dataContext));
        List<NDataSegment> segments = new SegmentPruningRule().pruneSegments(dataflow, context);
        Assert.assertTrue(segments.isEmpty());
        context.storageContext.setEmptyLayout(true);
        context.realization = dataflow;
        OLAPContext.registerContext(context);
        List<NativeQueryRealization> realizations = OLAPContext.getNativeRealizations();
        Assert.assertEquals(1, realizations.size());
        Assert.assertEquals(QueryMetrics.FILTER_CONFLICT, realizations.get(0).getIndexType());
        Assert.assertEquals(Long.valueOf(-1), realizations.get(0).getLayoutId());
    }
}
