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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.engine.SchemaMetaData;
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

public class TableIndexAnswerSelectStarTest extends NLocalWithSparkSessionTest {

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
        this.createTestMetadata("src/test/resources/ut_meta/tableindex_answer_selectstart");
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testTableIndexAnswerSelectStarPartialMatch() throws Exception {
        String sql = "select * from kylin_sales";
        overwriteSystemProp("kylin.query.use-tableindex-answer-select-star.enabled", "true");
        OLAPContext context = OlapContextUtil.getOlapContexts(getProject(), sql).get(0);
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getDataflow("ccb82d81-1497-ca6d-f226-3258a0f0ba4f");
        Assert.assertEquals(dataflow.getAllColumns().size(), context.allColumns.size());
        Map<String, String> sqlAlias2ModelName = OlapContextUtil.matchJoins(dataflow.getModel(), context);
        context.fixModel(dataflow.getModel(), sqlAlias2ModelName);
        NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                dataflow.getQueryableSegments(), context.getSQLDigest(), null);
        Assert.assertNotNull(layoutCandidate);
        Assert.assertEquals(20000000001L, layoutCandidate.getLayoutEntity().getId());
    }

    @Test
    public void testTableIndexAnswerSelectStarBaseTableIndex() throws Exception {
        String sql = "select * from test_kylin_fact \n";
        overwriteSystemProp("kylin.query.use-tableindex-answer-select-star.enabled", "true");
        OLAPContext context = OlapContextUtil.getOlapContexts(getProject(), sql).get(0);
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getDataflow("c7a44f37-8481-e78b-5cac-faa7d76767db");
        Assert.assertEquals(dataflow.getAllColumns().size(), context.allColumns.size());
        Map<String, String> sqlAlias2ModelName = OlapContextUtil.matchJoins(dataflow.getModel(), context);
        context.fixModel(dataflow.getModel(), sqlAlias2ModelName);
        NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                dataflow.getQueryableSegments(), context.getSQLDigest(), null);
        Assert.assertNotNull(layoutCandidate);
        Assert.assertEquals(20000010001L, layoutCandidate.getLayoutEntity().getId());
    }

    @Test
    public void testTableIndexAnswerSelectStarModelHavingCCs() throws Exception {
        overwriteSystemProp("kylin.query.use-tableindex-answer-select-star.enabled", "true");
        overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");
        String modelId = "c7a44f37-8481-e78b-5cac-faa7d76767db";
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());

        dataflowManager.updateDataflowStatus("baa44f37-8481-e78b-5cac-faa7d76767db", RealizationStatusEnum.OFFLINE);
        modelManager.updateDataModel(modelId, copyForWrite -> {
            ComputedColumnDesc newCC = new ComputedColumnDesc();
            newCC.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
            newCC.setTableAlias("TEST_KYLIN_FACT");
            newCC.setComment("");
            newCC.setColumnName("NEW_CC");
            newCC.setDatatype("BIGINT");
            newCC.setExpression("TEST_KYLIN_FACT.ORDER_ID + 1");
            newCC.setInnerExpression("`TEST_KYLIN_FACT`.`ORDER_ID` + 1");
            copyForWrite.getComputedColumnDescs().add(newCC);

            NDataModel.NamedColumn newCol = new NDataModel.NamedColumn();
            newCol.setName("NEW_CC");
            newCol.setId(copyForWrite.getAllNamedColumns().size());
            newCol.setAliasDotColumn("TEST_KYLIN_FACT.NEW_CC");
            newCol.setStatus(NDataModel.ColumnStatus.DIMENSION);
            copyForWrite.getAllNamedColumns().add(newCol);
        });

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelId);
        Long oldBaseAggLayout = indexPlan.getBaseAggLayoutId();
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> copyForWrite
                .markWhiteIndexToBeDelete(indexPlan.getUuid(), Sets.newHashSet(oldBaseAggLayout), new HashMap<>()));
        NDataModel model = modelManager.getDataModelDesc(modelId);
        LayoutEntity newBaseAggLayout = indexPlan.createBaseAggIndex(model);
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(),
                copyForWrite -> copyForWrite.createAndAddBaseIndex(Lists.newArrayList(newBaseAggLayout)));

        NDataflow dataflow = dataflowManager.getDataflow(modelId);
        String segId = "87d65498-b922-225c-1db7-13de001beba8";
        NDataLayout baseAggLayout = dataflow.getSegment(segId).getLayout(1L);
        NDataLayout baseTableLayout = dataflow.getSegment(segId).getLayout(20000010001L);
        Class<RootPersistentEntity> clazz = RootPersistentEntity.class;
        Field field = clazz.getDeclaredField("isCachedAndShared");
        Unsafe.changeAccessibleObject(field, true);
        field.set(baseAggLayout.getSegDetails(), false);
        field.set(baseTableLayout.getSegDetails(), false);
        baseAggLayout.setLayoutId(10001L);
        NDataflowUpdate updateOps = new NDataflowUpdate(modelId);
        updateOps.setToAddOrUpdateLayouts(baseAggLayout, baseTableLayout);
        dataflowManager.updateDataflow(updateOps);

        String sql = "select cal_dt, new_cc from test_kylin_fact";
        OLAPContext context = OlapContextUtil.getOlapContexts(getProject(), sql).get(0);
        Set<TblColRef> allColumns = context.realization.getAllColumns();
        Assert.assertEquals(13, allColumns.size());
        SchemaMetaData schemaMetaData = new SchemaMetaData(getProject(), KylinConfig.getInstanceFromEnv());
        Assert.assertEquals(26, schemaMetaData.getTables().get(1).getFields().size());

        Map<String, String> sqlAlias2ModelName = OlapContextUtil.matchJoins(dataflow.getModel(), context);
        context.fixModel(dataflow.getModel(), sqlAlias2ModelName);
        NLayoutCandidate layoutCandidate = QueryLayoutChooser.selectLayoutCandidate(dataflow,
                dataflow.getQueryableSegments(), context.getSQLDigest(), null);
        Assert.assertNotNull(layoutCandidate);
        Assert.assertEquals(20000010001L, layoutCandidate.getLayoutEntity().getId());
    }

    @Override
    public String getProject() {
        return "tableindex_answer_selectstart";
    }

}
