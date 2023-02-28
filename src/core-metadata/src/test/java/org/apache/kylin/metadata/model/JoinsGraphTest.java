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

package org.apache.kylin.metadata.model;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.model.graph.DefaultJoinEdgeMatcher;
import org.apache.kylin.metadata.model.graph.JoinsGraph;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JoinsGraphTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @Test
    public void testTableNotFound() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        Assert.assertThrows(KylinException.class, () -> modelDesc.findTable("not_exits"));
    }

    @Test
    public void testMatch() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        JoinsGraph orderIJFactGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .innerJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "TEST_KYLIN_FACT.ORDER_ID" }).build();
        JoinsGraph factIJOrderGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" }).build();
        Assert.assertTrue(orderIJFactGraph.match(factIJOrderGraph, new HashMap<>()));

        JoinsGraph orderLJfactGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .leftJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "TEST_KYLIN_FACT.ORDER_ID" }).build();
        JoinsGraph factLJorderGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" }).build();
        Assert.assertFalse(orderLJfactGraph.match(factLJorderGraph, new HashMap<>()));
    }

    @Test
    public void testInnerJoinPartialMatch() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        JoinsGraph innerJoinGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .innerJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        JoinsGraph innerAndInnerJoinGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .innerJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" })
                .innerJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_COUNTRY" }, new String[] { "BUYER_COUNTRY.COUNTRY" })
                .build();
        Assert.assertFalse(innerJoinGraph.match(innerAndInnerJoinGraph, new HashMap<>(),
                KylinConfig.getInstanceFromEnv().isQueryMatchPartialInnerJoinModel()));
        overwriteSystemProp("kylin.query.match-partial-inner-join-model", "true");
        Assert.assertTrue(innerJoinGraph.match(innerAndInnerJoinGraph, new HashMap<>(),
                KylinConfig.getInstanceFromEnv().isQueryMatchPartialInnerJoinModel()));
    }

    @Test
    public void testInnerJoinPartialMatchProjectConfig() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        JoinsGraph innerJoinGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .innerJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        JoinsGraph innerAndInnerJoinGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .innerJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" })
                .innerJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_COUNTRY" }, new String[] { "BUYER_COUNTRY.COUNTRY" })
                .build();
        Assert.assertFalse(innerJoinGraph.match(innerAndInnerJoinGraph, new HashMap<>(),
                NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject("default").getConfig()
                        .isQueryMatchPartialInnerJoinModel()));

        overrideProjectConfig(new HashMap<String, String>() {
            {
                put("kylin.query.match-partial-inner-join-model", "true");
            }
        });

        Assert.assertTrue(innerJoinGraph.match(innerAndInnerJoinGraph, new HashMap<>(),
                NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject("default").getConfig()
                        .isQueryMatchPartialInnerJoinModel()));

        // clean
        overrideProjectConfig(new HashMap<String, String>() {
            {
                put("kylin.query.match-partial-inner-join-model", "false");
            }
        });
    }

    private void overrideProjectConfig(Map<String, String> overrideKylinProps) {
        NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).updateProject("default", copyForWrite -> {
            LinkedHashMap<String, String> map = KylinConfig.trimKVFromMap(overrideKylinProps);
            copyForWrite.getOverrideKylinProps().putAll(map);
        });
    }

    @Test
    public void testMatchDupJoinTable() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" })
                .innerJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_COUNTRY" }, new String[] { "BUYER_COUNTRY.COUNTRY" })
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "SELLER_ACCOUNT.ACCOUNT_ID" })
                .build();
        JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "SELLER_ACCOUNT.ACCOUNT_ID" })
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" })
                .innerJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_COUNTRY" }, new String[] { "BUYER_COUNTRY.COUNTRY" })
                .build();
        Assert.assertTrue(graph1.match(graph2, new HashMap<>()));
        Assert.assertTrue(graph2.match(graph1, new HashMap<>()));

        JoinsGraph graph3 = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" })
                .leftJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_COUNTRY" }, new String[] { "BUYER_COUNTRY.COUNTRY" })
                .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "SELLER_ACCOUNT.ACCOUNT_ID" }).build();
        JoinsGraph graph4 = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "SELLER_ACCOUNT.ACCOUNT_ID" })
                .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" })
                .leftJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_COUNTRY" }, new String[] { "BUYER_COUNTRY.COUNTRY" })
                .build();
        Assert.assertTrue(graph3.match(graph4, new HashMap<>()));
        Assert.assertTrue(graph4.match(graph3, new HashMap<>()));
    }

    @Test
    public void testMatchLeft() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        JoinsGraph modelJoinsGraph = modelDesc.getJoinsGraph();

        JoinsGraph singleTblGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT").build();
        Assert.assertTrue(modelJoinsGraph.match(modelJoinsGraph, new HashMap<>()));
        Assert.assertTrue(singleTblGraph.match(singleTblGraph, new HashMap<>()));
        Assert.assertTrue(singleTblGraph.match(modelJoinsGraph, new HashMap<>()));
        Assert.assertFalse(modelJoinsGraph.match(singleTblGraph, new HashMap<>()));

        JoinsGraph noFactGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        Assert.assertFalse(noFactGraph.match(modelJoinsGraph, new HashMap<>()));

        JoinsGraph factJoinGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        Assert.assertTrue(factJoinGraph.match(modelJoinsGraph, new HashMap<>()));

        JoinsGraph joinedFactGraph = new MockJoinGraphBuilder(modelDesc, "BUYER_ACCOUNT")
                .leftJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }, new String[] { "TEST_ORDER.BUYER_ID" })
                .leftJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "TEST_KYLIN_FACT.ORDER_ID" }).build();
        Assert.assertFalse(joinedFactGraph.match(factJoinGraph, new HashMap<>()));
    }

    @Test
    public void testMatchInner() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDescByAlias("nmodel_basic_inner");
        JoinsGraph modelJoinsGraph = modelDesc.getJoinsGraph();

        JoinsGraph singleTblGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT").build();
        Assert.assertTrue(modelJoinsGraph.match(modelJoinsGraph, new HashMap<>()));
        Assert.assertTrue(singleTblGraph.match(singleTblGraph, new HashMap<>()));
        Assert.assertFalse(singleTblGraph.match(modelJoinsGraph, new HashMap<>()));
        Assert.assertFalse(modelJoinsGraph.match(singleTblGraph, new HashMap<>()));

        JoinsGraph noFactGraph = new MockJoinGraphBuilder(modelDesc, "TEST_ORDER")
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        Assert.assertFalse(noFactGraph.match(modelJoinsGraph, new HashMap<>()));

        JoinsGraph factJoinGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        Assert.assertFalse(factJoinGraph.match(modelJoinsGraph, new HashMap<>()));

        JoinsGraph joinedFactGraph = new MockJoinGraphBuilder(modelDesc, "BUYER_ACCOUNT")
                .innerJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }, new String[] { "TEST_ORDER.BUYER_ID" })
                .innerJoin(new String[] { "TEST_ORDER.ORDER_ID" }, new String[] { "TEST_KYLIN_FACT.ORDER_ID" }).build();
        Assert.assertTrue(joinedFactGraph.match(factJoinGraph, new HashMap<>()));
    }

    @Test
    public void testPartialMatch() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDescByAlias("nmodel_basic_inner");
        JoinsGraph modelJoinsGraph = modelDesc.getJoinsGraph();

        JoinsGraph factJoinGraph = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }).build();
        Assert.assertTrue(factJoinGraph.match(modelJoinsGraph, new HashMap<>(), true));
    }

    @Test
    public void testNonEquiLeftJoinGraphMatch() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDescByAlias("nmodel_basic_inner");

        {
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiLeftJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiLeftJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertTrue(graph1.match(graph2, new HashMap<>()));
        }

        {
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiLeftJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "SELLER_ACCOUNT.ACCOUNT_ID" })
                    .nonEquiLeftJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertTrue(graph1.match(graph2, new HashMap<>()));
        }

        {
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiLeftJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertFalse(graph1.match(graph2, new HashMap<>()));
        }

        {
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT").build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiLeftJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertFalse(graph1.match(graph2, new HashMap<>()));
        }

        {
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT").build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiLeftJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertTrue(graph1.match(graph2, new HashMap<>(), false, true));

            graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .leftJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .build();
            Assert.assertTrue(graph1.match(graph2, new HashMap<>(), false, true));
        }

    }

    @Test
    public void testNonEquiInnerJoinGraphMatch() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDescByAlias("nmodel_basic_inner");

        Supplier<HashMap<String, String>> matchesMapSupplier = HashMap::new;

        {
            //exactly match
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiInnerJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiInnerJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertTrue(graph1.match(graph2, matchesMapSupplier.get()));
        }

        {
            //partial match
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiInnerJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .leftJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "SELLER_ACCOUNT.ACCOUNT_ID" })
                    .nonEquiInnerJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertTrue(graph1.match(graph2, matchesMapSupplier.get()));
        }

        {
            //partial match inner join is not allowed
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiInnerJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertFalse(graph1.match(graph2, matchesMapSupplier.get()));
        }

        {
            //partial match inner join is not allowed-2
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT").build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiInnerJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            Assert.assertFalse(graph1.match(graph2, matchesMapSupplier.get()));
        }

        {
            JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .build();
            JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                    .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                    .nonEquiInnerJoin("BUYER_ACCOUNT", "TEST_ORDER", "TEST_ORDER.BUYER_ID").build();
            //partial match inner join is allowed even if kylin.query.match-partial-non-equi-join-model is on
            Assert.assertFalse(graph1.match(graph2, matchesMapSupplier.get(), false, true));
            //partial match inner join is allowed
            Assert.assertTrue(graph1.match(graph2, matchesMapSupplier.get(), true, true));
        }
    }

    @Test
    public void testColumnDescEquals() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        NTableMetadataManager manager = NTableMetadataManager.getInstance(getTestConfig(), "default");
        TableDesc tableDesc = manager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        DefaultJoinEdgeMatcher matcher = new DefaultJoinEdgeMatcher();
        ColumnDesc one = new ColumnDesc();
        one.setTable(tableDesc);
        one.setName("one");
        ColumnDesc two = new ColumnDesc();
        two.setTable(tableDesc);
        two.setName("two");
        ColumnDesc copyOfOne = new ColumnDesc(one);
        ColumnDesc copyOfTwo = new ColumnDesc(two);
        ColumnDesc[] a = new ColumnDesc[] { one, two };
        ColumnDesc[] b = new ColumnDesc[] { copyOfTwo, copyOfOne };
        Method method = matcher.getClass().getDeclaredMethod("columnDescEquals", ColumnDesc[].class,
                ColumnDesc[].class);
        Unsafe.changeAccessibleObject(method, true);
        Object invoke = method.invoke(matcher, a, b);
        Assert.assertEquals(true, invoke);
    }

    @Test
    public void testToString() {
        NDataModel modelDesc = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDescByAlias("nmodel_basic_inner");
        JoinsGraph graph1 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" }).build();
        Assert.assertTrue(StringUtils.isNotEmpty(graph1.toString()));

        JoinsGraph graph2 = new MockJoinGraphBuilder(modelDesc, "TEST_KYLIN_FACT")
                .innerJoin(new String[] { "TEST_KYLIN_FACT.ORDER_ID" }, new String[] { "TEST_ORDER.ORDER_ID" })
                .innerJoin(new String[] { "TEST_ORDER.BUYER_ID" }, new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" })
                .innerJoin(new String[] { "BUYER_ACCOUNT.ACCOUNT_COUNTRY" }, new String[] { "BUYER_COUNTRY.COUNTRY" })
                .build();
        Assert.assertTrue(StringUtils.isNotEmpty(graph2.toString()));
    }
}
