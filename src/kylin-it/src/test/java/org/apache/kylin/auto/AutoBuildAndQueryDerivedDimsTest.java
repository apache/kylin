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

package org.apache.kylin.auto;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.model.ModelJoinRelationTypeEnum;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.util.scd2.SCD2CondChecker;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.ExecAndComp.CompareLevel;
import org.apache.kylin.util.MetadataTestUtils;
import org.apache.kylin.util.SuggestTestBase;
import org.junit.Assert;
import org.junit.Test;

public class AutoBuildAndQueryDerivedDimsTest extends SuggestTestBase {

    private static final String NON_EQUIV_JOIN_FOLDER = "query/sql_derived_non_equi_join";
    private static final String SCD2_JOIN_FOLDER = "query/sql_derived_non_equi_join/scd2_all_forms";
    private static final String EQUIV_JOIN_FOLDER = "query/sql_derived_equi_join";

    @Test
    public void testNonEquiJoinDerived() throws Exception {
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.query.print-logical-plan", "TRUE");
        overwriteSystemProp("kylin.model.non-equi-join-recommendation-enabled", "TRUE");

        MetadataTestUtils.toSemiAutoMode(getProject());
        proposeBuildAndCompare(new TestScenario(CompareLevel.SAME, JoinType.LEFT, NON_EQUIV_JOIN_FOLDER), true);
        proposeBuildAndCompare(new TestScenario(CompareLevel.SAME, JoinType.INNER, NON_EQUIV_JOIN_FOLDER), true);
    }

    @Test
    public void testScd2Condition() throws Exception {
        overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.query.print-logical-plan", "TRUE");
        overwriteSystemProp("kylin.model.non-equi-join-recommendation-enabled", "TRUE");

        String[] sqls = new String[] { "select TEST_ORDER.ORDER_ID, BUYER_ID from TEST_ORDER\n"
                + "         left join TEST_KYLIN_FACT on TEST_ORDER.ORDER_ID = TEST_KYLIN_FACT.ORDER_ID\n"
                + "    and BUYER_ID >= SELLER_ID and BUYER_ID < LEAF_CATEG_ID" };

        AbstractContext context = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), sqls, true);
        MetadataTestUtils.toSemiAutoMode(getProject());
        Assert.assertEquals(1, context.getModelContexts().size());

        TestScenario testScenario = new TestScenario(CompareLevel.SAME, JoinType.DEFAULT, SCD2_JOIN_FOLDER);
        collectQueries(Lists.newArrayList(testScenario));
        buildAndCompare(BuildAndCompareContext.builder().testScenarios(Lists.newArrayList(testScenario)).build());
    }

    @Test
    public void testScd2FlatTableSql() {
        overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.model.non-equi-join-recommendation-enabled", "TRUE");

        String[] sqls = new String[] {
                "select TEST_ORDER.ORDER_ID, BUYER_ID from TEST_KYLIN_FACT \n"
                        + "         left join TEST_ORDER  on TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n"
                        + "    and BUYER_ID >= SELLER_ID and BUYER_ID < LEAF_CATEG_ID",
                "select TEST_ORDER.ORDER_ID, BUYER_ID from TEST_ORDER\n"
                        + "         left join TEST_KYLIN_FACT on TEST_ORDER.ORDER_ID = TEST_KYLIN_FACT.ORDER_ID\n"
                        + "    and BUYER_ID >= SELLER_ID and BUYER_ID < LEAF_CATEG_ID",
                "select TEST_ORDER.ORDER_ID, BUYER_ID from TEST_ORDER\n"
                        + "         left join TEST_KYLIN_FACT on TEST_ORDER.ORDER_ID = TEST_KYLIN_FACT.ORDER_ID\n"
                        + "    and BUYER_ID >= SELLER_ID and SELLER_ID > TEST_EXTENDED_COLUMN",

        };
        {
            AbstractContext context = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(),
                    new String[] { sqls[0] });
            NDataModel targetModel = context.getModelContexts().get(0).getTargetModel();
            String sql = PushDownUtil.generateFlatTableSql(targetModel, false);
            Assert.assertTrue(sql.contains("ON \"TEST_KYLIN_FACT\".\"ORDER_ID\" = \"TEST_ORDER\".\"ORDER_ID\"\n"
                    + " AND \"TEST_KYLIN_FACT\".\"SELLER_ID\" <= \"TEST_ORDER\".\"BUYER_ID\"\n"
                    + " AND \"TEST_KYLIN_FACT\".\"LEAF_CATEG_ID\" > \"TEST_ORDER\".\"BUYER_ID\""));
        }

        {
            AbstractContext context = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(),
                    new String[] { sqls[1] });
            NDataModel targetModel = context.getModelContexts().get(0).getTargetModel();
            String sql = PushDownUtil.generateFlatTableSql(targetModel, false);
            Assert.assertTrue(sql.contains("ON \"TEST_ORDER\".\"ORDER_ID\" = \"TEST_KYLIN_FACT\".\"ORDER_ID\"\n"
                    + " AND \"TEST_ORDER\".\"BUYER_ID\" >= \"TEST_KYLIN_FACT\".\"SELLER_ID\"\n"
                    + " AND \"TEST_ORDER\".\"BUYER_ID\" < \"TEST_KYLIN_FACT\".\"LEAF_CATEG_ID\""));
        }

        {
            AbstractContext context = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(),
                    new String[] { sqls[2] });
            NDataModel targetModel = context.getModelContexts().get(0).getTargetModel();
            String sql = PushDownUtil.generateFlatTableSql(targetModel, false);
            Assert.assertTrue(sql.contains("ON \"TEST_ORDER\".\"ORDER_ID\" = \"TEST_KYLIN_FACT\".\"ORDER_ID\"\n"
                    + " AND \"TEST_ORDER\".\"BUYER_ID\" >= \"TEST_KYLIN_FACT\".\"SELLER_ID\"\n"
                    + " AND \"TEST_ORDER\".\"TEST_EXTENDED_COLUMN\" < \"TEST_KYLIN_FACT\".\"SELLER_ID\""));
        }
    }

    @Test
    public void testNonEquivJoin() throws Exception {
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.query.print-logical-plan", "TRUE");
        overwriteSystemProp("kylin.model.non-equi-join-recommendation-enabled", "FALSE");

        MetadataTestUtils.toSemiAutoMode(getProject());
        proposeBuildAndCompare(new TestScenario(CompareLevel.SAME, JoinType.LEFT, NON_EQUIV_JOIN_FOLDER), false);
        proposeBuildAndCompare(new TestScenario(CompareLevel.SAME, JoinType.INNER, NON_EQUIV_JOIN_FOLDER), false);
    }

    @Test
    public void testNonEquivJoin2() throws Exception {
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "FALSE");
        overwriteSystemProp("kylin.query.print-logical-plan", "TRUE");
        overwriteSystemProp("kylin.model.non-equi-join-recommendation-enabled", "FALSE");

        MetadataTestUtils.toSemiAutoMode(getProject());
        proposeBuildAndCompare(new TestScenario(CompareLevel.SAME, JoinType.LEFT, NON_EQUIV_JOIN_FOLDER), false);
        proposeBuildAndCompare(new TestScenario(CompareLevel.SAME, JoinType.INNER, NON_EQUIV_JOIN_FOLDER), false);
    }

    private void proposeBuildAndCompare(TestScenario scenario, boolean isNeqJoinRecEnabled) throws Exception {
        List<Pair<String, String>> pairs = fetchQueries(NON_EQUIV_JOIN_FOLDER, scenario.getFromIndex(),
                scenario.getToIndex());
        Set<String> stdPathSet = pairs.stream().filter(p -> {
            String path = p.getFirst();
            return path.substring(path.lastIndexOf('/') + 1).startsWith("std");
        }).map(Pair::getFirst).collect(Collectors.toSet());
        AbstractContext context = proposeWithSmartMaster(getProject(), Lists.newArrayList(scenario));
        List<Pair<String, String>> queries = scenario.getQueries();
        Map<String, String> sqlToPathMap = Maps.newHashMap();
        for (Pair<String, String> query : queries) {
            sqlToPathMap.put(query.getValue(), query.getKey());
        }

        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        context.getAccelerateInfoMap().forEach((sql, info) -> {
            Set<AccelerateInfo.QueryLayoutRelation> relatedLayouts = info.getRelatedLayouts();

            String path = sqlToPathMap.get(sql);
            if (stdPathSet.contains(path) && isNeqJoinRecEnabled) {
                Assert.assertEquals(1, relatedLayouts.size());
                relatedLayouts.forEach(relation -> {
                    NDataModel model = modelManager.getDataModelDesc(relation.getModelId());
                    Assert.assertTrue(SCD2CondChecker.INSTANCE.isScd2Model(model));
                });
            } else {
                relatedLayouts.forEach(relation -> {
                    NDataModel model = modelManager.getDataModelDesc(relation.getModelId());
                    Assert.assertFalse(SCD2CondChecker.INSTANCE.isScd2Model(model));
                });
            }
        });

        buildAndCompare(scenario);
    }

    @Test
    public void testEquiDerivedColumnDisabled() throws Exception {
        MetadataTestUtils.toSemiAutoMode(getProject());

        NDataModel model = proposeSmartModel(EQUIV_JOIN_FOLDER, 0, JoinType.LEFT);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
            modelManager.updateDataModel(model.getId(), copyForWrite -> {
                copyForWrite.getJoinTables().get(1).setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.MANY_TO_MANY);
            });
            return null;
        }, getProject());

        try {
            compareDerivedWithInitialModel(EQUIV_JOIN_FOLDER, 0, JoinType.LEFT);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("No realization found for OlapContext"));
        }
    }

    @Test
    public void testDerivedPkFormFk_LeftJoin() throws Exception {
        MetadataTestUtils.toSemiAutoMode(getProject());
        proposeSmartModel(EQUIV_JOIN_FOLDER, 2, JoinType.LEFT);
        compareDerivedWithInitialModel(EQUIV_JOIN_FOLDER, 2, JoinType.LEFT);
    }

    @Test
    public void testDerivedPkFormFk_InnerJoin() throws Exception {
        MetadataTestUtils.toSemiAutoMode(getProject());
        proposeSmartModel(EQUIV_JOIN_FOLDER, 2, JoinType.INNER);
        compareDerivedWithInitialModel(EQUIV_JOIN_FOLDER, 2, JoinType.INNER);
    }

    @Test
    public void testDerivedFkFromPk_LeftJoin() throws Exception {

        MetadataTestUtils.toSemiAutoMode(getProject());
        proposeSmartModel(EQUIV_JOIN_FOLDER, 4, JoinType.LEFT);

        try {
            compareDerivedWithInitialModel(EQUIV_JOIN_FOLDER, 4, JoinType.LEFT);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("No realization found for OlapContext"));
        }
    }

    @Test
    public void testDerivedFkFromPk_InnerJoin() throws Exception {
        MetadataTestUtils.toSemiAutoMode(getProject());
        proposeSmartModel(EQUIV_JOIN_FOLDER, 4, JoinType.INNER);
        compareDerivedWithInitialModel(EQUIV_JOIN_FOLDER, 4, JoinType.INNER);
    }

    private void compareDerivedWithInitialModel(String testFolder, int startIndex, JoinType joinType) throws Exception {

        TestScenario derivedQueries = new TestScenario(CompareLevel.SAME, testFolder, joinType, startIndex + 1,
                startIndex + 2);
        collectQueries(Lists.newArrayList(derivedQueries));
        buildAndCompare(derivedQueries);
    }

    private NDataModel proposeSmartModel(String testFolder, int startIndex, JoinType joinType) throws IOException {
        AbstractContext context = proposeWithSmartMaster(getProject(), Lists
                .newArrayList(new TestScenario(CompareLevel.NONE, testFolder, joinType, startIndex, startIndex + 1)));

        Assert.assertEquals(1, context.getModelContexts().size());
        return context.getModelContexts().get(0).getTargetModel();
    }
}
