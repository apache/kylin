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

package org.apache.kylin.query;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.engine.QueryRoutingEngine;
import org.apache.kylin.query.engine.data.QueryResult;
import org.apache.kylin.query.relnode.OlapProjectRel;
import org.apache.kylin.query.relnode.OlapTableScan;
import org.apache.kylin.query.util.QueryContextCutter;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.IndexPlanService;
import org.apache.kylin.rest.service.ModelSemanticHelper;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ModelSmartService;
import org.apache.kylin.rest.service.NUserGroupService;
import org.apache.kylin.rest.service.OptRecService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.ProjectSmartService;
import org.apache.kylin.rest.service.QueryHistoryAccelerateScheduler;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.service.RawRecService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.util.SemiAutoTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

public class QueryGroupingSetsTest extends SemiAutoTestBase {

    private RawRecService rawRecService;
    private RDBMSQueryHistoryDAO queryHistoryDAO;
    private ProjectService projectService;
    private ProjectSmartService projectSmartService;

    @Mock
    private QueryService queryService;
    @Mock
    OptRecService optRecService = Mockito.spy(new OptRecService());
    @Mock
    ModelService modelService = Mockito.spy(ModelService.class);
    @Mock
    ModelSmartService modelSmartService = Mockito.spy(ModelSmartService.class);
    @Mock
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());
    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);
    @Mock
    private final IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);
    @InjectMocks
    private final ModelSemanticHelper semanticService = Mockito.spy(new ModelSemanticHelper());

    @InjectMocks
    private final QueryRoutingEngine queryRoutingEngine = Mockito.spy(QueryRoutingEngine.class);

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        rawRecService = new RawRecService();
        projectService = new ProjectService();
        projectSmartService = new ProjectSmartService();
        modelService.setSemanticUpdater(semanticService);
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        queryService = Mockito.spy(new QueryService());
        prepareACL();
        QueryHistoryAccelerateScheduler queryHistoryTaskScheduler = QueryHistoryAccelerateScheduler.getInstance();
        ReflectionTestUtils.setField(queryHistoryTaskScheduler, "querySmartSupporter", rawRecService);
        ReflectionTestUtils.setField(queryHistoryTaskScheduler, "userGroupService", userGroupService);
        queryHistoryTaskScheduler.init();
    }

    @After
    public void teardown() throws Exception {
        queryHistoryDAO.deleteAllQueryHistory();
        super.tearDown();
        QueryHistoryAccelerateScheduler.shutdown();
    }

    private void prepareACL() {
        ReflectionTestUtils.setField(queryService, "queryRoutingEngine", queryRoutingEngine);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(optRecService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(optRecService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "indexPlanService", indexPlanService);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "modelChangeSupporters", Collections.singletonList(rawRecService));
        ReflectionTestUtils.setField(modelSmartService, "optRecService", optRecService);
        ReflectionTestUtils.setField(modelSmartService, "modelService", modelService);
        ReflectionTestUtils.setField(modelSmartService, "indexPlanService", indexPlanService);
        ReflectionTestUtils.setField(modelSmartService, "rawRecService", rawRecService);
        ReflectionTestUtils.setField(modelSmartService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(rawRecService, "optRecService", optRecService);
        ReflectionTestUtils.setField(rawRecService, "projectSmartService", projectSmartService);

        ReflectionTestUtils.setField(projectSmartService, "projectSmartSupporter", rawRecService);
        ReflectionTestUtils.setField(projectSmartService, "aclEvaluate", aclEvaluate);
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @Test
    public void testQueryMultipleGroupingSets() throws Throwable {
        overwriteSystemProp("kylin.query.pushdown-enabled", "false");
        overwriteSystemProp("kylin.query.engine.split-group-sets-into-union", "true");
        KylinConfig kylinConfig = getTestConfig();
        String[] sqls = new String[] {
                "select LSTG_FORMAT_NAME,LSTG_SITE_ID from test_kylin_fact group by LSTG_FORMAT_NAME,LSTG_SITE_ID" };
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);
        buildAllModels(kylinConfig, getProject());

        String sql = "select GROUPING(gr1) as group1, GROUPING(gr2) as group2, count(*) as goal_group\n" + "from\n"
                + "  (select LSTG_FORMAT_NAME gr1,LSTG_SITE_ID gr2 from TEST_KYLIN_FACT)\n"
                + "group by GROUPING SETS((gr1), (gr2))\n" + "order by 1,2";

        QueryExec queryExec = new QueryExec(getProject(), kylinConfig);
        QueryResult queryResult = queryExec.executeQuery(sql);
        Assert.assertEquals(3, queryResult.getColumns().size());
        Assert.assertEquals(13, queryResult.getSize());
    }

    @Test
    public void testDuplicateGroupingSets() throws InterruptedException, SQLException {
        overwriteSystemProp("kylin.query.pushdown-enabled", "false");
        overwriteSystemProp("kylin.query.engine.split-group-sets-into-union", "true");
        KylinConfig kylinConfig = getTestConfig();
        String[] sqls = new String[] {
                "select LSTG_FORMAT_NAME,LSTG_SITE_ID from test_kylin_fact group by LSTG_FORMAT_NAME,LSTG_SITE_ID" };
        AccelerationUtil.runWithSmartContext(kylinConfig, getProject(), sqls, true);
        buildAllModels(kylinConfig, getProject());

        String sql = "select gr1, gr2, count(*) as goal_group\n" + "from\n"
                + "(select LSTG_FORMAT_NAME gr1, LSTG_SITE_ID gr2 from TEST_KYLIN_FACT)\n"
                + "group by gr1, gr2, GROUPING SETS((gr1), (gr2), (gr1, gr2), ())";

        QueryExec queryExec = new QueryExec(getProject(), kylinConfig);
        QueryResult queryResult = queryExec.executeQuery(sql);

        String expectedSql = "select gr1, gr2, count(*) as goal_group\n" + "from\n"
                + "(select LSTG_FORMAT_NAME gr1, LSTG_SITE_ID gr2 from TEST_KYLIN_FACT)\n"
                + "group by GROUPING SETS((gr1, gr2), (gr1, gr2), (gr1, gr2), (gr1, gr2))";
        QueryResult expectedResult = queryExec.executeQuery(expectedSql);

        Assert.assertEquals(3, queryResult.getColumns().size());
        Assert.assertEquals(160, queryResult.getSize());
        Assert.assertEquals(queryResult.getSize(), expectedResult.getSize());
        Assert.assertNotNull(queryResult.getRowsIterable());
        Assert.assertNotNull(queryResult.getRowsIterable());

        Iterator<List<String>> resultIter = queryResult.getRowsIterable().iterator();
        Iterator<List<String>> expectedResultIter = expectedResult.getRowsIterable().iterator();
        while (resultIter.hasNext()) {
            Assert.assertTrue(expectedResultIter.hasNext());
            Assert.assertEquals(resultIter.next(), expectedResultIter.next());
        }

        String uniqueSql = "select gr1, gr2, count(*) as goal_group\n" + "from\n"
                + "(select LSTG_FORMAT_NAME gr1, LSTG_SITE_ID gr2 from TEST_KYLIN_FACT)\n"
                + "group by GROUPING SETS((gr1, gr2))";
        QueryResult uniqueResult = queryExec.executeQuery(uniqueSql);
        Assert.assertEquals(uniqueResult.getSize() * 4, expectedResult.getSize());
    }

    @Test
    public void testRecomputeGroupingSqlDigest() throws Exception {
        String project = "default";
        String sql = "select GROUPING(gr1) as group1, GROUPING(gr2) as group2, count(*) as goal_group\n" + "from\n"
                + "  (select CAL_DT gr1,LSTG_FORMAT_NAME gr2 from TEST_KYLIN_FACT)\n"
                + "group by GROUPING SETS((gr1), (gr2))\n" + "order by 1,2";
        overwriteSystemProp("kylin.query.pushdown-enabled", "false");
        overwriteSystemProp("kylin.query.engine.split-group-sets-into-union", "true");

        QueryExec queryExec = new QueryExec(project, KylinConfig.getInstanceFromEnv());
        RelNode root = queryExec.parseAndOptimize(sql);
        QueryContextCutter.selectRealization(project, root, QueryContext.current().isForModeling());

        List<OlapProjectRel> projectRels = findProjectNodes(root);
        for (OlapProjectRel projectNode : projectRels) {
            String digest = projectNode.getDigest();
            Assert.assertTrue(digest.contains("groupByColumns"));
        }
    }

    @Test
    public void testRecomputeSqlDigestMultipleGroupBys() throws Exception {
        String project = "default";
        String sql = "SELECT CAL_DT, LSTG_FORMAT_NAME\n" + "FROM TEST_KYLIN_FACT\n"
                + "GROUP BY GROUPING SETS ((CAL_DT, LSTG_FORMAT_NAME),(CAL_DT),(LSTG_FORMAT_NAME),())\n"
                + "ORDER BY CAL_DT,LSTG_FORMAT_NAME";
        overwriteSystemProp("kylin.query.pushdown-enabled", "false");
        overwriteSystemProp("kylin.query.engine.split-group-sets-into-union", "true");

        QueryExec queryExec = new QueryExec(project, KylinConfig.getInstanceFromEnv());
        RelNode root = queryExec.parseAndOptimize(sql);
        QueryContextCutter.selectRealization(project, root, QueryContext.current().isForModeling());

        List<OlapProjectRel> projectRels = findProjectNodes(root);
        String digest = projectRels.get(0).getDigest();
        Assert.assertTrue(digest.contains("groupByColumns=[2, 3]"));
    }

    private static List<OlapProjectRel> findProjectNodes(RelNode node) {
        List<OlapProjectRel> projectRels = new ArrayList<>();
        findProjectNodesHelper(node, projectRels);
        return projectRels;
    }

    private static void findProjectNodesHelper(RelNode node, List<OlapProjectRel> projectRels) {
        if (node == null) {
            return;
        }
        if (node instanceof OlapTableScan) {
            OlapTableScan tableScan = (OlapTableScan) node;
            tableScan.getContext().genExecFunc(tableScan);
        }
        if (node instanceof OlapProjectRel && ((OlapProjectRel) node).getContext() != null) {
            projectRels.add((OlapProjectRel) node);
        }
        if (!node.getInputs().isEmpty()) {
            for (RelNode child : node.getInputs()) {
                findProjectNodesHelper(child, projectRels);
            }
        }
    }
}
