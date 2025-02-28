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
package org.apache.kylin.rest.service;

import static org.apache.spark.ddl.DDLConstant.HIVE_VIEW;
import static org.apache.spark.ddl.DDLConstant.LOGICAL_VIEW;
import static org.apache.spark.ddl.DDLConstant.REPLACE_LOGICAL_VIEW;
import static org.apache.spark.sql.LogicalViewLoader.LOADED_LOGICAL_VIEWS;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.view.LogicalView;
import org.apache.kylin.metadata.view.LogicalViewManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.ViewRequest;
import org.apache.kylin.rest.response.LoadTableResponse;
import org.apache.kylin.rest.response.LogicalViewResponse;
import org.apache.spark.sql.LogicalViewLoader;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.common.SparkDDLTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

public class SparkDDLTest extends NLocalFileMetadataTestCase {
  private SparkDDLService ddlService;
  private TableExtService tableExtService;
  private TableService tableService;
  private IUserGroupService userGroupService;
  private Integer LOGICAL_VIEW_CATCHUP_INTERVAL = 3;

  // Hive View
  private static final String CREATEVIEW_SQL1 =
      "CREATE VIEW `ssb`.`kylin_order_view` as select LO_ORDERKEY, C_NAME from SSB.p_lineorder t1 left join "
          + "SSB. CUSTOMER t2 on t1. LO_CUSTKEY = t2. C_CUSTKEY";
  private static final String CREATEVIEW_SQL2 = "CREATE VIEW `ssb`.`order_view2` as select * from SSB.P_LINEORDER";
  private static final String CREATEVIEW_SQL3 = "CREATE VIEW `ssb`.`order_view2` as abc";
  private static final String CREATEVIEW_SQL4 = "CREATE VIEW `ssb`.`kylin_order_view2` as select * from SSB.unload_table";
  private static final String CREATEVIEW_SQL5 = "CREATE VIEW `kylin_order_view2` as select * from SSB.P_LINEORDER";
  private static final String CREATEVIEW_SQL6 = "abc";
  private static final String CREATEVIEW_SQL7 = "CREATE VIEW `ssb`.`kylin_order_view3` as select * from SSB.P_LINEORDER";
  private static final String ALTERVIEW_SQL =
      "alter view `ssb`.`kylin_order_view` as select lo_orderkey from SSB.P_LINEORDER";
  private static final String DROPVIEW_SQL1 = "drop view `ssb`.`kylin_order_view`";
  private static final String DROPVIEW_SQL2 = "drop table `ssb`.`kylin_table1`";
  private static final String DROPVIEW_SQL3 = "drop table `ssb`.`kylin_order_view`";
  private static final String DROPVIEW_SQL4 = "drop table `kylin_table2`";

  private static final String SHOWVIEW_SQL = "show create table ssb.kylin_order_view";

  // Logical View
  private static final String CREATE_LOGICAL_VIEW_SQL1 = "CREATE LOGICAL VIEW  "
      + "logical_view_table1  AS select * from SSB.P_LINEORDER";
  private static final String CREATE_LOGICAL_VIEW_SQL2 = "CREATE LOGICAL VIEW  "
      + "logical_view_table2  AS select * from SSB.P_LINEORDER";
  private static final String CREATE_LOGICAL_VIEW_SQL3 = "CREATE LOGICAL VIEW  "
      + "logical_view_table3  AS select * from SSB.P_LINEORDER";
  private static final String CREATE_LOGICAL_VIEW_SQL4 = "CREATE LOGICAL VIEW  "
      + "logical_view_table4  AS select * from SSB.P_LINEORDER";
  private static final String CREATE_LOGICAL_VIEW_SQL5 = "CREATE LOGICAL VIEW  "
      + "logical_view_table5  AS select * from SSB.P_LINEORDER";
  private static final String REPLACE_LOGICAL_VIEW_SQL1 = "REPLACE LOGICAL VIEW  "
      + "logical_view_no_exist  AS select * from SSB.Customer";
  private static final String REPLACE_LOGICAL_VIEW_SQL2 = "REPLACE LOGICAL VIEW  "
      + "logical_view_table2  AS select * from SSB.Customer";
  private static final String DROP_LOGICAL_VIEW_SQL1 = "drop LOGICAL VIEW KYLIN_LOGICAL_VIEW.logical_view_table1";
  private static final String DROP_LOGICAL_VIEW_SQL2 = "drop LOGICAL VIEW KYLIN_LOGICAL_VIEW.logical_view_table3";
  private static final String SELECT_LOGICAL_VIEW_SQL = "select * from KYLIN_LOGICAL_VIEW.logical_view_table3";

  // DDL Config
  private static final String DDL_HIVE_CONFIG = "kylin.source.ddl.hive.enabled";
  private static final String DDL_LOGICAL_VIEW_CONFIG = "kylin.source.ddl.logical-view.enabled";

  @AfterClass
  public static void tearDownResource() {
    staticCleanupTestMetadata();
  }

  @Before
  public void setup() {
    createTestMetadata();

    ddlService = Mockito.spy(new SparkDDLService());
    tableExtService = Mockito.spy(new TableExtService());
    tableService = Mockito.spy(new TableService());
    userGroupService = Mockito.spy(NUserGroupService.class);

    Authentication authentication = new TestingAuthenticationToken("ADMIN",
        "ADMIN", Constant.ROLE_ADMIN);
    SecurityContextHolder.getContext().setAuthentication(authentication);
    ReflectionTestUtils.setField(ddlService, "userGroupService", userGroupService);
    ReflectionTestUtils.setField(tableExtService, "tableService", tableService);
    getTestConfig().setProperty(DDL_LOGICAL_VIEW_CONFIG, "true");
  }

  @After
  public void cleanup() {
    cleanupTestMetadata();
  }

  @Test
  public void testDDL() throws Exception {
    try {
      SparkDDLTestUtils.prepare();
      assertKylinExeption(
          () ->
              ddlService.executeSQL(new ViewRequest("ssb", CREATEVIEW_SQL1, HIVE_VIEW)),
          "Hive operation is not supported, please turn on config.");
      getTestConfig().setProperty(DDL_HIVE_CONFIG, "true");

      getTestConfig().setProperty(DDL_LOGICAL_VIEW_CONFIG, "false");
      assertKylinExeption(
          () ->
              ddlService.executeSQL(new ViewRequest("ssb", CREATE_LOGICAL_VIEW_SQL1, LOGICAL_VIEW)),
          "Logical View operation is not supported, please turn on config.");
      getTestConfig().setProperty(DDL_LOGICAL_VIEW_CONFIG, "true");
      getTestConfig().setProperty(
          "kylin.source.ddl.logical-view-catchup-interval", LOGICAL_VIEW_CATCHUP_INTERVAL.toString());

      testHiveDDL();

      testLogicalView();

      // User authentication
      Authentication authentication = new TestingAuthenticationToken("USER1",
          "", Constant.GROUP_ALL_USERS);
      SecurityContextHolder.getContext().setAuthentication(authentication);
      assertKylinExeption(
          () ->
              ddlService.executeSQL(new ViewRequest("ssb", CREATEVIEW_SQL1)),
          "");
    } finally {
      LogicalViewLoader.stopScheduler();
      LOADED_LOGICAL_VIEWS.clear();
      SparkSession spark = SparderEnv.getSparkSession();
      if (spark != null && !spark.sparkContext().isStopped()) {
        spark.stop();
      }
    }
  }

  private void testHiveDDL() throws Exception {
    // Hive View DDL
    ddlService.executeSQL(new ViewRequest("ssb", CREATEVIEW_SQL5, HIVE_VIEW));
    assertKylinExeption(
        () ->
            ddlService.executeSQL(new ViewRequest("ssb", CREATEVIEW_SQL2, HIVE_VIEW)),
        MsgPicker.getMsg().getDDLViewNameError());
    assertKylinExeption(() ->
        ddlService.executeSQL(new ViewRequest("ssb", CREATEVIEW_SQL3)), "");
    assertKylinExeption(() ->
        ddlService.executeSQL(new ViewRequest("ssb", CREATEVIEW_SQL6)), "");
    assertKylinExeption(
        () ->
            ddlService.executeSQL(new ViewRequest("ssb", CREATEVIEW_SQL4)),
        MsgPicker.getMsg().getDDLTableNotLoad("SSB.unload_table"));
    assertKylinExeption(
        () ->
            ddlService.executeSQL(new ViewRequest("ssb", DROPVIEW_SQL2)),
        MsgPicker.getMsg().getDDLDropError());
    assertKylinExeption(
        () ->
            ddlService.executeSQL(new ViewRequest("ssb", DROPVIEW_SQL3)), "");
    ddlService.executeSQL(new ViewRequest("ssb", CREATEVIEW_SQL1, HIVE_VIEW));
    ddlService.executeSQL(new ViewRequest("ssb", ALTERVIEW_SQL, HIVE_VIEW));
    String createViewSQL = ddlService.executeSQL(new ViewRequest("ssb", SHOWVIEW_SQL, HIVE_VIEW));
    Assert.assertTrue(createViewSQL.contains("kylin_order_view"));
    ddlService.executeSQL(new ViewRequest("ssb", DROPVIEW_SQL1, HIVE_VIEW));
  }

  private void testLogicalView() throws Exception {
    SparkSession spark = SparderEnv.getSparkSession();
    // Logical View DDL
    assertRuntimeException(() -> spark.sql(SELECT_LOGICAL_VIEW_SQL), "");
    ddlService.executeSQL(new ViewRequest("ssb", CREATE_LOGICAL_VIEW_SQL1, LOGICAL_VIEW));
    ddlService.executeSQL(new ViewRequest("ssb", CREATE_LOGICAL_VIEW_SQL2, LOGICAL_VIEW));
    ddlService.executeSQL(new ViewRequest("demo", CREATE_LOGICAL_VIEW_SQL4, LOGICAL_VIEW));
    ddlService.executeSQL(new ViewRequest("demo", CREATE_LOGICAL_VIEW_SQL5, LOGICAL_VIEW));
    assertKylinExeption(
        () ->
            ddlService.executeSQL(new ViewRequest("ssb", CREATE_LOGICAL_VIEW_SQL2, LOGICAL_VIEW)),
        MsgPicker.getMsg().getDDLViewNameDuplicateError());
    ddlService.executeSQL(new ViewRequest("ssb", DROP_LOGICAL_VIEW_SQL1, LOGICAL_VIEW));
    NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
    TableDesc tableDesc = tableMgr.getTableDesc("SSB.P_LINEORDER");
    String s = JsonUtil.writeValueAsIndentString(tableDesc);
    TableDesc newTable = JsonUtil.readValue(s, TableDesc.class);
    newTable.setName("KYLIN_LOGICAL_VIEW.logical_view_table3");
    newTable.setMvcc(-1);
    tableMgr.saveSourceTable(newTable);
    assertKylinExeption(
        () ->
            ddlService.executeSQL(new ViewRequest("ssb", DROP_LOGICAL_VIEW_SQL2, LOGICAL_VIEW)), "");
    assertKylinExeption(
        () ->
            ddlService.executeSQL(new ViewRequest("ssb", REPLACE_LOGICAL_VIEW_SQL1)),
        "View name is not found.");
    ddlService.executeSQL(new ViewRequest("ssb", REPLACE_LOGICAL_VIEW_SQL2));
    assertKylinExeption(
        () ->
            ddlService.executeSQL(new ViewRequest("demo", REPLACE_LOGICAL_VIEW_SQL2)),
        "View can only modified in Project");

    // Request Restrict
    assertKylinExeption(
        () ->
            ddlService.executeSQL(new ViewRequest("ssb", CREATE_LOGICAL_VIEW_SQL3, HIVE_VIEW)),
        "Only support");
    assertKylinExeption(
        () ->
            ddlService.executeSQL(new ViewRequest("ssb", CREATEVIEW_SQL7, LOGICAL_VIEW)),
        "Only support");
    assertKylinExeption(
        () ->
            ddlService.executeSQL(new ViewRequest("ssb", CREATE_LOGICAL_VIEW_SQL3, REPLACE_LOGICAL_VIEW)),
        "Only support");

    // Logical View Loader
    LogicalViewLoader.initScheduler();
    LogicalViewManager manager = LogicalViewManager.getInstance(KylinConfig.getInstanceFromEnv());
    LogicalView logicalView = new LogicalView("logical_view_table3", CREATE_LOGICAL_VIEW_SQL3,
        "ADMIN", "SSB");
    manager.update(logicalView);
    await().atMost(LOGICAL_VIEW_CATCHUP_INTERVAL * 10, TimeUnit.SECONDS).until(() -> {
      try {
        if (!LOADED_LOGICAL_VIEWS.containsKey("LOGICAL_VIEW_TABLE5")) {
          return false;
        }
        spark.sql(SELECT_LOGICAL_VIEW_SQL);
      } catch (Exception e) {
        return false;
      }
      return true;
    });
    manager.delete("logical_view_table5");
    await().atMost(LOGICAL_VIEW_CATCHUP_INTERVAL * 5, TimeUnit.SECONDS).until(() -> {
      if (LOADED_LOGICAL_VIEWS.containsKey("LOGICAL_VIEW_TABLE5")) {
        return false;
      }
      return true;
    });
    // DDL description
    List<List<String>> description = ddlService.pluginsDescription("ssb", "hive");
    Assert.assertEquals(4, description.get(0).size());

    description = ddlService.pluginsDescription("ssb", "logic");
    Assert.assertEquals(4, description.get(0).size());

    // view list in project
    List<LogicalViewResponse> logicalViewsInProject = ddlService.listAll("ssb", "");
    List<LogicalViewResponse> logicalViewsInProject2 = ddlService.listAll("ssb", "table2");
    List<LogicalViewResponse> logicalViewsInProject3 = ddlService.listAll("demo", "");
    Assert.assertEquals(3, logicalViewsInProject.size());
    Assert.assertEquals(1, logicalViewsInProject2.size());
    LogicalViewResponse confidentialTable =
        logicalViewsInProject.stream().filter(table -> table.getCreatedProject().equals("demo")).collect(
            Collectors.toList()).get(0);
    LogicalViewResponse noConfidentialTable =
        logicalViewsInProject.stream().filter(table -> table.getCreatedProject().equals("ssb")).collect(
            Collectors.toList()).get(0);
    LogicalViewResponse noConfidentialTable2 =
        logicalViewsInProject3.stream().filter(table -> table.getCreatedProject().equals("demo")).collect(
            Collectors.toList()).get(0);
    Assert.assertEquals("***", confidentialTable.getCreatedSql());
    Assert.assertNotEquals("***", noConfidentialTable.getCreatedSql());
    Assert.assertNotEquals("***", noConfidentialTable2.getCreatedSql());

    // load table list
    String[] failedLoadTables = {"KYLIN_LOGICAL_VIEW.logical_view_table2",
                                 "KYLIN_LOGICAL_VIEW.logical_view_table3",
                                 "KYLIN_LOGICAL_VIEW.logical_view_table4"};
    String[] successLoadTables = {"KYLIN_LOGICAL_VIEW.logical_view_table2",
                                  "KYLIN_LOGICAL_VIEW.logical_view_table3"};
    List<Pair<TableDesc, TableExtDesc>> canLoadTables = Lists.newArrayList();
    LoadTableResponse tableResponse = new LoadTableResponse();
    tableExtService.filterAccessTables(successLoadTables, canLoadTables, tableResponse, "ssb");
    Assert.assertEquals(2, canLoadTables.size());
    canLoadTables.clear();
    tableExtService.filterAccessTables(failedLoadTables, canLoadTables, tableResponse, "ssb");
    Assert.assertEquals(2, canLoadTables.size());
  }

  @Test
  public void testAddCatalogConfByJdbc() throws Exception {
    SparkDDLTestUtils.prepare();
    SparkSession session = SparderEnv.getSparkSession();
    String database = session.catalog().currentDatabase();
    LogicalViewLoader.addCatalogConfByJdbc(session, "ssb");
    Assert.assertEquals(database, session.catalog().currentDatabase());
  }
}
