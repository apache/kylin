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

import java.util.List;

import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.engine.spark.source.NSparkMetadataExplorer;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.common.SparkDDLTestUtils;
import org.apache.spark.sql.internal.SQLConf;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

public class SparkDDLTest extends NLocalFileMetadataTestCase {
  @Autowired
  private final SparkDDLService ddlService = Mockito.spy(new SparkDDLService());
  @Autowired
  private final IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

  private static final String CREATEVIEW_SQL1 =
      "CREATE VIEW `ssb`.`ke_order_view` as select LO_ORDERKEY, C_NAME from SSB.p_lineorder t1 left join "
          + "SSB. CUSTOMER t2 on t1. LO_CUSTKEY = t2. C_CUSTKEY";
  private static final String CREATEVIEW_SQL2 = "CREATE VIEW `ssb`.`order_view2` as select * from SSB.P_LINEORDER";
  private static final String CREATEVIEW_SQL3 = "CREATE VIEW `ssb`.`order_view2` as abc";
  private static final String CREATEVIEW_SQL4 = "CREATE VIEW `ssb`.`order_view2` as select * from SSB.unload_table";
  private static final String CREATEVIEW_SQL5 = "CREATE VIEW `ke_order_view2` as select * from SSB.P_LINEORDER";
  private static final String CREATEVIEW_SQL6 = "abc";
  private static final String ALTERVIEW_SQL =
      "alter view `ssb`.`ke_order_view` as select lo_orderkey from SSB.P_LINEORDER";
  private static final String DROPVIEW_SQL1 = "drop view `ssb`.`ke_order_view`";
  private static final String DROPVIEW_SQL2 = "drop table `ssb`.`ke_table1`";
  private static final String DROPVIEW_SQL3 = "drop table `ssb`.`ke_order_view`";
  private static final String DROPVIEW_SQL4 = "drop table `ke_table2`";
  private static final String SHOWVIEW_SQL = "show create table ssb.ke_order_view";

  @AfterClass
  public static void tearDownResource() {
    staticCleanupTestMetadata();
  }

  @Before
  public void setup() {
    createTestMetadata();
    Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
    SecurityContextHolder.getContext().setAuthentication(authentication);
    ReflectionTestUtils.setField(ddlService, "userGroupService", userGroupService);
  }

  @After
  public void cleanup() {
    cleanupTestMetadata();
  }

  @Test
  public void testDDL() throws Exception {
    try {
      assertKylinExeption(
          () ->
              ddlService.executeDDLSql("ssb", CREATEVIEW_SQL5),
          "DDL function has not been turned on.");

      getTestConfig().setProperty("kylin.source.ddl.enabled", "true");
      NTableMetadataManager tableManager = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
      SparkDDLTestUtils.prepare();
      ddlService.executeDDLSql("ssb", CREATEVIEW_SQL5);
      assertKylinExeption(
          () ->
              ddlService.executeDDLSql("ssb", CREATEVIEW_SQL2),
          MsgPicker.getMsg().getDDLViewNameError());
      assertKylinExeption(() ->
          ddlService.executeDDLSql("ssb", CREATEVIEW_SQL3), "");
      assertKylinExeption(() ->
          ddlService.executeDDLSql("ssb", CREATEVIEW_SQL6), "");
      assertKylinExeption(
          () ->
              ddlService.executeDDLSql("ssb", CREATEVIEW_SQL4),
          MsgPicker.getMsg().getDDLTableNotLoad("SSB.unload_table"));
      assertKylinExeption(
          () ->
              ddlService.executeDDLSql("ssb", DROPVIEW_SQL2),
          MsgPicker.getMsg().getDDLDropError());
      assertKylinExeption(
          () ->
              ddlService.executeDDLSql("ssb", DROPVIEW_SQL3), "");

      ddlService.executeDDLSql("ssb", CREATEVIEW_SQL1);
      ddlService.executeDDLSql("ssb", ALTERVIEW_SQL);
      String createViewSQL = ddlService.executeDDLSql("ssb", SHOWVIEW_SQL);
      Assert.assertTrue(createViewSQL.contains("ke_order_view"));
      ddlService.executeDDLSql("ssb", DROPVIEW_SQL1);

      Authentication authentication = new TestingAuthenticationToken("USER1",
          "", Constant.GROUP_ALL_USERS);
      SecurityContextHolder.getContext().setAuthentication(authentication);
      assertKylinExeption(
          () ->
              ddlService.executeDDLSql("ssb", CREATEVIEW_SQL1),
          MsgPicker.getMsg().getDDLPermissionDenied());

      // ddl description
      List<List<String>> description = ddlService.pluginsDescription("ssb");
      Assert.assertTrue(description.size() > 0);


      // read/write cluster
      SparderEnv.getSparkSession().sessionState().conf()
          .setConf(SQLConf.HIVE_SPECIFIC_FS_LOCATION(), "hdfs://read");
      new NSparkMetadataExplorer().checkDatabaseHadoopAccessFast("SSB");
    } finally {
      SparkSession spark = SparderEnv.getSparkSession();
      if (spark != null && !spark.sparkContext().isStopped()) {
        spark.stop();
      }
    }
  }
}
