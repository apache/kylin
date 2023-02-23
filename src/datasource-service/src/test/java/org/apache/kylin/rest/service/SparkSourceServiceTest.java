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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.test.TestingServer;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.request.DDLRequest;
import org.apache.kylin.rest.response.DDLResponse;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DDLDesc;
import org.apache.spark.sql.DdlOperation;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import com.google.common.collect.Maps;

public class SparkSourceServiceTest extends NLocalFileMetadataTestCase {

    protected static SparkSession ss;
    private NProjectManager projectManager;
    @InjectMocks
    private final SparkSourceService sparkSourceService = Mockito.spy(new SparkSourceService());
    private TestingServer zkTestServer;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        ss = SparkSession.builder().appName("local").master("local[1]").enableHiveSupport().getOrCreate();
        ss.sparkContext().hadoopConfiguration().set("javax.jdo.option.ConnectionURL",
                "jdbc:derby:memory:db;create=true");
        SparderEnv.setSparkSession(ss);
        createTestMetadata();
        projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject("default");
        LinkedHashMap<String, String> overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.source.default", "9");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());
        DDLRequest ddlRequest = new DDLRequest();
        ddlRequest.setSql("use default;create external table COUNTRY(COUNTRY string,LATITUDE double,"
                + "LONGITUDE double,NAME string) row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' "
                + "with serdeproperties(\"separatorChar\" = \",\", \"quoteChar\" = \"\\\"\") location "
                + "'../examples/test_case_data/localmeta/data'");
        sparkSourceService.executeSQL(ddlRequest);
        zkTestServer = new TestingServer(true);
        overwriteSystemProp("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        overwriteSystemProp("kap.env.zookeeper-max-retries", "1");
        overwriteSystemProp("kap.env.zookeeper-base-sleep-time", "1000");
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        ss.stop();
    }

    @Test
    public void testExecuteSQL() {
        {
            String sql = "create external table default.SALES(name string, district string)"
                    + " partitioned by (year int) row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'"
                    + " location '../examples/test_case_data/localmeta/data'";
            DDLDesc ddlDesc = new DDLDesc(sql, "default", "SALES", DDLDesc.DDLType.CREATE_TABLE);
            Assert.assertEquals(ddlDesc, sparkSourceService.executeSQL(sql));
        }
        {
            String sql = "alter table default.SALES add partition(year=2020)";
            DDLDesc ddlDesc = new DDLDesc(sql, "default", "SALES", DDLDesc.DDLType.ADD_PARTITION);
            Assert.assertEquals(ddlDesc, sparkSourceService.executeSQL(sql));
        }
        {
            String sql = "drop table default.SALES";
            DDLDesc ddlDesc = new DDLDesc(sql, "default", "SALES", DDLDesc.DDLType.DROP_TABLE);
            Assert.assertEquals(ddlDesc, sparkSourceService.executeSQL(sql));
        }
        {
            String sql = "create database sales_db";
            DDLDesc ddlDesc = new DDLDesc(sql, "sales_db", null, DDLDesc.DDLType.CREATE_DATABASE);
            Assert.assertEquals(ddlDesc, sparkSourceService.executeSQL(sql));
        }
        {
            String sql = "drop database sales_db";
            DDLDesc ddlDesc = new DDLDesc(sql, "sales_db", null, DDLDesc.DDLType.DROP_DATABASE);
            Assert.assertEquals(ddlDesc, sparkSourceService.executeSQL(sql));
        }
        {
            String sql = "create view COUNTRY_VIEW as select * from COUNTRY";
            DDLDesc ddlDesc = new DDLDesc(sql, "default", "COUNTRY_VIEW", DDLDesc.DDLType.CREATE_VIEW);
            Assert.assertEquals(ddlDesc, sparkSourceService.executeSQL(sql));
        }
        {
            String sql = "show databases";
            DDLDesc ddlDesc = new DDLDesc(sql, null, null, DDLDesc.DDLType.NONE);
            Assert.assertEquals(ddlDesc, sparkSourceService.executeSQL(sql));
        }
    }

    @Test
    public void testExecuteSQL2() {
        DDLRequest ddlRequest = new DDLRequest();
        ddlRequest.setSql("show databases;");
        DDLResponse ddlResponse = new DDLResponse();
        Map<String, DDLDesc> succeed = Maps.newHashMap();
        Map<String, String> failed = Maps.newHashMap();
        DDLDesc ddlDesc = new DDLDesc("show databases", null, null, DDLDesc.DDLType.NONE);
        succeed.put("show databases", ddlDesc);
        ddlResponse.setSucceed(succeed);
        ddlResponse.setFailed(failed);
        Assert.assertEquals(ddlResponse, sparkSourceService.executeSQL(ddlRequest));
    }

    @Test
    public void testGetTableDesc() {
        try {
            sparkSourceService.getTableDesc("default", "COUNTRY");
            DDLRequest ddlRequest = new DDLRequest();
            ddlRequest.setSql("use default;create table COUNTRY_PARQUET like COUNTRY using parquet;");
            sparkSourceService.executeSQL(ddlRequest);
            sparkSourceService.getTableDesc("default", "COUNTRY_PARQUET");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testMsck() {
        DDLRequest ddlRequest = new DDLRequest();
        ddlRequest.setSql("use default;create external table default.SALES(name string, district string)"
                + " partitioned by (year int) row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'"
                + " location '../examples/test_case_data/localmeta/data';"
                + " alter table SALES add partition(year=2020);");
        sparkSourceService.executeSQL(ddlRequest);
        List diffList = sparkSourceService.msck("default", "SALES");
        Assert.assertTrue(diffList.isEmpty());
    }

    @Test
    public void testListDatabase() {
        List<String> result = new ArrayList<>();
        result.add("DEFAULT");
        Assert.assertEquals(result, sparkSourceService.listDatabase());

    }

    @Test
    public void testListTables() throws Exception {
        Assert.assertEquals(11, sparkSourceService.listTables("DEFAULT", "default").size());
    }

    @Test
    public void testDatabaseExists() {
        Assert.assertTrue(sparkSourceService.databaseExists("default"));
    }

    @Test
    public void testDropTable() throws AnalysisException {
        sparkSourceService.dropTable("default", "COUNTRY");
        Assert.assertFalse(sparkSourceService.tableExists("default", "COUNTRY"));
    }

    @Test
    public void testListColumns() {
        Assert.assertEquals(4, sparkSourceService.listColumns("default", "COUNTRY").size());

    }

    @Test
    public void testExportTables() {
        // hive data source
        String expectedTableStructure = "CREATE EXTERNAL TABLE `default`.`hive_bigints`(   `id` BIGINT) "
                + "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' "
                + "WITH SERDEPROPERTIES (   'serialization.format' = '1') STORED AS   "
                + "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'   "
                + "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' "
                + "LOCATION 'file:/tmp/parquet_data' ";
        sparkSourceService.executeSQL(
                "CREATE EXTERNAL TABLE hive_bigints(id bigint)  STORED AS PARQUET LOCATION '/tmp/parquet_data'");
        String actureTableStructure = sparkSourceService.exportTables("default", new String[] { "hive_bigints" })
                .getTables().get("hive_bigints");
        Assert.assertEquals(actureTableStructure.substring(0, actureTableStructure.lastIndexOf("TBLPROPERTIES")),
                expectedTableStructure);
        Assert.assertTrue(DdlOperation.isHiveTable("default", "hive_bigints"));

        // spark datasource
        sparkSourceService.executeSQL(
                "CREATE EXTERNAL TABLE spark_bigints(id bigint) USING PARQUET LOCATION '/tmp/parquet_data_spark'");
        Assert.assertFalse(DdlOperation.isHiveTable("default", "spark_bigints"));
        String sparkDDL = sparkSourceService.exportTables("default", new String[] { "spark_bigints" }).getTables()
                .get("spark_bigints");
        Assert.assertFalse(sparkDDL.isEmpty());
        Assert.assertTrue(StringUtils.containsIgnoreCase(sparkDDL, "USING PARQUET"));

        // view
        sparkSourceService.executeSQL("CREATE VIEW view_bigints as select id from default.spark_bigints");
        String viewDDL = DdlOperation.collectDDL(TableIdentifier.apply("view_bigints"),
                "show create view default.view_bigints");
        Assert.assertFalse(StringUtils.isEmpty(viewDDL));
    }

    @Test
    public void testLoadSamplesException() {
        try {
            sparkSourceService.exportTables(null, new String[] { "hive_bigints" }).getTables().get("hive_bigints");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
        }
        try {
            sparkSourceService.exportTables("db", new String[] { "hive_bigints" }).getTables().get("hive_bigints");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
        }
        try {
            sparkSourceService.exportTables("default", new String[] {}).getTables().get("hive_bigints");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
        }
        try {
            sparkSourceService.exportTables("default", new String[] { "" }).getTables().get("hive_bigints");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
        }
        try {
            sparkSourceService.exportTables("default", new String[] { "not_exits" }).getTables().get("hive_bigints");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
        }
    }

    @Test
    public void testLoadSamples() throws IOException, InterruptedException {
        Assert.assertEquals(9, sparkSourceService.loadSamples(ss, SaveMode.Overwrite).size());
        // KC-6666, table not exists but table location exists
        // re-create spark context and re-load samples
        {
            ss.stop();
            ss = SparkSession.builder().appName("local").master("local[1]").enableHiveSupport().getOrCreate();
            ss.sparkContext().hadoopConfiguration().set("javax.jdo.option.ConnectionURL",
                    "jdbc:derby:memory:db;create=true");
        }
        Assert.assertEquals(9, sparkSourceService.loadSamples(ss, SaveMode.Overwrite).size());
        FileUtils.deleteDirectory(new File("spark-warehouse"));
    }

    @Test
    public void testLoadSamples2() throws Exception {
        Assert.assertEquals(9, sparkSourceService.loadSamples().size());
        FileUtils.deleteDirectory(new File("spark-warehouse"));
    }

    @Test
    public void testTableExists() {
        Assert.assertTrue(sparkSourceService.tableExists("default", "COUNTRY"));
    }
}
