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

package org.apache.kylin.engine.spark.utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.SystemPropertiesCache;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTestBase;
import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class HiveTransactionTableHelperTest extends NLocalWithSparkSessionTestBase {
    private final ColumnDesc[] COLUMN_DESCS = new ColumnDesc[2];
    private final String ORIGIN_DB = "testdb";
    private final String ORIGIN_TABLE = "test1";
    private final String INTERMEDIATE_TABLE = "test1_hive_tx";
    private final String STORAGE_FORMAT = "TEXTFILE";
    private final String STORAGE_DFS_DIR = "/test";
    private final String FILED_DELIMITER = "|";

    @BeforeClass
    public static void beforeClass() {
        if (SparderEnv.isSparkAvailable()) {
            SparderEnv.getSparkSession().close();
        }
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
    }

    @Before
    public void setup() {
        {
            ColumnDesc columnDesc = new ColumnDesc();
            columnDesc.setName("id1");
            columnDesc.setDatatype("integer");
            COLUMN_DESCS[0] = columnDesc;
        }
        {
            ColumnDesc columnDesc = new ColumnDesc();
            columnDesc.setName("str1");
            columnDesc.setDatatype("varchar");
            COLUMN_DESCS[1] = columnDesc;
        }
    }

    @Test
    public void testDoGetQueryHiveTemporaryTableSql() {
        SystemPropertiesCache.setProperty("kylin.source.provider.9",
                "org.apache.kylin.engine.spark.source.NSparkDataSource");
        SystemPropertiesCache.setProperty("kylin.build.resource.read-transactional-table-enabled", "true");
        KylinBuildEnv kylinBuildEnv = new KylinBuildEnv(getTestConfig());
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "tdh");
        TableDesc fact = tableMgr.getTableDesc("TDH_TEST.LINEORDER_PARTITION");
        fact.setTransactional(true);
        String result = HiveTransactionTableHelper.doGetQueryHiveTemporaryTableSql(fact, Maps.newHashMap(),
                "LO_ORDERKEY", kylinBuildEnv);
        Assert.assertTrue(
                result.startsWith("select LO_ORDERKEY from `TDH_TEST`.`LINEORDER_PARTITION_HIVE_TX_INTERMEDIATE`"));

        result = HiveTransactionTableHelper.doGetQueryHiveTemporaryTableSql(fact, Maps.newHashMap(),
                "LO_ORDERKEY, LO_LINENUMBER", kylinBuildEnv);
        Assert.assertTrue(result.startsWith(
                "select LO_ORDERKEY, LO_LINENUMBER from `TDH_TEST`.`LINEORDER_PARTITION_HIVE_TX_INTERMEDIATE`"));

        PartitionDesc partitionDesc = new PartitionDesc();
        ColumnDesc columnDesc = new ColumnDesc();
        columnDesc.setName("LO_DATE");
        columnDesc.setDatatype("date");
        columnDesc.setTable(fact);
        NDataModel nDataModel = new NDataModel();
        nDataModel.setUuid(UUID.randomUUID().toString());
        TableRef tableRef = new TableRef(nDataModel, "LINEORDER_PARTITION", fact, false);
        partitionDesc.setPartitionDateColumnRef(new TblColRef(tableRef, columnDesc));
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd hh:mm:ss");
        fact.setPartitionDesc(partitionDesc);
        Map<String, String> params = Maps.newHashMap();
        params.put("segmentStart", "1637387522");
        params.put("segmentEnd", "1637905922");
        result = HiveTransactionTableHelper.doGetQueryHiveTemporaryTableSql(fact, params, "LO_ORDERKEY, LO_LINENUMBER",
                kylinBuildEnv);
        Assert.assertTrue(result.startsWith(
                "select LO_ORDERKEY, LO_LINENUMBER from `TDH_TEST`.`LINEORDER_PARTITION_HIVE_TX_INTERMEDIATE`")
                && result.endsWith("WHERE `LO_DATE` BETWEEN '1970-01-20 06:49:47' AND '1970-01-20 06:58:25'"));
    }

    @Test
    public void testHiveInitStatement() {
        String DATABASE = "DEFAULT";
        Assert.assertEquals("USE `DEFAULT`;\n", HiveTransactionTableHelper.generateHiveInitStatements(DATABASE));
        Assert.assertEquals("", HiveTransactionTableHelper.generateHiveInitStatements(""));
    }

    @Test
    public void testGetTableDir() {
        Assert.assertEquals("test/table1", HiveTransactionTableHelper.getTableDir("table1", "test"));
        Assert.assertEquals("test/table1", HiveTransactionTableHelper.getTableDir("table1", "test/"));
    }

    @Test
    public void testGetHiveDataType() {
        Assert.assertEquals("string", HiveTransactionTableHelper.getHiveDataType("varchar"));
        Assert.assertNotEquals("integer", HiveTransactionTableHelper.getHiveDataType("integer"));
        Assert.assertEquals("int", HiveTransactionTableHelper.getHiveDataType("integer"));
        Assert.assertEquals("int", HiveTransactionTableHelper.getHiveDataType("integer"));
        Assert.assertEquals("bigint", HiveTransactionTableHelper.getHiveDataType("bigint"));
        Assert.assertNotEquals("int", HiveTransactionTableHelper.getHiveDataType("bigint"));
        Assert.assertEquals("decimal", HiveTransactionTableHelper.getHiveDataType("decimal"));
        Assert.assertEquals("abc", HiveTransactionTableHelper.getHiveDataType("abc"));
        Assert.assertEquals("123", HiveTransactionTableHelper.getHiveDataType("123"));
    }

    @Test
    public void testInsertDataStatement() {
        String queryCondition = "";
        String statement = String.format(Locale.ROOT, "INSERT OVERWRITE TABLE `test1_hive_tx` SELECT\n" + "`ID1`\n"
                + ",`STR1`\n" + "FROM `testdb`.`test1` %s\n;\n", queryCondition);
        Assert.assertEquals(statement, HiveTransactionTableHelper.generateInsertDataStatement(COLUMN_DESCS, ORIGIN_DB,
                ORIGIN_TABLE, INTERMEDIATE_TABLE, queryCondition));

        queryCondition = " where cal_dt between '2010-01-01 00:00:00' and  '2010-02-01 00:00:00'";
        statement = String.format(Locale.ROOT, "INSERT OVERWRITE TABLE `test1_hive_tx` SELECT\n" + "`ID1`\n"
                + ",`STR1`\n" + "FROM `testdb`.`test1` %s\n;\n", queryCondition);
        Assert.assertEquals(statement, HiveTransactionTableHelper.generateInsertDataStatement(COLUMN_DESCS, ORIGIN_DB,
                ORIGIN_TABLE, INTERMEDIATE_TABLE, queryCondition));

        statement = String.format(Locale.ROOT,
                "INSERT OVERWRITE TABLE test1_hive_tx SELECT\n" + "ID1\n" + ",STR1\n" + "FROM testdb.test1 %s\n;\n",
                queryCondition);
        Assert.assertNotEquals(statement, HiveTransactionTableHelper.generateInsertDataStatement(COLUMN_DESCS,
                ORIGIN_DB, ORIGIN_TABLE, INTERMEDIATE_TABLE, queryCondition));

        queryCondition = " where cal_dt between '2010-01-01 00:00:00' and  '2010-02-01 00:00:00'";
        statement = String.format(Locale.ROOT,
                "INSERT OVERWRITE TABLE test1_hive_tx SELECT\n" + "ID1\n" + ",STR1\n" + "FROM testdb.test1 %s\n;\n",
                queryCondition);
        Assert.assertNotEquals(statement, HiveTransactionTableHelper.generateInsertDataStatement(COLUMN_DESCS,
                ORIGIN_DB, ORIGIN_TABLE, INTERMEDIATE_TABLE, queryCondition));
    }

    @Test
    public void testDropTableStatement() {
        String statement = "DROP TABLE IF EXISTS `test1_hive_tx`;\n";
        Assert.assertEquals(statement, HiveTransactionTableHelper.generateDropTableStatement(INTERMEDIATE_TABLE));

        statement = "DROP TABLE IF EXISTS test1_hive_tx;\n";
        Assert.assertNotEquals(statement, HiveTransactionTableHelper.generateDropTableStatement(INTERMEDIATE_TABLE));
    }

    @Test
    public void testCreateTableStatement() {
        String statement = "CREATE EXTERNAL TABLE IF NOT EXISTS `test1_hive_tx`\n" + "(\n" + "`ID1` int\n"
                + ",`STR1` string\n" + ")\n" + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'\n"
                + "STORED AS TEXTFILE\n" + "LOCATION '/test/test1_hive_tx';\n"
                + "ALTER TABLE `test1_hive_tx` SET TBLPROPERTIES('auto.purge'='true');\n";
        String actual = HiveTransactionTableHelper.generateCreateTableStatement(INTERMEDIATE_TABLE,
                STORAGE_DFS_DIR.concat("/").concat(INTERMEDIATE_TABLE), COLUMN_DESCS, STORAGE_FORMAT, FILED_DELIMITER);
        Assert.assertEquals(statement, actual);

        statement = "CREATE EXTERNAL TABLE IF NOT EXISTS test1_hive_tx\n" + "(\n" + "ID1 int\n" + ",STR1 string\n"
                + ")\n" + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'\n" + "STORED AS TEXTFILE\n"
                + "LOCATION '/test/test1_hive_tx';\n"
                + "ALTER TABLE test1_hive_tx SET TBLPROPERTIES('auto.purge'='true');\n";
        actual = HiveTransactionTableHelper.generateCreateTableStatement(INTERMEDIATE_TABLE,
                STORAGE_DFS_DIR.concat("/").concat(INTERMEDIATE_TABLE), COLUMN_DESCS, STORAGE_FORMAT, FILED_DELIMITER);
        Assert.assertNotEquals(statement, actual);
    }

    @Test
    public void testCreateTableStatements() {
        TableDesc tableDesc = new TableDesc();
        tableDesc.setDatabase(ORIGIN_DB);
        tableDesc.setName(ORIGIN_TABLE);
        String queryCondition = "";
        String statement = String.format(Locale.ROOT,
                "DROP TABLE IF EXISTS test1_hive_tx`;\n" + "CREATE EXTERNAL TABLE IF NOT EXISTS `test1_hive_tx`\n"
                        + "(\n" + "`ID1` int\n" + ",`STR1` string\n" + ")\n"
                        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'\n" + "STORED AS TEXTFILE\n"
                        + "LOCATION '/test/test1_hive_tx';\n"
                        + "ALTER TABLE `test1_hive_tx` SET TBLPROPERTIES('auto.purge'='true');\n"
                        + "INSERT OVERWRITE TABLE `test1_hive_tx` SELECT\n" + "`ID1`\n" + ",`STR1`\n"
                        + "FROM `TESTDB`.`TEST1` %s\n;\n",
                queryCondition);
        String actual = HiveTransactionTableHelper.getCreateTableStatement(tableDesc, INTERMEDIATE_TABLE, COLUMN_DESCS,
                STORAGE_DFS_DIR.concat("/").concat(INTERMEDIATE_TABLE), STORAGE_FORMAT, FILED_DELIMITER,
                queryCondition);
        Assert.assertNotEquals(statement, actual);

        queryCondition = " where cal_dt between '2010-01-01 00:00:00' and  '2010-02-01 00:00:00'";
        statement = String.format(Locale.ROOT,
                "DROP TABLE IF EXISTS `test1_hive_tx`;\n" + "CREATE EXTERNAL TABLE IF NOT EXISTS `test1_hive_tx`\n"
                        + "(\n" + "`ID1` int\n" + ",`STR1` string\n" + ")\n"
                        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'\n" + "STORED AS TEXTFILE\n"
                        + "LOCATION '/test/test1_hive_tx';\n"
                        + "ALTER TABLE `test1_hive_tx` SET TBLPROPERTIES('auto.purge'='true');\n"
                        + "INSERT OVERWRITE TABLE `test1_hive_tx` SELECT\n" + "`ID1`\n" + ",`STR1`\n"
                        + "FROM `TESTDB`.`TEST1` %s\n;\n",
                queryCondition);
        actual = HiveTransactionTableHelper.getCreateTableStatement(tableDesc, INTERMEDIATE_TABLE, COLUMN_DESCS,
                STORAGE_DFS_DIR.concat("/").concat(INTERMEDIATE_TABLE), STORAGE_FORMAT, FILED_DELIMITER,
                queryCondition);
        Assert.assertEquals(statement, actual);
    }

    @Test
    public void testCreateTableStatements2() {
        TableDesc tableDesc = new TableDesc();
        tableDesc.setDatabase(ORIGIN_DB);
        tableDesc.setName(ORIGIN_TABLE);
        String queryCondition = "";
        String statement = String.format(Locale.ROOT,
                "DROP TABLE IF EXISTS test1_hive_tx;\n" + "CREATE EXTERNAL TABLE IF NOT EXISTS test1_hive_tx\n" + "(\n"
                        + "ID1 int\n" + ",STR1 string\n" + ")\n" + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'\n"
                        + "STORED AS TEXTFILE\n" + "LOCATION '/test/test1_hive_tx';\n"
                        + "ALTER TABLE test1_hive_tx SET TBLPROPERTIES('auto.purge'='true');\n"
                        + "INSERT OVERWRITE TABLE test1_hive_tx SELECT\n" + "ID1\n" + ",STR1\n"
                        + "FROM testdb.test1 %s\n;\n",
                queryCondition);
        String actual = HiveTransactionTableHelper.getCreateTableStatement(tableDesc, INTERMEDIATE_TABLE, COLUMN_DESCS,
                STORAGE_DFS_DIR.concat("/").concat(INTERMEDIATE_TABLE), STORAGE_FORMAT, FILED_DELIMITER,
                queryCondition);
        Assert.assertNotEquals(statement, actual);

        queryCondition = " where cal_dt between '2010-01-01 00:00:00' and  '2010-02-01 00:00:00'";
        statement = String.format(Locale.ROOT,
                "DROP TABLE IF EXISTS test1_hive_tx;\n" + "CREATE EXTERNAL TABLE IF NOT EXISTS test1_hive_tx\n" + "(\n"
                        + "ID1 int\n" + ",STR1 string\n" + ")\n" + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'\n"
                        + "STORED AS TEXTFILE\n" + "LOCATION '/test/test1_hive_tx';\n"
                        + "ALTER TABLE test1_hive_tx SET TBLPROPERTIES('auto.purge'='true');\n"
                        + "INSERT OVERWRITE TABLE test1_hive_tx SELECT\n" + "ID1\n" + ",STR1\n"
                        + "FROM testdb.test1 %s\n;\n",
                queryCondition);
        actual = HiveTransactionTableHelper.getCreateTableStatement(tableDesc, INTERMEDIATE_TABLE, COLUMN_DESCS,
                STORAGE_DFS_DIR.concat("/").concat(INTERMEDIATE_TABLE), STORAGE_FORMAT, FILED_DELIMITER,
                queryCondition);
        Assert.assertNotEquals(statement, actual);
    }

    @Test
    public void testCreateHiveTableDirIfNeeded() {
        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
        try {
            Path dirPath = new Path("/tmp", INTERMEDIATE_TABLE);
            HiveTransactionTableHelper.createHiveTableDirIfNeeded("/tmp", INTERMEDIATE_TABLE);
            Assert.assertEquals(Boolean.FALSE, fileSystem.exists(new Path(STORAGE_DFS_DIR, INTERMEDIATE_TABLE)));
            Assert.assertEquals(Boolean.TRUE, fileSystem.exists(dirPath));
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    @Test
    public void testDetermineDBUsed() {
        KylinBuildEnv env = mock(KylinBuildEnv.class);
        KylinConfig config = mock(KylinConfig.class);
        when(env.kylinConfig()).thenReturn(config);
        TableDesc tableDesc = mock(TableDesc.class);

        // Test using original db
        when(tableDesc.getCaseSensitiveDatabase()).thenReturn("testdb");
        when(config.getBuildResourceTemporaryWritableDB()).thenReturn(null);
        Assert.assertEquals("TESTDB", HiveTransactionTableHelper.determineDBUsed(env, tableDesc));

        // Test using config db
        when(tableDesc.getCaseSensitiveDatabase()).thenReturn("testdb");
        when(config.getBuildResourceTemporaryWritableDB()).thenReturn("another_db");
        Assert.assertEquals("ANOTHER_DB", HiveTransactionTableHelper.determineDBUsed(env, tableDesc));

        // Other unusual situation
        when(tableDesc.getCaseSensitiveDatabase()).thenReturn("null");
        when(config.getBuildResourceTemporaryWritableDB()).thenReturn(null);
        Assert.assertEquals("DEFAULT", HiveTransactionTableHelper.determineDBUsed(env, tableDesc));
    }
}
