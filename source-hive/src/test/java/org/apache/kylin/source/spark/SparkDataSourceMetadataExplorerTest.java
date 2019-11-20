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

package org.apache.kylin.source.spark;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.hive.CreateSparkDataSourceTableStep;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class SparkDataSourceMetadataExplorerTest extends SparkDataSourceTestBase {
    private SparkDataSourceMetadataExplorer metadataExplorer =
            new SparkDataSourceMetadataExplorer(config);

    @Test
    public void testMetadataExplorer() throws Exception {
        List<String> databases = metadataExplorer.listDatabases();
        assertTrue(databases.size() == 2);
        assertTrue(databases.contains(defaultDb));
        assertTrue(databases.contains(testDb));

        List<String> defaultTables = metadataExplorer.listTables(defaultDb);
        assertTrue(defaultTables.size() == 2);
        assertTrue(defaultTables.contains("employees_parquet"));
        assertTrue(defaultTables.contains("employees_orc"));

        List<String> testTables = metadataExplorer.listTables(testDb);
        assertTrue(testTables.size() == 2);
        assertTrue(testTables.contains("employees_csv"));
        assertTrue(testTables.contains("employees_json"));

        Pair<TableDesc, TableExtDesc> metadata1 =
                metadataExplorer.loadTableMetadata(testDb, "employees_csv", "default");
        TableDesc tableDesc1 = metadata1.getFirst();
        TableExtDesc tableExtDesc1 = metadata1.getSecond();
        assertTrue(tableDesc1.toString().equals("TableDesc{name='employees_csv', " +
                "columns=[ColumnDesc{id='1', name='name', datatype='string', comment='null'}, " +
                "ColumnDesc{id='2', name='salary', datatype='int', comment='null'}], " +
                "sourceType=5, " +
                "tableType='csv', " +
                "database=DatabaseDesc [name=test], " +
                "identity='TEST.EMPLOYEES_CSV'}"));
        assertTrue(tableExtDesc1.toString().equals(
                "TableExtDesc{name='TEST.EMPLOYEES_CSV', columns_samples=[]"));

        Pair<TableDesc, TableExtDesc> metadata2 =
                metadataExplorer.loadTableMetadata(testDb, "employees_json", "default");
        TableDesc tableDesc2 = metadata2.getFirst();
        TableExtDesc tableExtDesc2 = metadata2.getSecond();
        assertTrue(tableDesc2.toString().equals("TableDesc{name='employees_json', " +
                "columns=[ColumnDesc{id='1', name='name', datatype='string', comment='null'}, " +
                "ColumnDesc{id='2', name='salary', datatype='long', comment='null'}], " +
                "sourceType=5, " +
                "tableType='json', " +
                "database=DatabaseDesc [name=test], " +
                "identity='TEST.EMPLOYEES_JSON'}"));
        assertTrue(tableExtDesc2.toString().equals(
                "TableExtDesc{name='TEST.EMPLOYEES_JSON', columns_samples=[]"));

        Pair<TableDesc, TableExtDesc> metadata3 =
                metadataExplorer.loadTableMetadata(defaultDb, "employees_parquet", "default");
        TableDesc tableDesc3 = metadata3.getFirst();
        TableExtDesc tableExtDesc3 = metadata3.getSecond();
        assertTrue(tableDesc3.toString().equals("TableDesc{name='employees_parquet', " +
                "columns=[ColumnDesc{id='1', name='name', datatype='string', comment='null'}, " +
                "ColumnDesc{id='2', name='salary', datatype='long', comment='null'}], " +
                "sourceType=5, " +
                "tableType='parquet', " +
                "database=DatabaseDesc [name=spark_datasource], " +
                "identity='SPARK_DATASOURCE.EMPLOYEES_PARQUET'}"));
        assertTrue(tableExtDesc3.toString().equals(
                "TableExtDesc{name='SPARK_DATASOURCE.EMPLOYEES_PARQUET', columns_samples=[]"));

        Pair<TableDesc, TableExtDesc> metadata4 =
                metadataExplorer.loadTableMetadata(defaultDb, "employees_orc", "default");
        TableDesc tableDesc4 = metadata4.getFirst();
        TableExtDesc tableExtDesc4 = metadata4.getSecond();
        assertTrue(tableDesc4.toString().equals("TableDesc{name='employees_orc', " +
                "columns=[ColumnDesc{id='1', name='name', datatype='string', comment='null'}, " +
                "ColumnDesc{id='2', name='salary', datatype='long', comment='null'}], " +
                "sourceType=5, " +
                "tableType='org.apache.spark.sql.hive.orc', " +
                "database=DatabaseDesc [name=spark_datasource], " +
                "identity='SPARK_DATASOURCE.EMPLOYEES_ORC'}"));
        assertTrue(tableExtDesc4.toString().equals(
                "TableExtDesc{name='SPARK_DATASOURCE.EMPLOYEES_ORC', columns_samples=[]"));
    }

    @Test
    public void testCreateSparkDataSourceTable() throws PersistentException, ExecuteException, Exception {
        spark.sql("DROP TABLE IF EXISTS test.employees_csv");
        spark.sql("DROP TABLE IF EXISTS test.employees_json");
        spark.sql("DROP TABLE IF EXISTS spark_datasource.employees_parquet");
        spark.sql("DROP TABLE IF EXISTS spark_datasource.employees_orc");
        spark.sql("DROP DATABASE IF EXISTS spark_datasource CASCADE");
        spark.sql("DROP DATABASE IF EXISTS test CASCADE");

        CreateSparkDataSourceTableStep step = new CreateSparkDataSourceTableStep("default",
                "SELECT *\n" +
                        "FROM test.employees_csv csv\n" +
                        "\tJOIN test.employees_json json ON csv.name = json.name\n" +
                        "\tJOIN spark_datasource.employees_parquet parquet ON json.name = parquet.name\n" +
                        "\tJOIN spark_datasource.employees_orc orc ON parquet.name = orc.name");
        ExecuteResult executeResult = step.doWork(null);
        Assert.assertTrue(executeResult.output(), executeResult.succeed());

        assertTrue(spark.sql("select * from spark_datasource.employees_parquet where salary > 0").count() == 4);
        assertTrue(spark.sql("select * from spark_datasource.employees_orc where salary > 0").count() == 4);

        assertTrue(spark.sql("select * from test.employees_json where salary > 0").count() == 4);
        assertTrue(spark.sql("select * from test.employees_csv where salary > 0").count() == 4);
    }
}
