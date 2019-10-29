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

import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.apache.spark.sql.SparkSession;


public class SparkDataSourceTestBase {
    protected SparkSession spark = SparkSqlSource.sparkSession();
    protected String testDb = "test";
    protected KylinConfig config = getTestConfig();
    protected String defaultDb = config.getSparkSqlDefaultDatabase();

    private KylinConfig getTestConfig() {
        Properties properties = new Properties();
        properties.setProperty("kylin.source.sparksql.ds-ids", "ds1,ds2,ds3,ds4");

        // Can also use com.databricks.spark.csv
        properties.setProperty("kylin.source.sparksql.ds1.className", "csv");
        properties.setProperty("kylin.source.sparksql.ds1.paths",
                this.getClass().getClassLoader().getResource("employees.csv").getPath());
        properties.setProperty("kylin.source.sparksql.ds1.options",
                "{\"header\":\"true\",\"inferSchema\":\"true\"}");
        properties.setProperty("kylin.source.sparksql.ds1.tableName", testDb + ".employees_csv");

        // Can also use org.apache.spark.sql.json
        properties.setProperty("kylin.source.sparksql.ds2.className", "json");
        properties.setProperty("kylin.source.sparksql.ds2.paths",
                this.getClass().getClassLoader().getResource("employees.json").getPath());
        properties.setProperty("kylin.source.sparksql.ds2.options", "{}");
        properties.setProperty("kylin.source.sparksql.ds2.tableName", testDb + ".employees_json");


        // Can also use org.apache.spark.sql.parquet
        properties.setProperty("kylin.source.sparksql.ds3.className", "parquet");
        properties.setProperty("kylin.source.sparksql.ds3.paths",
                this.getClass().getClassLoader().getResource("employees.parquet").getPath());
        properties.setProperty("kylin.source.sparksql.ds3.options", "{}");
        properties.setProperty("kylin.source.sparksql.ds3.tableName", "employees_parquet");

        // Can also use orc
        properties.setProperty("kylin.source.sparksql.ds4.className",
                "org.apache.spark.sql.hive.orc");
        properties.setProperty("kylin.source.sparksql.ds4.paths",
                this.getClass().getClassLoader().getResource("employees.orc").getPath());
        properties.setProperty("kylin.source.sparksql.ds4.options", "{}");
        properties.setProperty("kylin.source.sparksql.ds4.tableName", "employees_orc");

        KylinConfig config = KylinConfig.createKylinConfig(properties);
        KylinConfig.setEnvInstance(config);
        return config;
    }
}
