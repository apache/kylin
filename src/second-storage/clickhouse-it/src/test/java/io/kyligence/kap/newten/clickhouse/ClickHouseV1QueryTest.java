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
package io.kyligence.kap.newten.clickhouse;

import java.sql.Connection;
import java.util.List;

import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.jdbc.ClickHouseDialect$;
import org.apache.spark.sql.execution.datasources.jdbc.ShardOptions$;
import org.apache.spark.sql.jdbc.JdbcDialects$;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.google.common.collect.ImmutableList;

import lombok.extern.slf4j.Slf4j;
import scala.collection.JavaConversions;

@Slf4j
public class ClickHouseV1QueryTest extends NLocalWithSparkSessionTest {

    @BeforeClass
    public static void beforeClass() {
        JdbcDialects$.MODULE$.registerDialect(ClickHouseDialect$.MODULE$);
        NLocalWithSparkSessionTest.beforeClass();
    }

    @AfterClass
    public static void afterClass() {
        NLocalWithSparkSessionTest.afterClass();
        JdbcDialects$.MODULE$.unregisterDialect(ClickHouseDialect$.MODULE$);
    }

    @Test
    public void testMultipleShard() throws Exception {
        boolean result = ClickHouseUtils.prepare2Instances(true, (JdbcDatabaseContainer<?> clickhouse1,
                Connection connection1, JdbcDatabaseContainer<?> clickhouse2, Connection connection2) -> {

            List<String> shardList = ImmutableList.of(clickhouse1.getJdbcUrl(), clickhouse2.getJdbcUrl());
            String shards = ShardOptions$.MODULE$.buildSharding(JavaConversions.asScalaBuffer(shardList));
            Dataset<Row> df = ss.read()
                    .format("org.apache.spark.sql.execution.datasources.jdbc.ShardJdbcRelationProvider")
                    .option("url", clickhouse1.getJdbcUrl())
                    .option("dbtable", ClickHouseUtils.PrepareTestData.db + "." + ClickHouseUtils.PrepareTestData.table)
                    .option(ShardOptions$.MODULE$.SHARD_URLS(), shards).load();

            Assert.assertEquals(7, df.count());
            df.select("n3").show();
            return true;
        });
        Assert.assertTrue(result);
    }
}
