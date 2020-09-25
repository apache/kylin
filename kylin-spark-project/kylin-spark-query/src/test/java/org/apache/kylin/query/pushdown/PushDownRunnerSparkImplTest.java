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

package org.apache.kylin.query.pushdown;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.spark.sql.SparderContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PushDownRunnerSparkImplTest extends LocalFileMetadataTestCase {

    SparkSession ss;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        ss = SparkSession.builder().appName("local").master("local[1]").getOrCreate();
        SparderContext.setSparkSession(ss);
        StructType schema = new StructType();
        schema = schema.add("TRANS_ID", DataTypes.LongType, false);
        schema = schema.add("ORDER_ID", DataTypes.LongType, false);
        schema = schema.add("CAL_DT", DataTypes.DateType, false);
        schema = schema.add("LSTG_FORMAT_NAME", DataTypes.StringType, false);
        schema = schema.add("LEAF_CATEG_ID", DataTypes.LongType, false);
        schema = schema.add("LSTG_SITE_ID", DataTypes.IntegerType, false);
        schema = schema.add("SLR_SEGMENT_CD", DataTypes.FloatType, false);
        schema = schema.add("SELLER_ID", DataTypes.LongType, false);
        schema = schema.add("PRICE", DataTypes.createDecimalType(19, 4), false);
        schema = schema.add("ITEM_COUNT", DataTypes.DoubleType, false);
        schema = schema.add("TEST_COUNT_DISTINCT_BITMAP", DataTypes.StringType, false);
        ss.read().schema(schema).csv("../../examples/test_case_data/parquet_test/data/DEFAULT.TEST_KYLIN_FACT.csv")
                .createOrReplaceTempView("TEST_KYLIN_FACT");
    }

    @After
    public void after() throws Exception {
        ss.stop();
        cleanupTestMetadata();
    }

    @Test
    public void testCast() {
        PushDownRunnerSparkImpl pushDownRunnerSpark = new PushDownRunnerSparkImpl();
        pushDownRunnerSpark.init(null);

        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();

        List<String> queries = new ArrayList<>();
        queries.add("SELECT cast(ORDER_ID as integer) FROM TEST_KYLIN_FACT limit 10");
        queries.add("SELECT cast(LSTG_SITE_ID as long) FROM TEST_KYLIN_FACT limit 10");
        queries.add("SELECT cast(LSTG_SITE_ID as short) FROM TEST_KYLIN_FACT limit 10");
        queries.add("SELECT CAST(ORDER_ID AS varchar(20)) FROM TEST_KYLIN_FACT limit 10");
        queries.add("SELECT CAST(ORDER_ID AS char(20)) FROM TEST_KYLIN_FACT limit 10");
        queries.add("select SELLER_ID,ITEM_COUNT,sum(price)\n" + //
                "from (\n" + //
                "SELECT SELLER_ID, ITEM_COUNT,price\n" + //
                "\t, concat(concat(CAST(year(CAST(CAL_DT AS date)) AS varchar(4)), '-'),\n" + //
                "CAST(month(CAST(CAL_DT AS date)) AS varchar(2))) AS prt_mth\n" + //
                "FROM TEST_KYLIN_FACT) \n" + //
                "group by SELLER_ID,ITEM_COUNT,price limit 10"); //

        queries.add("select SELLER_ID,ITEM_COUNT,sum(price)\n" + //
                "from (\n" + //
                "SELECT SELLER_ID, ITEM_COUNT,price\n" + //
                "\t, concat(concat(CAST(year(CAST(CAL_DT AS date)) AS char(4)), '-'),\n" + //
                "CAST(month(CAST(CAL_DT AS date)) AS char(2))) AS prt_mth\n" + //
                "FROM TEST_KYLIN_FACT) \n" + //
                "group by SELLER_ID,ITEM_COUNT,price limit 10");

        queries.forEach(q -> {
            returnRows.clear();
            pushDownRunnerSpark.executeQuery(q, returnRows, returnColumnMeta);
            Assert.assertEquals(10, returnRows.size());
        });

    }

    @Test
    public void testPushDownRunnerSpark() {
        PushDownRunnerSparkImpl pushDownRunnerSpark = new PushDownRunnerSparkImpl();
        pushDownRunnerSpark.init(null);

        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();

        String sql = "select * from TEST_KYLIN_FACT";
        pushDownRunnerSpark.executeQuery(sql, returnRows, returnColumnMeta);

        Assert.assertEquals(10000, returnRows.size());
        Assert.assertEquals(11, returnColumnMeta.size());
        Assert.assertEquals("SPARK-SQL", pushDownRunnerSpark.getName());
    }

    @Test
    public void testPushDownRunnerSparkWithDotColumn() {
        PushDownRunnerSparkImpl pushDownRunnerSpark = new PushDownRunnerSparkImpl();
        pushDownRunnerSpark.init(null);

        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();

        String sql = "select TEST_KYLIN_FACT.price as `TEST_KYLIN_FACT.price` from TEST_KYLIN_FACT";
        pushDownRunnerSpark.executeQuery(sql, returnRows, returnColumnMeta);

        Assert.assertEquals(10000, returnRows.size());
        Assert.assertEquals(1, returnColumnMeta.size());
        Assert.assertEquals("SPARK-SQL", pushDownRunnerSpark.getName());
    }

    @Test
    public void testSelectTwoSameExpr() {
        PushDownRunnerSparkImpl pushDownRunnerSpark = new PushDownRunnerSparkImpl();
        pushDownRunnerSpark.init(null);

        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();

        String sql = "select sum(price), sum(price) from TEST_KYLIN_FACT";
        pushDownRunnerSpark.executeQuery(sql, returnRows, returnColumnMeta);

        Assert.assertEquals(1, returnRows.size());
        Assert.assertEquals(2, returnColumnMeta.size());
        Assert.assertEquals("SPARK-SQL", pushDownRunnerSpark.getName());
    }

    public void createTestMetadata() {
        String tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
    }
}
