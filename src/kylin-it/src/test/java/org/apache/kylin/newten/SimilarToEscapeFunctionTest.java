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

package org.apache.kylin.newten;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SimilarToEscapeFunctionTest extends NLocalWithSparkSessionTest {
    @BeforeClass
    public static void initSpark() {
        if (Shell.MAC)
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy
        if (ss != null && !ss.sparkContext().isStopped()) {
            ss.stop();
        }
        sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set("spark.memory.fraction", "0.1");
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");
        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/file_pruning");
        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/file_pruning" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Override
    public String getProject() {
        return "file_pruning";
    }

    @Test
    public void testSimilarToEscapeFunction() throws Exception {
        fullBuild("9cde9d25-9334-4b92-b229-a00f49453757");
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        List<String> result;

        String query1 = "SELECT count(*) FROM TEST_MEASURE where NAME1 SIMILAR TO 'Ch.na'";
        result = ExecAndComp.queryModel(getProject(), query1).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("3", result.get(0));

        String query2 = "SELECT count(*) FROM TEST_MEASURE where NAME1 SIMILAR TO 'Ch@_na' ESCAPE '@'";
        result = ExecAndComp.queryModel(getProject(), query2).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("1", result.get(0));

        String query3 = "SELECT count(*) FROM TEST_MEASURE where NAME1 SIMILAR TO 'Ch%%na' ESCAPE '%'";
        result = ExecAndComp.queryModel(getProject(), query3).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("1", result.get(0));

        result.remove(0);

        try {
            String query4 = "SELECT count(*) FROM TEST_MEASURE where NAME1 SIMILAR TO 'Ch@.na' ESCAPE '@@'";
            result = ExecAndComp.queryModel(getProject(), query4).collectAsList().stream()
                    .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        } catch (Exception e) {
            Assert.assertEquals("0", String.valueOf(result.size()));
        }

        String query5 = "SELECT count(*) FROM TEST_MEASURE where NAME1 SIMILAR TO 'Ch@%na' ESCAPE '@'";
        result = ExecAndComp.queryModel(getProject(), query5).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("1", result.get(0));

        String query6 = "SELECT count(*) FROM TEST_MEASURE where NAME1 SIMILAR TO 'Ch%na' ESCAPE '~'";
        result = ExecAndComp.queryModel(getProject(), query6).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("1", result.get(0));
    }
}
