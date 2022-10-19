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

package org.apache.kylin.query.engine;


import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.KylinSession$;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SparderInitSQLConfTest extends NLocalFileMetadataTestCase {

    @BeforeClass
    public static void beforeClass() throws Exception {

        try (SparkSession sparkSession = SparderEnv.getSparkSession()) {
            // do nothing
        }
    }

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetOrCreateKylinSession() {
        SparkConf sparkConf = getSparkConf();
        SparkSession.Builder sessionBuilder = SparkSession.builder()
                .enableHiveSupport().config(sparkConf)
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        SparkSession session = KylinSession$.MODULE$.KylinBuilder(sessionBuilder).buildCluster().getOrCreateKylinSession();
        Assert.assertTrue(session.sessionState().conf().ansiEnabled());
        Assert.assertTrue(SQLConf.get().ansiEnabled());
        session.close();
    }

    @Test
    public void testCloneSession() {
        SparkConf sparkConf = getSparkConf();
        SparkSession.Builder sessionBuilder = SparkSession.builder()
                .enableHiveSupport().config(sparkConf)
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        SparkSession session = KylinSession$.MODULE$.KylinBuilder(sessionBuilder).buildCluster().getOrCreateKylinSession();
        session = session.cloneSession();
        Assert.assertTrue(session.sessionState().conf().ansiEnabled());
        Assert.assertTrue(SQLConf.get().ansiEnabled());
        session.close();
    }

    @Test
    public void testNewSession() {
        SparkConf sparkConf = getSparkConf();
        SparkSession.Builder sessionBuilder = SparkSession.builder()
                .enableHiveSupport().config(sparkConf)
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        SparkSession session = KylinSession$.MODULE$.KylinBuilder(sessionBuilder).buildCluster().getOrCreateKylinSession();
        session = session.newSession();
        Assert.assertTrue(session.sessionState().conf().ansiEnabled());
        Assert.assertTrue(SQLConf.get().ansiEnabled());
        session.close();
    }

    public SparkConf getSparkConf() {
        SparkConf sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr()).setMaster("local[1]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");
        sparkConf.set("spark.sql.ansi.enabled", "true");
        return sparkConf;
    }

}
