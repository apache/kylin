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
package org.apache.kylin.metadata;

import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.KylinSession$;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.Assert;
import org.junit.Test;

public class SQLConfTest extends NLocalWithSparkSessionTest {

    @Test
    public void testSQLConf() {
        String shuffleName = "spark.sql.shuffle.partitions";
        SparkConf sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr()).setMaster("local[1]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set(shuffleName, "1");
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");
        sparkConf.set("spark.sql.ansi.enabled", "true");

        SparkSession.Builder sessionBuilder = SparkSession.builder()
                .enableHiveSupport().config(sparkConf)
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
        SparkSession kylinSession = KylinSession$.MODULE$.KylinBuilder(sessionBuilder).buildCluster().getOrCreateKylinSession();
        SQLConf defaultSQLConf = SQLConf.get();
        SQLConf kylinSessionSQLConf = kylinSession.sessionState().conf();
        Assert.assertEquals(kylinSessionSQLConf, defaultSQLConf);
        kylinSession.newSession();
        SQLConf defaultSQLConfAfter = SQLConf.get();
        SQLConf kylinSessionSQLConfAfter = kylinSession.sessionState().conf();
        Assert.assertEquals(defaultSQLConf, defaultSQLConfAfter);
        Assert.assertEquals(kylinSessionSQLConf, kylinSessionSQLConfAfter);
    }

}
