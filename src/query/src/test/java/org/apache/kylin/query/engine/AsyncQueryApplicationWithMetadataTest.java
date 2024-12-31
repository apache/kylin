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

import static org.apache.kylin.job.constant.ExecutableConstants.COLUMNAR_SHUFFLE_MANAGER;
import static org.apache.kylin.job.constant.ExecutableConstants.GLUTEN_PLUGIN;
import static org.apache.kylin.job.constant.ExecutableConstants.SPARK_PLUGINS;
import static org.apache.kylin.job.constant.ExecutableConstants.SPARK_SHUFFLE_MANAGER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.util.TestUtils;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.streaming.ReflectionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

@MetadataInfo
class AsyncQueryApplicationWithMetadataTest {

    @Test
    void testDisableCurrentThreadGlutenIfNeed() throws Exception {
        val asyncQueryApplication = new AsyncQueryApplication();
        val config = TestUtils.getTestConfig();
        ReflectionUtils.setField(asyncQueryApplication, "config", config);

        val sparkConf = new SparkConf();
        sparkConf.set("spark.master", "local[111]");
        assertFalseEnableForCurrentThread(asyncQueryApplication, sparkConf);

        config.setProperty("kylin.unique-async-query.gluten.enabled", KylinConfigBase.TRUE);
        assertNullEnableForCurrentThread(asyncQueryApplication, sparkConf);

        config.setProperty("kylin.unique-async-query.gluten.enabled", KylinConfigBase.FALSE);
        assertFalseEnableForCurrentThread(asyncQueryApplication, sparkConf);
    }

    private void assertNullEnableForCurrentThread(SparkApplication application, SparkConf sparkConf) {
        try (val sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate();) {
            ReflectionUtils.setField(application, "ss", sparkSession);

            application.disableCurrentThreadGlutenIfNeed();

            val ss = (SparkSession) ReflectionUtils.getField(application, "ss");
            assertNull(ss.sparkContext().getLocalProperty("gluten.enabledForCurrentThread"));
        }
    }

    private void assertFalseEnableForCurrentThread(SparkApplication application, SparkConf sparkConf) {
        try (val sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate();) {
            ReflectionUtils.setField(application, "ss", sparkSession);

            application.disableCurrentThreadGlutenIfNeed();

            val ss = (SparkSession) ReflectionUtils.getField(application, "ss");
            assertEquals("false", ss.sparkContext().getLocalProperty("gluten.enabledForCurrentThread"));
        }
    }

    @Test
    void testRemoveGlutenParamsIfNeed() throws Exception {
        val asyncQueryApplication = new AsyncQueryApplication() {
            @Override
            protected void doExecute() {
                // do nothing
            }
        };
        val config = TestUtils.getTestConfig();
        ReflectionUtils.setField(asyncQueryApplication, "config", config);
        val sparkPrefix = "kylin.query.async-query.spark-conf.";
        config.setProperty("kylin.env", "PROD");
        config.setProperty(sparkPrefix + SPARK_PLUGINS, GLUTEN_PLUGIN + ",org.apache.spark.kyuubi.KyuubiPlugin");
        config.setProperty(sparkPrefix + "spark.gluten.enable", "true");
        config.setProperty(sparkPrefix + "spark.master", "yarn");
        config.setProperty(sparkPrefix + "spark.eventLog.enabled", "false");
        config.setProperty(sparkPrefix + SPARK_SHUFFLE_MANAGER, COLUMNAR_SHUFFLE_MANAGER);
        config.setProperty("kylin.engine.gluten.enabled", "true");

        assertWithOutGluten(asyncQueryApplication);

        config.setProperty("kylin.unique-async-query.gluten.enabled", KylinConfigBase.TRUE);
        assertWithGluten(asyncQueryApplication);

        config.setProperty("kylin.unique-async-query.gluten.enabled", KylinConfigBase.FALSE);
        assertWithOutGluten(asyncQueryApplication);

    }

    private static void assertWithGluten(SparkApplication application) throws Exception {
        val sparkConf = new SparkConf();
        sparkConf.set("spark.master", "yarn");
        sparkConf.set("spark.eventLog.enabled", "false");
        application.exchangeSparkConf(sparkConf);
        val atomicSparkConf = ((AtomicReference<SparkConf>) ReflectionTestUtils.getField(application,
                "atomicSparkConf"));
        val actalSparkConf = atomicSparkConf.get();
        assertEquals(COLUMNAR_SHUFFLE_MANAGER, actalSparkConf.get(SPARK_SHUFFLE_MANAGER));
        assertEquals("true", actalSparkConf.get("spark.gluten.enable"));
        assertEquals(GLUTEN_PLUGIN + ",org.apache.spark.kyuubi.KyuubiPlugin", actalSparkConf.get(SPARK_PLUGINS));
        assertEquals("yarn", actalSparkConf.get("spark.master"));
        assertEquals("false", actalSparkConf.get("spark.eventLog.enabled"));
    }

    private static void assertWithOutGluten(SparkApplication application) throws Exception {
        val sparkConf = new SparkConf();
        sparkConf.set("spark.master", "yarn");
        sparkConf.set("spark.eventLog.enabled", "false");
        application.exchangeSparkConf(sparkConf);
        val atomicSparkConf = ((AtomicReference<SparkConf>) ReflectionTestUtils.getField(application,
                "atomicSparkConf"));
        val actalSparkConf = atomicSparkConf.get();
        assertFalse(Arrays.stream(actalSparkConf.getAll()).anyMatch(conf -> conf._1.contains("gluten")));
        assertEquals("sort", actalSparkConf.get(SPARK_SHUFFLE_MANAGER));
        assertEquals("org.apache.spark.kyuubi.KyuubiPlugin", actalSparkConf.get(SPARK_PLUGINS));
    }
}
