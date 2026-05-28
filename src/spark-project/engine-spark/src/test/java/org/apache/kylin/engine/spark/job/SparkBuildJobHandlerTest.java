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

package org.apache.kylin.engine.spark.job;

import static org.apache.kylin.engine.spark.job.NSparkExecutable.SPARK_MASTER;

import java.lang.reflect.Method;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTestBase;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.exception.ExecuteException;
import org.junit.Assert;
import org.junit.Test;

public class SparkBuildJobHandlerTest extends NLocalWithSparkSessionTestBase {

    @Test
    public void testKillOrphanApplicationIfExists() {
        KylinConfig config = getTestConfig();
        ISparkJobHandler handler = (ISparkJobHandler) ClassUtil.newInstance(config.getSparkBuildJobHandlerClassName());
        Assert.assertTrue(handler instanceof DefaultSparkBuildJobHandler);
        Map<String, String> sparkConf = Maps.newHashMap();
        String jobStepId = "testId";
        handler.killOrphanApplicationIfExists(getProject(), jobStepId, config, false, sparkConf);
        config.setProperty("kylin.engine.cluster-manager-timeout-threshold", "3s");

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setProject(getProject());
        config.setProperty("kylin.engine.spark-conf." + SPARK_MASTER, "mock");
        sparkExecutable.killOrphanApplicationIfExists(jobStepId);
    }

    @Test
    public void testCheckApplicationJar() {
        KylinConfig config = getTestConfig();
        ISparkJobHandler handler = (ISparkJobHandler) ClassUtil.newInstance(config.getSparkBuildJobHandlerClassName());
        Assert.assertTrue(handler instanceof DefaultSparkBuildJobHandler);
        try {
            handler.checkApplicationJar(config);
            Assert.fail();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e instanceof IllegalStateException);
        }
        String key = "kylin.engine.spark.job-jar";
        config.setProperty(key, "hdfs://127.0.0.1:0/mock");
        try {
            handler.checkApplicationJar(config);
            Assert.fail();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e instanceof ExecuteException);
        }
    }

    @Test
    public void testExecuteCmd() throws ExecuteException {
        KylinConfig config = getTestConfig();
        ISparkJobHandler handler = (ISparkJobHandler) ClassUtil.newInstance(config.getSparkBuildJobHandlerClassName());
        Assert.assertTrue(handler instanceof DefaultSparkBuildJobHandler);
        String cmd = "";
        Map<String, String> updateInfo = handler.runSparkSubmit(cmd, "");
        Assert.assertEquals(cmd, updateInfo.get("output"));
        Assert.assertNotNull(updateInfo.get("process_id"));

    }

    @Test
    public void testAppendSparkConfRejectSingleQuote() {
        DefaultSparkBuildJobHandler handler = new DefaultSparkBuildJobHandler();

        StringBuilder sb = new StringBuilder();
        handler.appendSparkConf(sb, "spark.yarn.queue", "normalQueue");
        Assert.assertTrue(sb.toString().contains("--conf 'spark.yarn.queue=normalQueue'"));

        // conf value containing single quote must be rejected
        try {
            handler.appendSparkConf(new StringBuilder(), "spark.yarn.queue",
                    "default'; touch /tmp/pwned; echo '");
            Assert.fail("Should have thrown IllegalArgumentException for value containing single quote");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("single quote"));
        }

        // pipe without single quote is acceptable
        StringBuilder sb2 = new StringBuilder();
        handler.appendSparkConf(sb2, "spark.yarn.queue", "a|b");
        Assert.assertTrue(sb2.toString().contains("--conf 'spark.yarn.queue=a|b'"));
    }

    @Test
    public void testCheckCommandInjectionBlocked() throws Exception {
        DefaultSparkBuildJobHandler handler = new DefaultSparkBuildJobHandler();
        Method method = DefaultSparkBuildJobHandler.class.getDeclaredMethod("checkCommandInjection", String.class);
        method.setAccessible(true);

        String[] blockedPayloads = {
                "--conf 'spark.yarn.queue=`id`'",
                "--conf 'spark.yarn.queue=$(whoami)'",
        };

        for (String payload : blockedPayloads) {
            try {
                method.invoke(handler, payload);
                Assert.fail("Should have thrown IllegalArgumentException for payload: " + payload);
            } catch (java.lang.reflect.InvocationTargetException e) {
                Assert.assertTrue("Payload not blocked: " + payload,
                        e.getCause() instanceof IllegalArgumentException);
            }
        }
    }

    @Test
    public void testCheckCommandInjectionAllowed() throws Exception {
        DefaultSparkBuildJobHandler handler = new DefaultSparkBuildJobHandler();
        Method method = DefaultSparkBuildJobHandler.class.getDeclaredMethod("checkCommandInjection", String.class);
        method.setAccessible(true);

        String[] allowedPayloads = {
                "--conf 'spark.yarn.queue=default' \\\n--conf 'spark.executor.memory=1024m'",
                "--conf 'spark.driver.extraJavaOptions=-Dconfig=value'",
                "--conf 'spark.yarn.queue=normal_queue.v1'",
        };

        for (String payload : allowedPayloads) {
            method.invoke(handler, payload);
        }
    }

    @Test
    public void testGenerateSparkCmdWithMaliciousQueue() throws Exception {
        KylinConfig config = getTestConfig();
        config.setProperty("kylin.engine.spark-conf.spark.master", "local[2]");
        config.setProperty("kylin.engine.spark-conf.spark.yarn.queue", "default'; touch /tmp/pwned; echo '");
        config.setProperty("kylin.engine.spark-conf.spark.executor.memory", "1024m");
        config.setProperty("kylin.env.hadoop-conf-dir", "/dummy");

        SparkAppDescription desc = new SparkAppDescription();
        desc.setHadoopConfDir("/dummy");
        desc.setKylinJobJar("mock.jar");
        desc.setAppArgs("mock-args");
        desc.setJobNamePrefix("test_");
        desc.setJobId("test-job-id");
        desc.setComma(",");
        desc.setSparkJars(Sets.newHashSet("jar1.jar"));
        desc.setSparkFiles(Sets.newHashSet("file1.conf"));

        Map<String, String> sparkConf = Maps.newHashMap();
        sparkConf.put("spark.yarn.queue", "default'; touch /tmp/pwned; echo '");
        sparkConf.put("spark.executor.memory", "1024m");
        desc.setSparkConf(sparkConf);

        ISparkJobHandler handler = new DefaultSparkBuildJobHandler();
        try {
            handler.generateSparkCmd(config, desc);
            Assert.fail("Should have thrown IllegalArgumentException for conf value containing single quote");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("single quote"));
        }
    }
}
