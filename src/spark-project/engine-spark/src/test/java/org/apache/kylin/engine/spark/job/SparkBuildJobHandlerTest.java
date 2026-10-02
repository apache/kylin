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

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTestBase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
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
        SparkSubmitCommand cmd = new SparkSubmitCommand(Arrays.asList("/bin/echo", "spark-submit"), Maps.newHashMap());
        Map<String, String> updateInfo = handler.runSparkSubmit(cmd, "");
        Assert.assertEquals("spark-submit\n", updateInfo.get("output"));
        Assert.assertNotNull(updateInfo.get("process_id"));

    }

    @Test
    public void testAppendSparkConf() {
        DefaultSparkBuildJobHandler handler = new DefaultSparkBuildJobHandler();

        List<String> arguments = Lists.newArrayList();
        handler.appendSparkConf(arguments, "spark.yarn.queue", "normalQueue");
        Assert.assertEquals(Arrays.asList("--conf", "spark.yarn.queue=normalQueue"), arguments);

        arguments.clear();
        handler.appendSparkConf(arguments, "spark.yarn.queue", "default'; touch /tmp/pwned; echo '");
        Assert.assertEquals(Arrays.asList("--conf", "spark.yarn.queue=default'; touch /tmp/pwned; echo '"), arguments);

        arguments.clear();
        handler.appendSparkConf(arguments, "spark.yarn.queue", "a|b");
        Assert.assertEquals(Arrays.asList("--conf", "spark.yarn.queue=a|b"), arguments);
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
        desc.setSparkJars(Sets.newHashSet("jar1.jar;touch /tmp/pwned"));
        desc.setSparkFiles(Sets.newHashSet("file1.conf|cat /tmp/pwned"));

        Map<String, String> sparkConf = Maps.newHashMap();
        sparkConf.put("spark.yarn.queue", "default'; touch /tmp/pwned; echo '");
        sparkConf.put("spark.executor.memory", "1024m");
        desc.setSparkConf(sparkConf);

        ISparkJobHandler handler = new DefaultSparkBuildJobHandler();
        SparkSubmitCommand cmd = (SparkSubmitCommand) handler.generateSparkCmd(config, desc);
        Assert.assertTrue(cmd.getArguments().contains("spark.yarn.queue=default'; touch /tmp/pwned; echo '"));
        Assert.assertTrue(cmd.getArguments().contains("jar1.jar;touch /tmp/pwned"));
        Assert.assertTrue(cmd.getArguments().contains("file1.conf|cat /tmp/pwned"));
    }

    @Test
    public void testGenerateSparkCmdPreservesArgumentOrder() {
        SparkAppDescription desc = new SparkAppDescription();
        desc.setHadoopConfDir("/etc/hadoop");
        desc.setKylinJobJar("job.jar");
        desc.setAppArgs("-className org.example.Main file:/tmp/args.json");
        desc.setJobNamePrefix("job_");
        desc.setJobId("id");
        desc.setComma(",");
        desc.setSparkJars(Sets.newLinkedHashSet(Arrays.asList("a.jar", "b.jar")));
        desc.setSparkFiles(Sets.newLinkedHashSet(Arrays.asList("a.conf", "b.conf")));

        Map<String, String> sparkConf = Maps.newLinkedHashMap();
        sparkConf.put("spark.executor.memory", "1024m");
        sparkConf.put("spark.yarn.queue", "default");
        desc.setSparkConf(sparkConf);

        SparkSubmitCommand cmd = (SparkSubmitCommand) new DefaultSparkBuildJobHandler().generateSparkCmd(getTestConfig(),
                desc);

        Assert.assertEquals(Arrays.asList(KylinConfigBase.getSparkHome() + File.separator + "bin/spark-submit",
                "--class", "org.apache.kylin.engine.spark.application.SparkEntry", "--name", "job_id", "--jars",
                "a.jar,b.jar", "--files", "a.conf,b.conf", "--conf", "spark.executor.memory=1024m", "--conf",
                "spark.yarn.queue=default", "job.jar", "-className", "org.example.Main", "file:/tmp/args.json"),
                cmd.getArguments());
        Assert.assertEquals("/etc/hadoop", cmd.getEnvironment().get("HADOOP_CONF_DIR"));
    }
}
