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

import java.util.Objects;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import lombok.val;
import lombok.var;

public class NSparkExecutableTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private NDataModelManager modelManager;

    @Before
    public void setup() {
        createTestMetadata();
        modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void attachMetadataAndKylinProps() throws Exception {
        KylinConfig config = getTestConfig();
        val junitFolder = temporaryFolder.getRoot();
        val path = junitFolder.getAbsolutePath();
        MockSparkTestExecutable executable = new MockSparkTestExecutable();
        executable.setMetaUrl(path);
        executable.setProject("default");
        Assert.assertEquals(8, executable.getMetadataDumpList(config).size());
        NDataModel model = modelManager.getDataModelDesc("82fa7671-a935-45f5-8779-85703601f49a");
        for (int i = 0; i < 10; i++) {
            new Thread(new AddModelRunner(model)).start();
        }
        executable.attachMetadataAndKylinProps(config);
        Assert.assertEquals(2, Objects.requireNonNull(junitFolder.listFiles()).length);
    }

    private void addModel(NDataModel model) {
        for (int i = 0; i < 3; i++) {
            model = modelManager.copyForWrite(model);
            model.setUuid(RandomUtil.randomUUIDStr());
            model.setAlias(RandomUtil.randomUUIDStr());
            model.setMvcc(-1);
            modelManager.createDataModelDesc(model, "owner");
        }
    }

    @Test
    public void testGenerateSparkCmd() {
        KylinConfig kylinConfig = getTestConfig();
        overwriteSystemProp("KYLIN_HOME", "/kylin");

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setProject("default");
        String hadoopConf = System.getProperty("KYLIN_HOME") + "/hadoop";
        String kylinJobJar = System.getProperty("KYLIN_HOME") + "/lib/job.jar";
        String appArgs = "/tmp/output";

        overwriteSystemProp("kylin.engine.spark.job-jar", kylinJobJar);
        {
            val desc = sparkExecutable.getSparkAppDesc();
            desc.setHadoopConfDir(hadoopConf);
            desc.setKylinJobJar(kylinJobJar);
            desc.setAppArgs(appArgs);
            String cmd = (String) sparkExecutable.sparkJobHandler.generateSparkCmd(kylinConfig, desc);

            Assert.assertNotNull(cmd);
            Assert.assertTrue(cmd.contains("spark-submit"));
            Assert.assertTrue(
                    cmd.contains("log4j.configurationFile=file:" + kylinConfig.getLogSparkDriverPropertiesFile()));
            Assert.assertTrue(cmd.contains("spark.executor.extraClassPath=job.jar"));
            Assert.assertTrue(cmd.contains("spark.driver.log4j.appender.hdfs.File="));
            Assert.assertTrue(cmd.contains("kylin.hdfs.working.dir="));
        }

        overwriteSystemProp("kylin.engine.extra-jars-path", "/this_new_path.jar");
        {
            val desc = sparkExecutable.getSparkAppDesc();
            desc.setHadoopConfDir(hadoopConf);
            desc.setKylinJobJar(kylinJobJar);
            desc.setAppArgs(appArgs);
            String cmd = (String) sparkExecutable.sparkJobHandler.generateSparkCmd(kylinConfig, desc);

            Assert.assertNotNull(cmd);
            Assert.assertTrue(cmd.contains("/this_new_path.jar"));
        }

        overwriteSystemProp("kylin.engine.spark-conf.spark.driver.extraJavaOptions",
                "'`touch /tmp/foo.bar` $(touch /tmp/foo.bar)'");
        {
            try {
                val desc = sparkExecutable.getSparkAppDesc();
                desc.setHadoopConfDir(hadoopConf);
                desc.setKylinJobJar(kylinJobJar);
                desc.setAppArgs(appArgs);
                String cmd = (String) sparkExecutable.sparkJobHandler.generateSparkCmd(kylinConfig, desc);
            } catch (IllegalArgumentException iae) {
                Assert.assertTrue(iae.getMessage().contains("Not allowed to specify injected command"));
            }
        }
    }

    @Test
    public void testPlatformZKEnable() {
        KylinConfig kylinConfig = getTestConfig();
        overwriteSystemProp("KYLIN_HOME", "/kylin");

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setProject("default");

        var driverExtraJavaOptions = sparkExecutable.getDriverExtraJavaOptions(kylinConfig);
        Assert.assertFalse(StringUtils.contains(driverExtraJavaOptions, "-Djava.security.auth.login.config="));

        kylinConfig.setProperty("kylin.kerberos.enabled", "true");
        driverExtraJavaOptions = sparkExecutable.getDriverExtraJavaOptions(kylinConfig);
        Assert.assertTrue(StringUtils.contains(driverExtraJavaOptions, "-Djava.security.auth.login.config="));

        kylinConfig.setProperty("kylin.env.zk-kerberos-enabled", "false");
        driverExtraJavaOptions = sparkExecutable.getDriverExtraJavaOptions(kylinConfig);
        Assert.assertFalse(StringUtils.contains(driverExtraJavaOptions, "-Djava.security.auth.login.config="));
    }

    class AddModelRunner implements Runnable {

        private final NDataModel model;

        AddModelRunner(NDataModel model) {
            this.model = model;
        }

        @Override
        public void run() {
            UnitOfWork.doInTransactionWithRetry(() -> {
                addModel(model);
                Thread.sleep(new Random().nextInt(50));
                return null;
            }, "default");
        }
    }
}
