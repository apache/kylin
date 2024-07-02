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
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.profiler.BuildAsyncProfilerSparkPlugin;
import org.apache.kylin.query.plugin.profiler.QueryAsyncProfilerSparkPlugin;
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
    public void setUp() {
        createTestMetadata();
        modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
    }

    @After
    public void tearDown() {
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

        private void addModel(NDataModel model) {
            for (int i = 0; i < 3; i++) {
                model = modelManager.copyForWrite(model);
                model.setUuid(RandomUtil.randomUUIDStr());
                model.setAlias(RandomUtil.randomUUIDStr());
                model.setMvcc(-1);
                modelManager.createDataModelDesc(model, "owner");
            }
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

        // Spark plugin
        overwriteSystemProp("kylin.engine.async-profiler-enabled", "true");
        {
            val desc = sparkExecutable.getSparkAppDesc();
            desc.setHadoopConfDir(hadoopConf);
            desc.setKylinJobJar(kylinJobJar);
            desc.setAppArgs(appArgs);
            String cmd = (String) sparkExecutable.sparkJobHandler.generateSparkCmd(kylinConfig, desc);

            Assert.assertNotNull(cmd);
            Assert.assertTrue(cmd.contains("spark.plugins=," + BuildAsyncProfilerSparkPlugin.class.getCanonicalName()));
        }

        overwriteSystemProp("kylin.engine.spark-conf.spark.plugins",
                QueryAsyncProfilerSparkPlugin.class.getCanonicalName());
        {
            val desc = sparkExecutable.getSparkAppDesc();
            desc.setHadoopConfDir(hadoopConf);
            desc.setKylinJobJar(kylinJobJar);
            desc.setAppArgs(appArgs);
            String cmd = (String) sparkExecutable.sparkJobHandler.generateSparkCmd(kylinConfig, desc);

            Assert.assertNotNull(cmd);
            Assert.assertTrue(cmd.contains("spark.plugins=" + QueryAsyncProfilerSparkPlugin.class.getCanonicalName()
                    + "," + BuildAsyncProfilerSparkPlugin.class.getCanonicalName()));
        }

        overwriteSystemProp("kylin.engine.async-profiler-enabled", "false");
        {
            val desc = sparkExecutable.getSparkAppDesc();
            desc.setHadoopConfDir(hadoopConf);
            desc.setKylinJobJar(kylinJobJar);
            desc.setAppArgs(appArgs);
            String cmd = (String) sparkExecutable.sparkJobHandler.generateSparkCmd(kylinConfig, desc);

            Assert.assertNotNull(cmd);
            Assert.assertFalse(
                    cmd.contains("spark.plugins=," + BuildAsyncProfilerSparkPlugin.class.getCanonicalName()));
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

    @Test
    public void testDriverProfileExtraJavaOptions() {
        KylinConfig kylinConfig = getTestConfig();
        overwriteSystemProp("KYLIN_HOME", "/kylin");
        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setProject("default");

        kylinConfig.setProperty("kylin.engine.async-profiler-enabled", "false");

        val driverExtraJavaOptions = sparkExecutable.getDriverExtraJavaOptions(kylinConfig);
        Assert.assertFalse(StringUtils.contains(driverExtraJavaOptions, "-Dspark.profiler.flagsDir="));
        Assert.assertFalse(StringUtils.contains(driverExtraJavaOptions, "-Dspark.profiler.collection.timeout="));
        Assert.assertFalse(StringUtils.contains(driverExtraJavaOptions, "-Dspark.profiler.profiling.timeout="));

        kylinConfig.setProperty("kylin.engine.async-profiler-enabled", "true");
        val reWriteDriverExtraJavaOptions = sparkExecutable.getDriverExtraJavaOptions(kylinConfig);
        Assert.assertTrue(StringUtils.contains(reWriteDriverExtraJavaOptions, "-Dspark.profiler.flagsDir="));
        Assert.assertTrue(StringUtils.contains(reWriteDriverExtraJavaOptions, "-Dspark.profiler.collection.timeout="));
        Assert.assertTrue(StringUtils.contains(reWriteDriverExtraJavaOptions, "-Dspark.profiler.profiling.timeout="));
    }

    @Test
    public void testGetKylinConfigExt() {
        KylinConfig kylinConfig = KylinConfig.createKylinConfig(getTestConfig());
        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setProject("default");
        kylinConfig.setProperty("kylin.engine.spark-conf.test", "123");

        var kylinConfigExt = sparkExecutable.getKylinConfigExt(kylinConfig, "default");
        Assert.assertEquals("123", kylinConfigExt.getOptional("kylin.engine.spark-conf.test", null));

        sparkExecutable.setParam(NBatchConstants.P_DATAFLOW_ID, RandomUtil.randomUUIDStr());
        kylinConfigExt = sparkExecutable.getKylinConfigExt(kylinConfig, "default");
        Assert.assertEquals("123", kylinConfigExt.getOptional("kylin.engine.spark-conf.test", null));

        sparkExecutable.setParam(NBatchConstants.P_DATAFLOW_ID, "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        kylinConfigExt = sparkExecutable.getKylinConfigExt(kylinConfig, "default");
        Assert.assertEquals("123", kylinConfigExt.getOptional("kylin.engine.spark-conf.test", null));
    }
}
