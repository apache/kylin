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

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.job.exception.SchedulerException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NSparkExecutableTest extends LocalWithSparkSessionTest {

    @Before
    public void setup() throws SchedulerException {
        super.setup();
    }

    @After
    public void destroy() {
        super.after();
    }

    @Test
    public void testGenerateSparkCmd() {
        KylinConfig kylinConfig = getTestConfig();
        final String kylinHome = System.getProperty("KYLIN_HOME");
        try {
            System.setProperty("KYLIN_HOME", "/kylin");

            NSparkExecutable sparkExecutable = new NSparkExecutable();
            sparkExecutable.setProject("default");

            String hadoopConf = System.getProperty("KYLIN_HOME") + "/hadoop";
            String kylinJobJar = System.getProperty("KYLIN_HOME") + "/lib/job.jar";
            String appArgs = "/tmp/output";

            String cmd = sparkExecutable.generateSparkCmd(kylinConfig, hadoopConf, kylinJobJar, kylinJobJar, appArgs);
            Assert.assertNotNull(cmd);
            Assert.assertTrue(cmd.contains("export HADOOP_CONF_DIR"));
            Assert.assertTrue(cmd.contains("spark-submit"));
            Assert.assertTrue(cmd.contains("spark.executor.extraClassPath=job.jar"));
            Assert.assertTrue(cmd.contains("log4j.configuration="));
        } finally {
            if (StringUtils.isEmpty(kylinHome)) {
                System.clearProperty("KYLIN_HOME");
            } else {
                System.setProperty("KYLIN_HOME", kylinHome);
            }
        }
    }
}