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

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.exception.ExecuteException;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.job.ISparkJobHandler;

public class SparkBuildJobHandlerTest extends NLocalWithSparkSessionTest {

    @Test
    public void testKillOrphanApplicationIfExists() {
        KylinConfig config = getTestConfig();
        ISparkJobHandler handler = (ISparkJobHandler) ClassUtil.newInstance(config.getSparkBuildJobHandlerClassName());
        Assert.assertTrue(handler instanceof DefaultSparkBuildJobHandler);
        Map<String, String> sparkConf = Maps.newHashMap();
        String jobStepId = "testId";
        handler.killOrphanApplicationIfExists(getProject(), jobStepId, config, sparkConf);
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
}
