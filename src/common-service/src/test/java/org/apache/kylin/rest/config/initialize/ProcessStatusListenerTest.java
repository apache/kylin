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

package org.apache.kylin.rest.config.initialize;

import static org.awaitility.Awaitility.await;

import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.ProcessUtils;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.execution.ExecutableManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class ProcessStatusListenerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
        getTestConfig().setProperty("kylin.engine.spark.cluster-manager-class-name",
                "org.apache.kylin.rest.config.initialize.MockClusterManager");
    }

    @After
    public void tearDown() {
        EventBusFactory.getInstance().restart();
        cleanupTestMetadata();
    }

    @Test
    public void testKillProcess() {
        EventBusFactory.getInstance().register(ProcessStatusListener.getInstance(), true);
        val executableManager = ExecutableManager.getInstance(getTestConfig(), "default");
        final String jobId = "job000000001";
        final String execCmd = "nohup sleep 30 & sleep 30";

        Thread execThread = new Thread(() -> {
            CliCommandExecutor exec = new CliCommandExecutor();
            try {
                exec.execute(execCmd, null, jobId);
            } catch (ShellException e) {
                // do nothing
                e.printStackTrace();
            }
        });
        execThread.start();

        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            val jobMap = ProcessStatusListener.parseProcessFile();
            val pid = jobMap.entrySet().stream().filter(entry -> entry.getValue().equals(jobId)).map(Map.Entry::getKey)
                    .findFirst();
            Assert.assertTrue(pid.isPresent());
            Assert.assertTrue(ProcessUtils.isAlive(pid.get()));
        });

        overwriteSystemProp("KYLIN_HOME", Paths.get(System.getProperty("user.dir")).getParent().getParent() + "/build");
        executableManager.destroyProcess(jobId);

        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            val jobMap = ProcessStatusListener.parseProcessFile();
            val pid = jobMap.entrySet().stream().filter(entry -> entry.getValue().equals(jobId)).map(Map.Entry::getKey)
                    .findFirst();
            Assert.assertFalse(pid.isPresent());
        });

    }
}
