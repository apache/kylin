/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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

package io.kyligence.kap.engine.spark.job;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Random;
import java.util.UUID;

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
        Assert.assertEquals(6, executable.getMetadataDumpList(config).size());
        NDataModel model = modelManager.getDataModelDesc("82fa7671-a935-45f5-8779-85703601f49a");
        for (int i = 0; i < 10; i++) {
            new Thread(new AddModelRunner(model)).start();
        }
        executable.attachMetadataAndKylinProps(config);
        Assert.assertEquals(2, junitFolder.listFiles().length);
        File modelFiles = new File(path + "/default/model_desc");
        Assert.assertTrue(modelFiles.listFiles().length % 3 == 0);
    }

    class AddModelRunner implements Runnable {

        private NDataModel model;

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

    private void addModel(NDataModel model) {
        for (int i = 0; i < 3; i++) {
            model = modelManager.copyForWrite(model);
            model.setUuid(UUID.randomUUID().toString());
            model.setAlias(UUID.randomUUID().toString());
            model.setMvcc(-1);
            modelManager.createDataModelDesc(model, "owner");
        }
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
            Assert.assertTrue(cmd.contains("spark-submit"));
            Assert.assertTrue(cmd.contains("log4j.configuration=file:" + kylinConfig.getLogSparkDriverPropertiesFile()));
            Assert.assertTrue(cmd.contains("spark.executor.extraClassPath=job.jar"));
            Assert.assertTrue(cmd.contains("spark.driver.log4j.appender.hdfs.File="));
            Assert.assertTrue(cmd.contains("kap.hdfs.working.dir="));
        } finally {
            if (StringUtils.isEmpty(kylinHome)) {
                System.clearProperty("KYLIN_HOME");
            } else {
                System.setProperty("KYLIN_HOME", kylinHome);
            }
        }
    }
}