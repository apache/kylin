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

package org.apache.kylin.rec;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.io.Files;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SmartDemoTest extends AbstractTestCase {
    private static final String TEST_META_BASE = "src/test/resources/nsmart/";

    @After
    public void afterClass() {
        KylinConfig.destroyInstance();
    }

    @Test
    public void testE2E_LearnKylin() throws Exception {
        testInternal(TEST_META_BASE + "learn_kylin/meta", "learn_kylin", TEST_META_BASE + "learn_kylin/sql");
    }

    @Test
    public void testE2E_SSB() throws Exception {
        testInternal(TEST_META_BASE + "ssb/meta", "ssb", TEST_META_BASE + "ssb/sql");
    }

    @Test
    public void testE2E_TPCH_LineItem() throws Exception {
        testInternal(TEST_META_BASE + "tpch/meta", "tpch", TEST_META_BASE + "tpch/sql_tmp");
    }

    @Test
    public void testE2E_Airline() throws Exception {
        testInternal(TEST_META_BASE + "airline/meta", "airline", TEST_META_BASE + "airline/sql");
    }

    @Test
    public void testE2E_TPCDS() throws Exception {
        testInternal(TEST_META_BASE + "tpcds/meta", "TPC_DS_2", TEST_META_BASE + "tpcds/sql_ss");
    }

    private void testInternal(String metaDir, String projectName, String sqlDir) throws Exception {
        List<String> sqlList = Lists.newArrayList();
        if (sqlDir != null) {
            File sqlFile = new File(sqlDir);
            if (sqlFile.isDirectory()) {
                File[] sqlFiles = sqlFile.listFiles();
                Preconditions.checkArgument(sqlFiles != null && sqlFiles.length > 0,
                        "SQL files not found under " + sqlFile.getAbsolutePath());

                for (File file : sqlFiles) {
                    sqlList.add(FileUtils.readFileToString(file, Charset.defaultCharset()));
                }
            } else if (sqlFile.isFile()) {
                try (InputStream os = java.nio.file.Files.newInputStream(sqlFile.toPath());
                        BufferedReader br = new BufferedReader(new InputStreamReader(os, Charset.defaultCharset()))) {
                    String line;
                    StringBuilder sb = new StringBuilder();
                    while ((line = br.readLine()) != null) {
                        if (line.endsWith(";")) {
                            sb.append(line);
                            sb.deleteCharAt(sb.length() - 1);
                            sqlList.add(sb.toString());
                            sb = new StringBuilder();
                        } else {
                            sb.append(line);
                            sb.append("\n");
                        }
                    }
                }
            }
        }

        File tmpHome = Files.createTempDir();
        File tmpMeta = new File(tmpHome, "metadata");
        overwriteSystemProp("KYLIN_HOME", tmpHome.getAbsolutePath());
        FileUtils.copyDirectory(new File(metaDir), tmpMeta);
        FileUtils.touch(new File(tmpHome.getAbsolutePath() + "/kylin.properties"));
        KylinConfig.setKylinConfigForLocalTest(tmpHome.getCanonicalPath());
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.query.security.acl-tcr-enabled", "false");
        kylinConfig.setProperty("kylin.smart.conf.propose-runner-type", "in-memory");
        kylinConfig.setProperty("kylin.env", "UT");
        Class.forName("org.h2.Driver");

        try (SetAndUnsetThreadLocalConfig ignored = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig)) {
            AccelerationUtil.runWithSmartContext(kylinConfig, projectName, sqlList.toArray(new String[0]), true);
            NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, projectName);
            Assert.assertFalse(dataflowManager.listUnderliningDataModels().isEmpty());
            log.info("Number of models: {}", dataflowManager.listUnderliningDataModels().size());

            NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, projectName);
            Assert.assertFalse(indexPlanManager.listAllIndexPlans().isEmpty());
            log.info("Number of cubes: {}", indexPlanManager.listAllIndexPlans().size());
        }

        FileUtils.forceDelete(tmpMeta);
    }
}
