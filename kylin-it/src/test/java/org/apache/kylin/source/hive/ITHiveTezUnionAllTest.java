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

package org.apache.kylin.source.hive;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.HiveCmdBuilder;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ITHiveTezUnionAllTest extends HBaseMetadataTestCase {
    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void doTests() throws Exception {
        testMaterializeView(true);
        testMaterializeView(false);
    }

    private void testMaterializeView(boolean isDistributeBy) throws Exception {
        KylinConfig config = getTestConfig();

        /**
         * For UT debug
         * config.setProperty("kylin.job.use-remote-cli", "true");
         */

        String viewName = "test_union_all_view";
        String tableName = "test_union_all_table";

        HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        JobEngineConfig jobConf = new JobEngineConfig(config);
        String storagePath = JobBuilderSupport.getJobWorkingDir(jobConf, "it-test") + "/" + tableName;

        StringBuilder testCmd = new StringBuilder();
        testCmd.append("USE " + config.getHiveDatabaseForIntermediateTable() + ";").append("\n");
        testCmd.append("SET hive.execution.engine=tez;");
        testCmd.append("DROP VIEW IF EXISTS " + viewName + ";\n");
        testCmd.append(
                "CREATE VIEW " + viewName + " AS SELECT * FROM test_kylin_fact UNION ALL SELECT * FROM test_kylin_fact")
                .append(";\n");
        testCmd.append("DROP TABLE IF EXISTS " + tableName + ";\n");
        testCmd.append("CREATE TABLE IF NOT EXISTS " + tableName + "\n");
        testCmd.append("LOCATION '" + storagePath + "'\n");
        testCmd.append("AS SELECT * FROM " + viewName + "\n");
        if (isDistributeBy)
            hiveCmdBuilder.addStatementWithRedistributeBy(testCmd);
        else
            hiveCmdBuilder.addStatement(testCmd.toString());

        Path rootPath = new Path(storagePath);
        FileSystem fs = HadoopUtil.getFileSystem(storagePath);

        fs.delete(rootPath, true);
        fs.mkdirs(rootPath);

        config.getCliCommandExecutor().execute(hiveCmdBuilder.build());

        rootPath = fs.makeQualified(rootPath);
        for (FileStatus statsFolder : fs.listStatus(rootPath)) {
            if (isDistributeBy)
                Assert.assertTrue(!statsFolder.isDirectory());
            else
                Assert.assertTrue(statsFolder.isDirectory());
        }

        HiveCmdBuilder cleanupCmdBuilder = new HiveCmdBuilder();
        StringBuilder cleanupCmd = new StringBuilder();
        cleanupCmd.append("USE " + config.getHiveDatabaseForIntermediateTable() + ";").append("\n");
        cleanupCmd.append("DROP VIEW IF EXISTS " + viewName + ";\n");
        cleanupCmd.append("DROP TABLE IF EXISTS " + tableName + ";\n");
        cleanupCmdBuilder.addStatement(cleanupCmd.toString());
        config.getCliCommandExecutor().execute(cleanupCmdBuilder.build());
        fs.delete(rootPath, true);
    }
}
