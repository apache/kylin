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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

public class SparkCleanupTransactionalTableStepTest extends NLocalWithSparkSessionTest {

    @Test
    public void testGenerateDropTableCommand() {
        SparkCleanupTransactionalTableStep sparkCleanupTransactionalTableStep = new SparkCleanupTransactionalTableStep();
        String expectResult = "USE `TEST_CDP`;\nDROP TABLE IF EXISTS `TEST_HIVE_TX_INTERMEDIATE5c5851ef8544`;\n";
        String tableFullName = "TEST_CDP.TEST_HIVE_TX_INTERMEDIATE5c5851ef8544";
        String cmd = sparkCleanupTransactionalTableStep.generateDropTableCommand(tableFullName);
        Assert.assertEquals(expectResult, cmd);

        expectResult = "DROP TABLE IF EXISTS `TEST_HIVE_TX_INTERMEDIATE5c5851ef8544`;\n";
        tableFullName = "TEST_HIVE_TX_INTERMEDIATE5c5851ef8544";
        cmd = sparkCleanupTransactionalTableStep.generateDropTableCommand(tableFullName);
        Assert.assertEquals(expectResult, cmd);

        expectResult = "";
        tableFullName = "";
        cmd = sparkCleanupTransactionalTableStep.generateDropTableCommand(tableFullName);
        Assert.assertEquals(expectResult, cmd);
    }

    @Test
    public void testDoWork() {
        SparkCleanupTransactionalTableStep step = new SparkCleanupTransactionalTableStep(0);
        ExecutableContext context = new ExecutableContext(Maps.newConcurrentMap(), Maps.newConcurrentMap(),
                getTestConfig(), 0);
        step.setProject("SSB");

        try {
            createHDFSFile();
            step.doWork(context);
        } catch (ExecuteException e) {
            Assert.assertEquals("Can not delete intermediate table", e.getMessage());
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void createHDFSFile() throws IOException {
        KylinConfig config = getTestConfig();
        String dir = config.getJobTmpTransactionalTableDir("SSB", null);
        Path path = new Path(dir);
        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
        if (!fileSystem.exists(path)) {
            fileSystem.mkdirs(path);
            fileSystem.setPermission(path, new FsPermission((short) 00777));
            path = new Path(dir + "/TEST_CDP.TEST_HIVE_TX_INTERMEDIATE5c5851ef8544");
            fileSystem.createNewFile(path);
        }
    }
}
