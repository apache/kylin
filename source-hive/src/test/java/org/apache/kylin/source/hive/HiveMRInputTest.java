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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HiveMRInputTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @Test
    public void TestGetJobWorkingDir() throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path jobWorkDirPath = null;
        KylinConfig kylinConfig = mock(KylinConfig.class);
        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig)) {
            when(kylinConfig.getHiveTableDirCreateFirst()).thenReturn(true);
            when(kylinConfig.getHdfsWorkingDirectory()).thenReturn("/tmp/kylin/");
            DefaultChainedExecutable defaultChainedExecutable = mock(DefaultChainedExecutable.class);
            defaultChainedExecutable.setId(RandomUtil.randomUUID().toString());

            String jobWorkingDir = HiveInputBase.getJobWorkingDir(defaultChainedExecutable,
                    KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory());
            jobWorkDirPath = new Path(jobWorkingDir);
            Assert.assertTrue(fileSystem.exists(jobWorkDirPath));
        } finally {
            if (jobWorkDirPath != null)
                fileSystem.deleteOnExit(jobWorkDirPath);
        }
    }

    @Test
    public void testMaterializeViewHql() {
        final int viewSize = 2;
        String[] mockedViewNames = { "mockedView1", "mockedView2" };
        String[] mockedTalbeNames = { "`mockedTable1`", "`mockedTable2`" };
        String mockedWorkingDir = "mockedWorkingDir";

        StringBuilder hqls = new StringBuilder();
        for (int i = 0; i < viewSize; i++) {
            String hql = HiveInputBase.materializeViewHql(mockedViewNames[i], mockedTalbeNames[i], mockedWorkingDir);
            hqls.append(hql);
        }

        for (String sub : StringUtil.splitAndTrim(hqls.toString(), "\n")) {
            Assert.assertTrue(sub.endsWith(";"));
        }

        Assert.assertEquals("DROP TABLE IF EXISTS `mockedView1`;\n"
                + "CREATE TABLE IF NOT EXISTS `mockedView1` LIKE `mockedTable1` LOCATION 'mockedWorkingDir/mockedView1';\n"
                + "ALTER TABLE `mockedView1` SET TBLPROPERTIES('auto.purge'='true');\n"
                + "INSERT OVERWRITE TABLE `mockedView1` SELECT * FROM `mockedTable1`;\n"
                + "DROP TABLE IF EXISTS `mockedView2`;\n"
                + "CREATE TABLE IF NOT EXISTS `mockedView2` LIKE `mockedTable2` LOCATION 'mockedWorkingDir/mockedView2';\n"
                + "ALTER TABLE `mockedView2` SET TBLPROPERTIES('auto.purge'='true');\n"
                + "INSERT OVERWRITE TABLE `mockedView2` SELECT * FROM `mockedTable2`;\n",
                hqls.toString());
    }

}