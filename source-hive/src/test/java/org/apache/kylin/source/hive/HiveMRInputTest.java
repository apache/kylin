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
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.junit.Assert;
import org.junit.Test;

public class HiveMRInputTest {

    @Test
    public void TestGetJobWorkingDir() throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        Path jobWorkDirPath = null;
        KylinConfig kylinConfig = mock(KylinConfig.class);
        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig)) {
            when(kylinConfig.getHiveTableDirCreateFirst()).thenReturn(true);
            when(kylinConfig.getHdfsWorkingDirectory()).thenReturn("/tmp/kylin/");
            DefaultChainedExecutable defaultChainedExecutable = mock(DefaultChainedExecutable.class);
            defaultChainedExecutable.setId(UUID.randomUUID().toString());

            HiveMRInput.BatchCubingInputSide batchCubingInputSide = new HiveMRInput.BatchCubingInputSide(null);
            String jobWorkingDir = batchCubingInputSide.getJobWorkingDir(defaultChainedExecutable);
            jobWorkDirPath = new Path(jobWorkingDir);
            Assert.assertTrue(fileSystem.exists(jobWorkDirPath));
        } finally {
            if (jobWorkDirPath != null)
                fileSystem.deleteOnExit(jobWorkDirPath);
        }
    }

}