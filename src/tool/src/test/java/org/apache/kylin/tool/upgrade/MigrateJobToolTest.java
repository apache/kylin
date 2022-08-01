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

package org.apache.kylin.tool.upgrade;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class MigrateJobToolTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata("src/test/resources/ut_upgrade_tool");
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void test() {
        // execute before
        NExecutableManager executableManager = NExecutableManager.getInstance(getTestConfig(), "version40");

        List<AbstractExecutable> executeJobs = executableManager.getAllExecutables().stream()
                .filter(executable -> JobTypeEnum.INC_BUILD == executable.getJobType()
                        || JobTypeEnum.INDEX_BUILD == executable.getJobType()
                        || JobTypeEnum.INDEX_REFRESH == executable.getJobType()
                        || JobTypeEnum.INDEX_MERGE == executable.getJobType())
                .filter(executable -> ExecutableState.RUNNING == executable.getStatus()
                        || ExecutableState.ERROR == executable.getStatus()
                        || ExecutableState.PAUSED == executable.getStatus())
                .collect(Collectors.toList());

        Assert.assertEquals(1, executeJobs.size());

        val tool = new MigrateJobTool();

        tool.execute(new String[] { "-dir", getTestConfig().getMetadataUrl().toString() });

        getTestConfig().clearManagersByProject("version40");
        getTestConfig().clearManagers();
        ResourceStore.clearCache();

        executableManager = NExecutableManager.getInstance(getTestConfig(), "version40");

        executeJobs = executableManager.getAllExecutables().stream()
                .filter(executable -> JobTypeEnum.INC_BUILD == executable.getJobType()
                        || JobTypeEnum.INDEX_BUILD == executable.getJobType()
                        || JobTypeEnum.INDEX_REFRESH == executable.getJobType()
                        || JobTypeEnum.SUB_PARTITION_REFRESH == executable.getJobType()
                        || JobTypeEnum.INDEX_MERGE == executable.getJobType())
                .filter(executable -> ExecutableState.RUNNING == executable.getStatus()
                        || ExecutableState.ERROR == executable.getStatus()
                        || ExecutableState.PAUSED == executable.getStatus())
                .collect(Collectors.toList());

        Assert.assertEquals(1, executeJobs.size());

        for (AbstractExecutable executeJob : executeJobs) {
            DefaultChainedExecutableOnModel job = (DefaultChainedExecutableOnModel) executeJob;
            Assert.assertEquals(3, job.getTasks().size());

            switch (job.getJobType()) {
            case INDEX_BUILD:
                Assert.assertEquals("org.apache.kylin.engine.spark.job.ExecutableAddCuboidHandler",
                        job.getHandler().getClass().getName());
                break;
            case INC_BUILD:
                Assert.assertEquals("org.apache.kylin.engine.spark.job.ExecutableAddSegmentHandler",
                        job.getHandler().getClass().getName());
                break;
            case INDEX_REFRESH:
            case SUB_PARTITION_REFRESH:
            case INDEX_MERGE:
                Assert.assertEquals("org.apache.kylin.engine.spark.job.ExecutableMergeOrRefreshHandler",
                        job.getHandler().getClass().getName());
                break;
            default:
                break;
            }
        }

    }
}
