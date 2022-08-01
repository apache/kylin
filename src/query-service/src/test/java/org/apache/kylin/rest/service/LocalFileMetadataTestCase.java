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
package org.apache.kylin.rest.service;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.engine.spark.ExecutableUtils;
import org.junit.Before;

import lombok.val;

/**
 *
 **/
public class LocalFileMetadataTestCase extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        ExecutableUtils.initJobFactory();
    }

    protected List<AbstractExecutable> getRunningExecutables(String project, String model) {
        return NExecutableManager.getInstance(getTestConfig(), project).getRunningExecutables(project, model);
    }

    protected void deleteJobByForce(AbstractExecutable executable) {
        val exManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        exManager.updateJobOutput(executable.getId(), ExecutableState.DISCARDED);
        exManager.deleteJob(executable.getId());
    }

}
