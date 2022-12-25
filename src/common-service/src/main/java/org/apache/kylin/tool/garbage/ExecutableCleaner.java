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

package org.apache.kylin.tool.garbage;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutableCleaner extends MetadataCleaner {

    private static final Logger logger = LoggerFactory.getLogger(ExecutableCleaner.class);

    public ExecutableCleaner(String project) {
        super(project);
    }

    @Override
    public void cleanup() {

        logger.info("Start to clean executable in project {}", project);

        KylinConfig config = KylinConfig.getInstanceFromEnv();

        long expirationTime = config.getExecutableSurvivalTimeThreshold();

        NExecutableManager executableManager = NExecutableManager.getInstance(config, project);

        List<AbstractExecutable> executables = executableManager.getAllExecutables();
        List<AbstractExecutable> filteredExecutables = executables.stream().filter(job -> {
            if ((System.currentTimeMillis() - job.getCreateTime()) < expirationTime) {
                return false;
            }
            ExecutableState state = job.getStatus();
            return state.isFinalState();
        }).collect(Collectors.toList());

        for (AbstractExecutable executable : filteredExecutables) {
            executableManager.deleteJob(executable.getId());
        }
        logger.info("Clean executable in project {} finished", project);
    }

}
