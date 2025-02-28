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

package org.apache.kylin.rec.runner;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.ShellException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ForkBasedJobRunner extends JobRunnerFactory.AbstractJobRunner {

    private final CliCommandExecutor cliExecutor = new CliCommandExecutor();

    public ForkBasedJobRunner(KylinConfig kylinConfig, String project, List<String> originResources) {
        super(kylinConfig, project, originResources);
    }

    @Override
    protected void doExecute(ExecutableApplication app, Map<String, String> args) throws ShellException {
        String finalCommand = String.format(Locale.ROOT,
                "export SPARK_LOCAL=true && bash -x %s/sbin/bootstrap.sh %s %s 2>>%s", KylinConfigBase.getKylinHome(),
                app.getClass().getName(), formatArgs(args), getJobTmpDir() + "/job.log");
        log.info("Try to execute {}", finalCommand);
        cliExecutor.execute(finalCommand, new BufferedLogger(log), jobId);
    }

}
