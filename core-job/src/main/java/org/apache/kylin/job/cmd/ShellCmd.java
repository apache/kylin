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

package org.apache.kylin.job.cmd;

import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.job.constant.JobStepStatusEnum;
import org.apache.kylin.job.exception.JobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * @author xjiang
 * 
 */
public class ShellCmd implements IJobCommand {

    private static Logger log = LoggerFactory.getLogger(ShellCmd.class);

    private final String executeCommand;
    private final ICommandOutput output;
    private final boolean isAsync;
    private final CliCommandExecutor cliCommandExecutor;

    private FutureTask<Integer> future;

    private ShellCmd(String executeCmd, ICommandOutput out, String host, int port, String user, String password, boolean async) {
        this.executeCommand = executeCmd;
        this.output = out;
        this.cliCommandExecutor = new CliCommandExecutor();
        this.cliCommandExecutor.setRunAtRemote(host, port, user, password);
        this.isAsync = async;
    }

    public ShellCmd(String executeCmd, String host, int port, String user, String password, boolean async) {
        this(executeCmd, new ShellCmdOutput(), host, port, user, password, async);
    }

    @Override
    public ICommandOutput execute() throws JobException {

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        future = new FutureTask<Integer>(new Callable<Integer>() {
            public Integer call() throws JobException, IOException {
                executor.shutdown();
                return executeCommand(executeCommand);
            }
        });
        executor.execute(future);

        int exitCode = -1;
        if (!isAsync) {
            try {
                exitCode = future.get();
                log.info("finish executing");
            } catch (CancellationException e) {
                log.debug("Command is cancelled");
                exitCode = -2;
            } catch (Exception e) {
                throw new JobException("Error when execute job " + executeCommand, e);
            } finally {
                if (exitCode == 0) {
                    output.setStatus(JobStepStatusEnum.FINISHED);
                } else if (exitCode == -2) {
                    output.setStatus(JobStepStatusEnum.DISCARDED);
                } else {
                    output.setStatus(JobStepStatusEnum.ERROR);
                }
                output.setExitCode(exitCode);
            }
        }
        return output;
    }

    protected int executeCommand(String command) throws JobException, IOException {
        output.reset();
        output.setStatus(JobStepStatusEnum.RUNNING);
        return cliCommandExecutor.execute(command, output).getFirst();
    }

    @Override
    public void cancel() throws JobException {
        future.cancel(true);
    }

}
