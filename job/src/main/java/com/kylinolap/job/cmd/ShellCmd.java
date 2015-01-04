/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.job.cmd;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.exception.JobException;

/**
 * @author xjiang
 * 
 * FIXME should reuse common.util.SSHClient
 */
public class ShellCmd implements IJobCommand {

    private static Logger log = LoggerFactory.getLogger(ShellCmd.class);

    private final String executeCommand;
    private final ICommandOutput output;
    private final String remoteHost;
    private final String remoteUser;
    private final String remotePassword;
    private final String identityPath;
    private final boolean isAsync;

    private FutureTask<Integer> future;

    protected ShellCmd(String executeCmd, ICommandOutput out, String host, String user, String password, boolean async) {
        this.executeCommand = executeCmd;
        this.output = out;
        this.remoteHost = host;
        this.remoteUser = user;
        if (password != null && new File(password).exists()) {
            this.identityPath = new File(password).getAbsolutePath();
            this.remotePassword = null;
        } else {
            this.remotePassword = password;
            this.identityPath = null;
        }
        this.isAsync = async;
    }

    public ShellCmd(String executeCmd, String host, String user, String password, boolean async) {
        this(executeCmd, new ShellCmdOutput(), host, user, password, async);
    }

    @Override
    public ICommandOutput execute() throws JobException {

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        future = new FutureTask<Integer>(new Callable<Integer>() {
            public Integer call() throws JobException {
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
                throw new JobException("Error when exectute job " + executeCommand, e);
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

    protected int executeCommand(String command) throws JobException {
        output.reset();
        if (remoteHost != null) {
            log.debug("Executing remote cmd: " + command);
            return remoteExec(command);
        } else {
            log.debug("Executing local cmd: " + command);
            return localExec(command);
        }
    }

    private int localExec(String command) throws JobException {
        output.setStatus(JobStepStatusEnum.RUNNING);
        String[] cmd = new String[3];
        cmd[0] = "/bin/bash";
        cmd[1] = "-c";
        cmd[2] = command;

        BufferedReader reader = null;
        int exitCode = -1;
        try {
            ProcessBuilder builder = new ProcessBuilder(cmd);
            builder.redirectErrorStream(true);
            Process proc = builder.start();

            reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line = null;
            while ((line = reader.readLine()) != null) {
                output.appendOutput(line);
            }

            exitCode = proc.waitFor();
        } catch (Exception e) {
            throw new JobException(e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new JobException(e);
                }
            }
        }
        return exitCode;
    }

    private int remoteExec(String command) throws JobException {
        output.setStatus(JobStepStatusEnum.RUNNING);
        Session session = null;
        Channel channel = null;
        int exitCode = -1;
        try {
            JSch jsch = new JSch();
            if (identityPath != null) {
                jsch.addIdentity(identityPath);
            }

            session = jsch.getSession(remoteUser, remoteHost, 22);
            if (remotePassword != null) {
                session.setPassword(remotePassword);
            }
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();

            channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            channel.setInputStream(null);
            PipedInputStream in = new PipedInputStream(64 * 1024);
            PipedOutputStream out = new PipedOutputStream(in);
            channel.setOutputStream(out);
            ((ChannelExec) channel).setErrStream(out); // redirect error to out
            channel.connect();

            byte[] tmp = new byte[1024];
            while (true) {
                while (in.available() > 0) {
                    int i = in.read(tmp, 0, 1024);
                    if (i < 0)
                        break;
                    output.appendOutput(new String(tmp, 0, i));
                }
                if (channel.isClosed()) {
                    if (in.available() > 0) {
                        continue;
                    }
                    exitCode = channel.getExitStatus();
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception ee) {
                    throw ee;
                }
            }
        } catch (Exception e) {
            throw new JobException(e);
        } finally {
            if (channel != null) {
                channel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }
        return exitCode;
    }

    @Override
    public void cancel() throws JobException {
        future.cancel(true);
    }

    public static void main(String[] args) throws JobException {
        ShellCmdOutput output = new ShellCmdOutput();
        ShellCmd shellCmd = new ShellCmd(args[0], output, args[1], args[2], args[3], false);
        shellCmd.execute();

        System.out.println("============================================================================");
        System.out.println(output.getExitCode());
        System.out.println(output.getOutput());
    }
}
