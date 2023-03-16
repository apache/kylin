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

package org.apache.kylin.common.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.val;

/**
 * @author yangli9
 */
public class CliCommandExecutor {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CliCommandExecutor.class);

    private String remoteHost;
    private int port;
    private String remoteUser;
    private String remotePwd;
    private String localIdentityPath;
    private static final int REMOTE_TIMEOUT_SECONDS = 3600;

    // records down child process ids

    public CliCommandExecutor() {
    }

    public CliCommandExecutor(String remoteHost, String remoteUser, String remotePwd) {
        this.remoteHost = remoteHost;
        this.remoteUser = remoteUser;
        this.remotePwd = remotePwd;
        this.port = 22;
    }

    public CliCommandExecutor(String remoteHost, String remoteUser, String remotePwd, String localIdentityPath,
            int port) {
        this.remoteHost = remoteHost;
        this.remoteUser = remoteUser;
        this.remotePwd = remotePwd;
        this.localIdentityPath = localIdentityPath;
        this.port = port;
    }

    public CliCommandExecutor(String remoteHost, String remoteUser, String remotePwd, String localIdentityPath) {
        this(remoteHost, remoteUser, remotePwd);
        this.localIdentityPath = localIdentityPath;
    }

    public void setRunAtRemote(String host, int port, String user, String pwd) {
        this.remoteHost = host;
        this.port = port;
        this.remoteUser = user;
        this.remotePwd = pwd;
    }

    public void setRunAtLocal() {
        this.remoteHost = null;
        this.remoteUser = null;
        this.remotePwd = null;
    }

    public void setLocalIdentityPath(String localIdentityPath) {
        this.localIdentityPath = localIdentityPath;
    }

    public void copyFile(String localFile, String destDir) throws IOException {
        if (remoteHost == null)
            copyNative(localFile, destDir);
        else
            copyLocalToRemote(localFile, destDir);
    }

    private void copyNative(String localFile, String destDir) throws IOException {
        File src = new File(localFile);
        File dest = new File(destDir, src.getName());
        FileUtils.copyFile(src, dest);
    }

    private void copyLocalToRemote(String localFile, String destDir) throws IOException {
        SSHClient ssh = getSshClient();
        try {
            ssh.scpFileToRemote(localFile, destDir);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    public void copyRemoteToLocal(String remoteFile, String destDir) throws IOException {
        SSHClient ssh = getSshClient();
        try {
            ssh.scpRemoteFileToLocal(remoteFile, destDir);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @VisibleForTesting
    public SSHClient getSshClient() {
        return new SSHClient(remoteHost, port, remoteUser, remotePwd, localIdentityPath);
    }

    public CliCmdExecResult execute(String command, Logger logAppender) throws ShellException {
        return execute(command, logAppender, null);
    }

    public CliCmdExecResult execute(String command, Logger logAppender, String jobId) throws ShellException {
        CliCmdExecResult r;
        if (remoteHost == null) {
            r = runNativeCommand(command, logAppender, jobId);
        } else {
            val remoteResult = runRemoteCommand(command, logAppender);
            r = new CliCmdExecResult(remoteResult.getFirst(), remoteResult.getSecond(), null);
        }

        if (r.getCode() != 0)
            throw new ShellException("OS command error exit with return code: " + r.getCode() //
                    + ", error message: " + r.getCmd() + "The command is: \n" + command
                    + (remoteHost == null ? "" : " (remoteHost:" + remoteHost + ")") //
            );

        return r;
    }

    private Pair<Integer, String> runRemoteCommand(String command, Logger logAppender) throws ShellException {
        try {
            SSHClient ssh = getSshClient();

            SSHClientOutput sshOutput;
            sshOutput = ssh.execCommand(command, REMOTE_TIMEOUT_SECONDS, logAppender);
            int exitCode = sshOutput.getExitCode();
            String output = sshOutput.getText();
            return Pair.newPair(exitCode, output);
        } catch (Exception e) {
            throw new ShellException(e);
        }
    }

    private CliCmdExecResult runNativeCommand(String command, Logger logAppender, String jobId) throws ShellException {
        int pid = 0;

        try {

            String[] cmd = new String[3];
            String osName = System.getProperty("os.name");
            if (osName.startsWith("Windows")) {
                cmd[0] = "cmd.exe";
                cmd[1] = "/C";
            } else {
                cmd[0] = "/bin/bash";
                cmd[1] = "-c";
            }
            cmd[2] = command;

            ProcessBuilder builder = new ProcessBuilder(cmd);
            builder.environment().putAll(System.getenv());
            builder.redirectErrorStream(true);
            Process proc = builder.start();
            pid = ProcessUtils.getPid(proc);
            logger.info("sub process {} on behalf of job {}, start to run...", pid, jobId);
            EventBusFactory.getInstance().postSync(new ProcessStart(pid, jobId));

            StringBuilder result = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
                String line;

                while ((line = reader.readLine()) != null) {
                    result.append(line).append('\n');
                    if (logAppender != null) {
                        logAppender.log(line);
                    }
                    if (Thread.currentThread().isInterrupted()) {
                        String msg = Arrays.toString(cmd) + " is interrupt";
                        logger.warn(msg);
                        throw new InterruptedException(msg);
                    }
                }
            }

            try {
                int exitCode = proc.waitFor();
                String b = result.toString();
                if (b.length() > (100 << 20)) {
                    logger.info("[LESS_LIKELY_THINGS_HAPPENED]Sub process log larger than 100M");
                }
                return new CliCmdExecResult(exitCode, b, pid + "");

            } catch (InterruptedException e) {
                logger.warn("Thread is interrupted, cmd: {}, pid: {}", cmd, pid, e);
                Thread.currentThread().interrupt();
                throw e;
            }
        } catch (Exception e) {
            throw new ShellException(e);
        } finally {
            EventBusFactory.getInstance().postSync(new ProcessFinished(pid));
        }
    }

    @Setter
    @Getter
    @AllArgsConstructor
    @ToString
    public static class ProcessStart {

        int pid;

        String jobId;
    }

    @Setter
    @Getter
    @AllArgsConstructor
    @ToString
    public static class ProcessFinished {

        int pid;
    }

    @Setter
    @Getter
    @AllArgsConstructor
    @ToString
    public static class JobKilled {

        String jobId;

    }

    @AllArgsConstructor
    public static class CliCmdExecResult {
        @Getter
        @Setter
        int code;

        @Getter
        @Setter
        String cmd;

        @Getter
        @Setter
        String processId;
    }
}
