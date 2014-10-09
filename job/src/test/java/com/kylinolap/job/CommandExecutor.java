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

package com.kylinolap.job;

/**
 * @author ysong1
 *
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.kylinolap.common.util.SSHClient;
import com.kylinolap.common.util.SSHClientOutput;

public class CommandExecutor {

    private String command;
    private String remoteHost;
    private String remoteUser;
    private String remotePwd;
    private int remoteTimeoutSeconds = 3600;
    private int exitCode = 0;

    private CliOutputConsumer cliOutputConsumer;

    public CommandExecutor(String command, CliOutputConsumer cliOutputConsumer) {
        this.command = command;
        this.cliOutputConsumer = cliOutputConsumer;
    }

    public void setRunAtRemote(String host, String user, String pwd) {
        this.remoteHost = host;
        this.remoteUser = user;
        this.remotePwd = pwd;
    }

    public void setRunAtLocal() {
        this.remoteHost = null;
        this.remoteUser = null;
        this.remotePwd = null;
    }

    public int execute(boolean wait) throws IOException {
        if (remoteHost == null) {
            if (wait) {
                runNativeCommand();
            } else {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            runNativeCommand();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
            }

        } else {
            if (wait) {
                runRemoteCommand();
            } else {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            runRemoteCommand();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
            }
        }

        return exitCode;
    }

    private void runRemoteCommand() throws IOException {
        SSHClient ssh = new SSHClient(remoteHost, remoteUser, remotePwd, this.cliOutputConsumer);

        SSHClientOutput sshOutput;
        try {
            sshOutput = ssh.execCommand(command, remoteTimeoutSeconds);
            exitCode = sshOutput.getExitCode();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void runNativeCommand() throws IOException {
        String[] cmd = new String[3];
        String osName = System.getProperty("os.name");
        if (osName.startsWith("Windows")) {
            cmd[0] = "cmd";
            cmd[1] = "/C";
        } else {
            cmd[0] = "/bin/bash";
            cmd[1] = "-c";
        }
        cmd[2] = command;

        ProcessBuilder builder = new ProcessBuilder(cmd);
        builder.redirectErrorStream(true);
        Process proc = builder.start();

        BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        String line = null;

        while ((line = reader.readLine()) != null) {
            this.cliOutputConsumer.consume(line);
        }

        try {
            exitCode = proc.waitFor();
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            reader.close();
        }
    }
}