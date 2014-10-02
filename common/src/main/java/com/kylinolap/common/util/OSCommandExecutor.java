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

package com.kylinolap.common.util;

import org.apache.commons.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author yangli9
 */
public class OSCommandExecutor {

    private String command;
    private String remoteHost;
    private String remoteUser;
    private String remotePwd;
    private int remoteTimeoutSeconds = 3600;

    private String output;
    private int exitCode;

    public OSCommandExecutor(String command) {
        this.command = command;
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

    public String execute() throws IOException {
        if (remoteHost == null)
            runNativeCommand();
        else
            runRemoteCommand();

        if (exitCode != 0)
            throw new IOException("OS command error exit with " + exitCode + " -- " + command);

        return output;
    }

    private void runRemoteCommand() throws IOException {
        SSHClient ssh = new SSHClient(remoteHost, remoteUser, remotePwd, null);

        SSHClientOutput sshOutput;
        try {
            sshOutput = ssh.execCommand(command, remoteTimeoutSeconds);
            exitCode = sshOutput.getExitCode();
            output = sshOutput.getText();
        } catch (Exception e) {
            throw new IOException(e);
        }

    }

    private void runNativeCommand() throws IOException {
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
        builder.redirectErrorStream(true);
        Process proc = builder.start();

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        IOUtils.copy(proc.getInputStream(), buf);
        output = buf.toString("UTF-8");

        try {
            exitCode = proc.waitFor();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

}
