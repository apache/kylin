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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.JobProcessContext;
import org.slf4j.LoggerFactory;

/**
 * @author yangli9
 */
public class CliCommandExecutor {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CliCommandExecutor.class);
    private String remoteHost;
    private int port;
    private String remoteUser;
    private String remotePwd;
    private int remoteTimeoutSeconds = 3600;

    public CliCommandExecutor() {
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

    public void copyFile(String localFile, String destDir) throws IOException {
        if (remoteHost == null)
            copyNative(localFile, destDir);
        else
            copyRemote(localFile, destDir);
    }

    private void copyNative(String localFile, String destDir) throws IOException {
        File src = new File(localFile);
        File dest = new File(destDir, src.getName());
        FileUtils.copyFile(src, dest);
    }

    private void copyRemote(String localFile, String destDir) throws IOException {
        SSHClient ssh = new SSHClient(remoteHost, port, remoteUser, remotePwd);
        try {
            ssh.scpFileToRemote(localFile, destDir);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    public Pair<Integer, String> execute(String command) throws IOException {
        return execute(command, new SoutLogger(), null);
    }

    public Pair<Integer, String> execute(String command, Logger logAppender, String jobId) throws IOException {
        Pair<Integer, String> r;
        if (remoteHost == null) {
            r = runNativeCommand(command, logAppender, jobId);
        } else {
            r = runRemoteCommand(command, logAppender);
        }

        if (r.getFirst() != 0)
            throw new IOException("OS command error exit with return code: " + r.getFirst() //
                    + ", error message: " + r.getSecond() + "The command is: \n" + command
                    + (remoteHost == null ? "" : " (remoteHost:" + remoteHost + ")") //
            );

        return r;
    }

    private Pair<Integer, String> runRemoteCommand(String command, Logger logAppender) throws IOException {
        SSHClient ssh = new SSHClient(remoteHost, port, remoteUser, remotePwd);

        SSHClientOutput sshOutput;
        try {
            sshOutput = ssh.execCommand(command, remoteTimeoutSeconds, logAppender);
            int exitCode = sshOutput.getExitCode();
            String output = sshOutput.getText();
            return Pair.newPair(exitCode, output);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private Pair<Integer, String> runNativeCommand(String command, Logger logAppender, String jobId) throws IOException {
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
            builder.redirectErrorStream(true);
            Process proc = builder.start();

            if (StringUtils.isNotBlank(jobId)) {
                logger.info("Register process {} to {}", proc.toString(), jobId);
                JobProcessContext.registerProcess(jobId, proc);
            }

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8));
            String line;
            StringBuilder result = new StringBuilder();
            while ((line = reader.readLine()) != null && !Thread.currentThread().isInterrupted()) {
                result.append(line).append('\n');
                if (logAppender != null) {
                    logAppender.log(line);
                }
            }

            if (Thread.interrupted()) {
                logger.info("CliCommandExecutor is interrupted by other, kill the sub process: " + command);
                proc.destroy();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // do nothing
                }
                return Pair.newPair(1, "Killed");
            }

            try {
                int exitCode = proc.waitFor();
                return Pair.newPair(exitCode, result.toString());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        } finally {
            if (StringUtils.isNotBlank(jobId)) {
                JobProcessContext.removeProcess(jobId);
            }
        }
    }

    public static final String COMMAND_BLOCK_LIST = "[ &`>|{}()$;\\-#~!+*\\\\]+";
    public static final String COMMAND_WHITE_LIST = "[^\\w%,@/:=?.\"\\[\\]]";
    public static final String HIVE_BLOCK_LIST = "[ <>()$;\\-#!+*\"'/=%@]+";


    /**
     * <pre>
     * Check parameter for preventing command injection, replace illegal character into empty character.
     *
     * Note:
     * 1. Whitespace is also refused because parameter is a single word, should not contains it
     * 2. Some character may be illegal but still be accepted because commandParameter maybe a URI/path expression,
     *     you may check "Character part" in https://docs.oracle.com/javase/8/docs/api/java/net/URI.html,
     *     here is the character which is not banned.
     *
     *     1. dot .
     *     2. slash /
     *     3. colon :
     *     4. equal =
     *     5. ?
     *     6. @
     *     7. bracket []
     *     8. comma ,
     *     9. %
     * </pre>
     */
    public static String checkParameter(String commandParameter) {
        return checkParameter(commandParameter, COMMAND_BLOCK_LIST);
    }

    public static String checkParameterWhiteList(String commandParameter) {
        return checkParameter(commandParameter, COMMAND_WHITE_LIST);
    }

    public static String checkHiveProperty(String hiveProperty) {
        return checkParameter(hiveProperty, HIVE_BLOCK_LIST);
    }

    private static String checkParameter(String commandParameter, String rex) {
        String repaired = commandParameter.replaceAll(rex, "");
        if (repaired.length() != commandParameter.length()) {
            logger.warn("Detected illegal character in command {} by {} , replace it to {}.", commandParameter, rex, repaired);
        }
        return repaired;
    }
}
