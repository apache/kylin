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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.lang.reflect.Method;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import lombok.val;

@MetadataInfo(onlyProps = true)
public class CliCommandExecutorTest {

    @Test
    public void testCopyRemoteToLocal(@TempDir File root, TestInfo testInfo) throws Exception {

        File mainDir = new File(root, testInfo.getTestMethod().map(Method::getName).orElse(""));
        FileUtils.forceMkdir(mainDir);

        File tmpDir = new File(mainDir, "from");
        File tempFile = new File(tmpDir, "temp-file.log");
        FileUtils.writeStringToFile(tempFile, "abc");
        File targetDir = new File(mainDir, "to");

        SSHClient mockSSHClient = MockSSHClient.getInstance();
        CliCommandExecutor cliSpy = Mockito.spy(new CliCommandExecutor("localhost", "root", null));
        Mockito.doReturn(mockSSHClient).when(cliSpy).getSshClient();

        cliSpy.copyRemoteToLocal(tempFile.getAbsolutePath(), targetDir.getAbsolutePath());

        val fileList = targetDir.listFiles();
        Assert.assertNotNull(fileList);
        Assert.assertEquals(1, fileList.length);
        Assert.assertEquals(fileList[0].getName(), tempFile.getName());

    }

    @Test
    public void testCopyLocalToRemote(@TempDir File root, TestInfo testInfo) throws Exception {
        File mainDir = new File(root, testInfo.getTestMethod().map(Method::getName).orElse(""));
        FileUtils.forceMkdir(mainDir);

        File tmpDir = new File(mainDir, "from");
        File tempFile = new File(tmpDir, "temp-file.log");
        FileUtils.writeStringToFile(tempFile, "abc");
        File targetDir = new File(mainDir, "to");

        SSHClient mockSSHClient = MockSSHClient.getInstance();
        CliCommandExecutor cliSpy = Mockito.spy(new CliCommandExecutor("localhost", "root", null));
        Mockito.doReturn(mockSSHClient).when(cliSpy).getSshClient();

        cliSpy.copyFile(tempFile.getAbsolutePath(), targetDir.getAbsolutePath());

        val fileList = targetDir.listFiles();
        Assert.assertNotNull(fileList);
        Assert.assertEquals(1, fileList.length);
        Assert.assertEquals(fileList[0].getName(), tempFile.getName());

    }

    @Test
    void testLargeOutputSlice() throws Exception {
        // These test assert the kylin.command.max-output-length is 5M chars as 10 MB size
        int maxCommandLineOutputLength = KylinConfig.getInstanceFromEnv().getMaxCommandLineOutputLength();
        assertEquals(5 * 1024 * 1024, maxCommandLineOutputLength);
        CliCommandExecutor cliCommandExecutor = new CliCommandExecutor();
        // generate a large output and expect the output is truncated
        CliCommandExecutor.CliCmdExecResult res = cliCommandExecutor.execute(
                "echo 'testhead' && line=$(printf 'a%.0s' {1..1251}) && yes \"$line\" 2>/dev/null | head -n 5505 && echo 'testtail'",
                null);
        assertEquals(maxCommandLineOutputLength, res.getCmd().length());
        assertTrue(res.getCmd().startsWith("testhead"));
        assertTrue(res.getCmd().endsWith("testtail\n"));
        // multiline long string
        res = cliCommandExecutor
                .execute("echo 'testhead' && line=$(printf 'a%.0s' {1..111}) && yes \"$line\" 2>/dev/null | head -n " + 1024 * 52
                        + "&& echo 'testtail'", null);
        assertEquals(maxCommandLineOutputLength, res.getCmd().length());
        assertTrue(res.getCmd().startsWith("testhead"));
        assertTrue(res.getCmd().endsWith("testtail\n"));
        // multiline short string
        res = cliCommandExecutor.execute("echo 'testhead' && line=$(printf 'a%.0s' {1..9}) && yes \"$line\" 2>/dev/null | head -n "
                + 1024 * 513 + "&& echo 'testtail'", null);
        assertEquals(maxCommandLineOutputLength, res.getCmd().length());
        assertTrue(res.getCmd().startsWith("testhead"));
        assertTrue(res.getCmd().endsWith("testtail\n"));

        // generate large output less than 10 mb and expect the same output
        res = cliCommandExecutor.execute(
                "echo 'testhead' && line=$(printf 'a%.0s' {1..1251}) && yes \"$line\" 2>/dev/null | head -n 1000 && echo 'testtail'",
                null);
        final int HEAD_TAIL_LEN = 18;
        assertEquals(1252 * 1000 + HEAD_TAIL_LEN, res.getCmd().length());
        assertTrue(res.getCmd().startsWith("testhead"));
        assertTrue(res.getCmd().endsWith("testtail\n"));

        // multiline long string
        res = cliCommandExecutor
                .execute("echo 'testhead' && line=$(printf 'a%.0s' {1..111}) && yes \"$line\" 2>/dev/null | head -n " + 1024 * 10
                        + "&& echo 'testtail'", null);
        assertEquals(112 * 10 * 1024 + HEAD_TAIL_LEN, res.getCmd().length());
        assertTrue(res.getCmd().startsWith("testhead"));
        assertTrue(res.getCmd().endsWith("testtail\n"));
        // multiline short string
        res = cliCommandExecutor.execute("echo 'testhead' && line=$(printf 'a%.0s' {1..9}) && yes \"$line\" 2>/dev/null | head -n "
                + 1024 * 100 + "&& echo 'testtail'", null);
        assertEquals(10 * 100 * 1024 + HEAD_TAIL_LEN, res.getCmd().length());
        assertTrue(res.getCmd().startsWith("testhead"));
        assertTrue(res.getCmd().endsWith("testtail\n"));
    }
}

class MockSSHClient extends SSHClient {

    public static MockSSHClient getInstance() {
        return new MockSSHClient(null, -1, null, null);
    }

    public MockSSHClient(String hostname, int port, String username, String password) {
        super(hostname, port, username, password);
    }

    @Override
    public void scpRemoteFileToLocal(String remoteFile, String localTargetDirectory) throws Exception {
        FileUtils.copyFileToDirectory(new File(remoteFile), new File(localTargetDirectory));
    }

    @Override
    public void scpFileToRemote(String localFile, String remoteTargetDirectory) throws Exception {
        FileUtils.copyFileToDirectory(new File(localFile), new File(remoteTargetDirectory));
    }
}
