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
package org.apache.kylin.tool.util;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Locale;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ToolUtilTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public static int getFreePort() throws Exception {
        for (int port = 1025; port < 65000; ++port) {
            if (!ToolUtil.isPortAvailable("127.0.0.1", port)) {
                return port;
            }
        }
        throw new RuntimeException("no available port");
    }

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testDumpKylinJStack() throws Exception {
        String mainDir = temporaryFolder.getRoot() + "/testDumpKylinJStack";

        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        String pid = runtimeMXBean.getName().split("@")[0];

        File pidFie = new File(ToolUtil.getKylinHome(), "pid");
        FileUtils.writeStringToFile(pidFie, pid);
        Assert.assertEquals(pid, ToolUtil.getKylinPid());

        File jstackFile = new File(mainDir, String.format(Locale.ROOT, "jstack.%s", pid));
        ToolUtil.dumpKylinJStack(jstackFile);

        FileUtils.deleteQuietly(pidFie);
        Assert.assertTrue(jstackFile.exists());
    }

    @Test
    public void testIsPortAvailable() throws Exception {
        int port = getFreePort();
        Assert.assertFalse(ToolUtil.isPortAvailable("127.0.0.1", port));
        new Thread(() -> {
            try {
                ServerSocket serverSocket = new ServerSocket(port);
                Socket socket = serverSocket.accept();
                socket.close();
                serverSocket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        Thread.sleep(3000);
        Assert.assertTrue(ToolUtil.isPortAvailable("127.0.0.1", port));

    }

    @Test
    public void testGetHdfsJobTmpDir() {
        String path = ToolUtil.getHdfsJobTmpDir("abcd");
        Assert.assertTrue(path.endsWith("abcd/job_tmp"));
    }

    @Test
    public void testExistsLinuxCommon() {
        boolean abcdExist = ToolUtil.existsLinuxCommon("abcd");
        Assert.assertFalse(abcdExist);

        boolean lsExist = ToolUtil.existsLinuxCommon("ls");
        Assert.assertTrue(lsExist);
    }
}
