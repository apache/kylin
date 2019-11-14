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
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.Reader;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniLocalZookeeperCluster {

    private static final Logger LOG = LoggerFactory.getLogger(MiniLocalZookeeperCluster.class);
    private NIOServerCnxnFactory serverCnxnFactory;
    private ZooKeeperServer zkServer;
    private File baseDir;
    private int zkClientPort = -1;
    private boolean started = false;

    public MiniLocalZookeeperCluster() {
    }

    public boolean isStarted() {
        return this.started;
    }

    public int start() throws Exception {
        if (started) {
            throw new RuntimeException("Mini Zookeeper Cluster has started");
        }
        this.baseDir = this.getBaseDir();
        int clientPort = this.selectClientPort();
        ZooKeeperServer server = new ZooKeeperServer(baseDir, baseDir, 2000);
        server.setMinSessionTimeout(-1);
        server.setMaxSessionTimeout(-1);
        NIOServerCnxnFactory standaloneServerFactory;
        while (true) {
            try {
                standaloneServerFactory = new NIOServerCnxnFactory();
                standaloneServerFactory.configure(new InetSocketAddress(clientPort), 1000);
            } catch (BindException e) {
                LOG.debug("Failed binding ZK Server to client port:{} ", clientPort, e);
                // This port is already in use, try to use another.
                clientPort = selectClientPort();
                continue;
            }
            break;
        }
        standaloneServerFactory.startup(server);
        // Runs a 'stat' against the servers.
        if (!waitForServerUp(clientPort, 30000)) {
            throw new IOException("Waiting for startup of standalone server");
        }

        this.serverCnxnFactory = standaloneServerFactory;
        this.zkServer = server;
        this.zkClientPort = clientPort;
        this.started = true;

        LOG.info("Started MiniZooKeeperCluster and ran successful 'stat' " + "on client port=" + clientPort);
        return clientPort;
    }

    public void shutdown() throws Exception {
        if (!started) {
            LOG.info("This zookeeper cluster has already shutdown");
            return;
        }
        this.serverCnxnFactory.shutdown();
        if (!waitForServerDown(this.zkClientPort, 3000)) {
            throw new IOException("Waiting for shutdown of standalone server");
        }
        this.zkServer.getZKDatabase().clear();
        this.deleteDirectory(this.baseDir);
        if (started) {
            started = false;
            LOG.info("Shutdown MiniZK cluster with all ZK servers");
        }
    }

    public void deleteDirectory(File file) {

        if (file.isFile()) {
            file.delete();
        } else {
            File list[] = file.listFiles();
            if (list != null) {
                for (File f : list) {
                    deleteDirectory(f);
                }
                file.delete();
            }
        }
    }

    private int selectClientPort() {
        return 0xc000 + new Random().nextInt(0x3f00);
    }

    private File getBaseDir() {
        File baseDir = new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA, "zookeeper_test").getAbsoluteFile();
        if (!baseDir.exists()) {
            baseDir.mkdir();
        }
        return baseDir;
    }

    private static void setupTestEnv() {
        System.setProperty("zookeeper.preAllocSize", "100");
        FileTxnLog.setPreallocSize(100 * 1024);
        System.setProperty("zookeeper.4lw.commands.whitelist", "*");
    }

    private boolean waitForServerUp(int port, long timeout) throws IOException {
        long start = System.currentTimeMillis();
        while (true) {
            try {
                Socket sock = new Socket("localhost", port);
                BufferedReader reader = null;
                try {
                    OutputStream outstream = sock.getOutputStream();
                    outstream.write(Bytes.toBytes("stat"));
                    outstream.flush();

                    Reader isr = new InputStreamReader(sock.getInputStream(), StandardCharsets.UTF_8);
                    reader = new BufferedReader(isr);
                    String line = reader.readLine();
                    if (line != null && line.startsWith("Zookeeper version:")) {
                        return true;
                    }
                } finally {
                    sock.close();
                    if (reader != null) {
                        reader.close();
                    }
                }
            } catch (IOException e) {
                // ignore as this is expected
                LOG.info("server localhost:" + port + " not up " + e);
            }
            if (System.currentTimeMillis() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                throw (InterruptedIOException) new InterruptedIOException().initCause(e);
            }
        }
        return false;
    }

    private boolean waitForServerDown(int port, long timeout) throws IOException {
        long start = System.currentTimeMillis();
        while (true) {
            try {
                Socket sock = new Socket("localhost", port);
                try {
                    OutputStream outstream = sock.getOutputStream();
                    outstream.write(Bytes.toBytes("stat"));
                    outstream.flush();
                } finally {
                    sock.close();
                }
            } catch (IOException e) {
                return true;
            }

            if (System.currentTimeMillis() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                throw (InterruptedIOException) new InterruptedIOException().initCause(e);
            }
        }
        return false;
    }

    public int getZkClientPort() {
        return this.zkClientPort;
    }
}
