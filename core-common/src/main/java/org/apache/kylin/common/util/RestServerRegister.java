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

import java.net.InetAddress;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.kylin.common.KylinConfig;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestServerRegister {

    private static final Logger LOG = LoggerFactory.getLogger(RestServerRegister.class);

    private static RestServerRegister instance = null;

    private KylinConfig kylinConfig;
    private CuratorFramework curatorClient = null;

    private Watcher serverNodeWatcher;
    private List<String> servers;

    private RestServerRegister() {
    }

    private void init(KylinConfig config) {
        this.kylinConfig = config;
        this.curatorClient = ZKUtil.newZookeeperClient(this.kylinConfig);
        this.serverNodeWatcher = new ServerNodeWatcher();
    }

    public static RestServerRegister getInstance() {
        return getInstance(KylinConfig.getInstanceFromEnv());
    }

    public static synchronized RestServerRegister getInstance(KylinConfig config) {
        if (instance == null) {

            instance = new RestServerRegister();
            instance.init(config);
            try {
                instance.listen();
            } catch (Exception e) {
                LOG.error("registry client based on zookeeper listen occur error", e);
                throw new RuntimeException(e);
            }
        }
        return instance;
    }

    public void close() {
        if (curatorClient.getState() == CuratorFrameworkState.STARTED) {
            LOG.info("Closing Rest Server Registry");
            curatorClient.close();
        }
    }

    public void listen() throws Exception {
        String restNode = this.kylinConfig.getRestServersZookeeperNode();
        LOG.info("Rest server registry client is listening this path:{}", restNode);
        ZKUtil.initPstPathWithParents(this.curatorClient, restNode);
        this.servers = this.listServers(this.curatorClient, restNode, this.serverNodeWatcher);
    }

    public void register() throws Exception {
        String restNode = this.kylinConfig.getRestServersZookeeperNode();
        String serverNode = this.getServerNode(restNode);

        LOG.info("Start to register server address to zookeeper, znode is :" + serverNode);
        ZKUtil.initEphPstPathWithParentsAndData(this.curatorClient, serverNode,
                Bytes.toBytes(System.currentTimeMillis()));
    }

    public List<String> getServers() {
        return this.servers;
    }

    private List<String> listServers(CuratorFramework curatorClient, String registryBasePath, Watcher watcher) {
        List<String> servers = null;
        try {
            servers = ZKUtil.watchedGetChildren(curatorClient, registryBasePath, watcher);
        } catch (Exception e) {
            LOG.error("Rest server register get zookeeper children error, path = {}", registryBasePath, e);
        }
        return servers;
    }

    private String getServerNode(String basePath) throws Exception {

        InetAddress address = ToolUtil.getFirstIPV4NonLoopBackAddress();
        String listenPort = ToolUtil.getListenPort();
        listenPort = listenPort == null ? "7070" : listenPort;
        String serverNode = basePath + "/" + address.getHostAddress() + ":" + listenPort;
        return serverNode;
    }

    public class ServerNodeWatcher implements Watcher {
        @Override
        public void process(WatchedEvent watchedEvent) {
            switch (watchedEvent.getType()) {
            case NodeChildrenChanged: {
                List<String> newServers = listServers(curatorClient, kylinConfig.getRestServersZookeeperNode(),
                        serverNodeWatcher);
                LOG.info("Rest server node on zookeeper ({}) whose children has changed, "
                        + "old servers: {}, new servers: {}", watchedEvent.getPath(), servers, newServers);
                servers = newServers;
                break;
            }
            }
        }
    }
}