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

import java.nio.charset.Charset;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorUtil {
    private static final Logger LOG = LoggerFactory.getLogger(CuratorUtil.class);

    /**
     * 创建path
     * @param client
     * @param path
     */
    public static void initPstPath(CuratorFramework client, String path) throws Exception {

        try {
            if (client.checkExists().forPath(path) == null) {
                client.create().withMode(CreateMode.PERSISTENT).forPath(path, new byte[0]);
            }
        } catch (Exception e) {
            LOG.error("Create Persistent Path Error , Path = " + path, e);
            throw e;
        }
    }

    public static void initPstPathWithParents(CuratorFramework client, String path) throws Exception {

        try {
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, new byte[0]);
            }
        } catch (Exception e) {
            LOG.error("Create Persistent Path Error , Path = " + path, e);
            throw e;
        }
    }

    public static void initEphPstPathWithParents(CuratorFramework client, String path) throws Exception {
        initEphPstPathWithParentsAndData(client, path, new byte[] { 0 });
    }

    public static void initEphPstPathWithParentsAndData(CuratorFramework client, String path, byte[] data)
            throws Exception {

        try {
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, data);
            }
        } catch (Exception e) {
            LOG.error("Create Ephemeral Path Error , Path = " + path, e);
            throw e;
        }
    }

    public static String getData(CuratorFramework client, String path) {

        try {
            if (client.checkExists().forPath(path) != null) {
                byte[] data = client.getData().forPath(path);
                return new String(data, Charset.forName("UTF-8"));
            }
        } catch (Exception e) {
            LOG.error("Get Data Error , Path = " + path, e);
        }
        return null;
    }

    /**
     * Get children and set the given watcher on the node.
     */
    public static List<String> watchedGetChildren(CuratorFramework client, String path, Watcher watcher)
            throws Exception {

        if (client.checkExists().forPath(path) != null) {
            return client.getChildren().usingWatcher(watcher).forPath(path);
        }
        return null;

    }

    /**
     * Get children and set the given watcher on the node.
     */
    public static byte[] watchedGetData(CuratorFramework client, String path, Watcher watcher) throws Exception {

        if (client.checkExists().forPath(path) != null) {
            return client.getData().usingWatcher(watcher).forPath(path);
        }
        return null;
    }

}
