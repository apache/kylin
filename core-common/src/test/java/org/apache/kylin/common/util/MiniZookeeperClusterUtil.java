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

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniZookeeperClusterUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MiniZookeeperClusterUtil.class);

    private static MiniLocalZookeeperCluster zookeeperCluster;

    public static MiniLocalZookeeperCluster startMiniZookeeperCluster() throws Exception {

        if (zookeeperCluster == null) {
            zookeeperCluster = new MiniLocalZookeeperCluster();
        }
        if (zookeeperCluster.isStarted()) {
            throw new RuntimeException("Mini Zookeeper cluster has already started");
        }
        zookeeperCluster.start();
        return zookeeperCluster;
    }

    public static void shutdownMiniZookeeperCluster() throws Exception {

        if (zookeeperCluster == null || !zookeeperCluster.isStarted()) {
            LOG.info("Mini Zookeeper cluster has already stoped");
            return;
        }
        zookeeperCluster.shutdown();
    }

    public static MiniLocalZookeeperCluster getZookeeperCluster() {
        return zookeeperCluster;
    }
}
