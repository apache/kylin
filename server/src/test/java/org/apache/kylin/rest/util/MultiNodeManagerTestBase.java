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

package org.apache.kylin.rest.util;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CheckUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.rest.broadcaster.BroadcasterReceiveServlet;
import org.apache.kylin.rest.service.CacheService;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiNodeManagerTestBase extends LocalFileMetadataTestCase {
    private Server server;
    protected static String PROJECT = "default";
    protected static String USER = "u1";
    protected static String TABLE = "t1";

    protected KylinConfig configA;
    protected KylinConfig configB;
    private static final Logger logger = LoggerFactory.getLogger(MultiNodeManagerTestBase.class);

    @Before
    public void setup() throws Exception {
        staticCreateTestMetadata();
        System.clearProperty("kylin.server.cluster-servers");
        int port = CheckUtil.randomAvailablePort(40000, 50000);
        logger.info("Chosen port for CacheServiceTest is " + port);
        configA = KylinConfig.getInstanceFromEnv();
        configA.setProperty("kylin.server.cluster-servers", "localhost:" + port);
        configB = KylinConfig.createKylinConfig(configA);
        configB.setProperty("kylin.server.cluster-servers", "localhost:" + port);
        configB.setMetadataUrl("../examples/test_metadata");

        server = new Server(port);
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);

        final CacheService serviceA = new CacheService() {
            @Override
            public KylinConfig getConfig() {
                return configA;
            }
        };
        final CacheService serviceB = new CacheService() {
            @Override
            public KylinConfig getConfig() {
                return configB;
            }
        };

        context.addServlet(new ServletHolder(new BroadcasterReceiveServlet(new BroadcasterReceiveServlet.BroadcasterHandler() {
            @Override
            public void handle(String entity, String cacheKey, String event) {
                Broadcaster.Event wipeEvent = Broadcaster.Event.getEvent(event);
                final String log = "wipe cache type: " + entity + " event:" + wipeEvent + " name:" + cacheKey;
                logger.info(log);
                try {
                    serviceA.notifyMetadataChange(entity, wipeEvent, cacheKey);
                    serviceB.notifyMetadataChange(entity, wipeEvent, cacheKey);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        })), "/");
        server.start();
    }

    @After
    public void after() throws Exception {
        server.stop();
        cleanAfterClass();
    }
}
