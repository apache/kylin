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

package org.apache.kylin.restclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class ITRestClientTest extends HBaseMetadataTestCase {

    private static Server server = null;

    private static SystemPropertiesOverride sysPropsOverride = new SystemPropertiesOverride();

    private static final String HOST = "localhost";

    private static final int PORT = new Random().nextInt(100) + 37070;

    private static final String USERNAME = "ADMIN";

    private static final String PASSWD = "KYLIN";

    private static final String PROJECT_NAME = "default";

    private static final String CUBE_NAME = "ci_left_join_cube";

    private static final Logger logger = LoggerFactory.getLogger(ITRestClientTest.class);

    @BeforeClass
    public static void beforeClass() throws Exception {
        logger.info("random jetty port: " + PORT);
        sysPropsOverride.override("spring.profiles.active", "testing");
        sysPropsOverride.override("catalina.home", "."); // resources/log4j.properties ref ${catalina.home}
        staticCreateTestMetadata();
        startJetty();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        stopJetty();
        staticCleanupTestMetadata();
        sysPropsOverride.restore();
    }

    @Test
    public void testGetCube() throws Exception {
        RestClient client = new RestClient(HOST, PORT, USERNAME, PASSWD);
        HashMap result = client.getCube(CUBE_NAME);
        assertEquals("READY", result.get("status"));
    }

    @Test
    public void testChangeCubeStatus() throws Exception {
        RestClient client = new RestClient(HOST, PORT, USERNAME, PASSWD);
        assertTrue(client.disableCube(CUBE_NAME));
        assertTrue(client.enableCube(CUBE_NAME));
    }

    @Test
    public void testChangeCache() throws Exception {
        RestClient client = new RestClient(HOST, PORT, USERNAME, PASSWD);
        assertTrue(client.disableCache());
        assertTrue(client.enableCache());
    }

    @Test
    public void testQuery() throws Exception {
        RestClient client = new RestClient(HOST, PORT, USERNAME, PASSWD);
        String sql = "select count(*) from TEST_KYLIN_FACT; ";
        HttpResponse result = client.query(sql, PROJECT_NAME);
    }

    protected static void stopJetty() throws Exception {
        if (server != null)
            server.stop();

        File workFolder = new File("work");
        if (workFolder.isDirectory() && workFolder.exists()) {
            FileUtils.deleteDirectory(workFolder);
        }
    }

    protected static void startJetty() throws Exception {

        server = new Server(PORT);

        WebAppContext context = new WebAppContext();
        context.setDescriptor("../server/src/main/webapp/WEB-INF/web.xml");
        context.setResourceBase("../server/src/main/webapp");
        context.setContextPath("/kylin");
        context.setParentLoaderPriority(true);

        server.setHandler(context);

        server.start();

    }

    private static class SystemPropertiesOverride {
        HashMap<String, String> backup = new HashMap<String, String>();

        public void override(String key, String value) {
            backup.put(key, System.getProperty(key));
            System.setProperty(key, value);
        }

        public void restore() {
            for (String key : backup.keySet()) {
                String value = backup.get(key);
                if (value == null)
                    System.clearProperty(key);
                else
                    System.setProperty(key, value);
            }
            backup.clear();
        }
    }
}
