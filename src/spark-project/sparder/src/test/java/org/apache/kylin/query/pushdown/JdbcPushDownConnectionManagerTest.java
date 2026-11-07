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

package org.apache.kylin.query.pushdown;

import java.sql.Connection;
import java.util.Map;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JdbcPushDownConnectionManagerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testReplacedConnectionManagerIsClosed() throws Exception {
        String project = "pushdown-pool-" + UUID.randomUUID();
        Map<String, String> overrides = pushdownOverrides("2");
        KylinConfig config = KylinConfigExt.createInstance(KylinConfig.getInstanceFromEnv(), overrides);

        JdbcPushDownConnectionManager manager = JdbcPushDownConnectionManager.getConnectionManager(config, project);
        try (Connection ignored = manager.getConnection()) {
            Assert.assertNotNull(ignored);
        }

        overrides.put("kylin.query.pushdown.jdbc.pool-max-total", "3");
        KylinConfig changedConfig = KylinConfigExt.createInstance(KylinConfig.getInstanceFromEnv(), overrides);
        JdbcPushDownConnectionManager changedManager = JdbcPushDownConnectionManager.getConnectionManager(
                changedConfig, project);

        Assert.assertNotSame(manager, changedManager);
        Assert.assertThrows(RuntimeException.class, manager::getConnection);
        try (Connection ignored = changedManager.getConnection()) {
            Assert.assertNotNull(ignored);
        } finally {
            changedManager.close();
        }
    }

    private Map<String, String> pushdownOverrides(String maxTotal) {
        Map<String, String> overrides = Maps.newHashMap();
        overrides.put("kylin.query.pushdown.jdbc.url",
                "jdbc:h2:mem:pushdown_pool_reload;DB_CLOSE_DELAY=-1");
        overrides.put("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        overrides.put("kylin.query.pushdown.jdbc.username", "sa");
        overrides.put("kylin.query.pushdown.jdbc.password", "");
        overrides.put("kylin.query.pushdown.jdbc.pool-max-total", maxTotal);
        return overrides;
    }
}
