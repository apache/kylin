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
package org.apache.kylin.tool.upgrade;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DeleteFavoriteQueryCLITest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata("src/test/resources/ut_upgrade_tool");
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void test() {
        KylinConfig systemKylinConfig = KylinConfig.getInstanceFromEnv();
        systemKylinConfig.setProperty("kylin.env", "PROD");

        FavoriteRuleManager favoriteRuleManager = FavoriteRuleManager.getInstance(systemKylinConfig, "broken_test");
        Assert.assertTrue(favoriteRuleManager.getAll().size() > 0);

        DeleteFavoriteQueryCLI deleteFavoriteQueryCLI = new DeleteFavoriteQueryCLI();
        deleteFavoriteQueryCLI.execute(new String[] { "-d", getTestConfig().getMetadataUrl().toString(), "-e" });

        Assert.assertEquals(favoriteRuleManager.getAll().size(), 0);
    }
}
