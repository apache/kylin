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

package org.apache.kylin.query.util;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KeywordDefaultDirtyHackTest extends LocalFileMetadataTestCase {
    private KeywordDefaultDirtyHack kwDefaultHack = new KeywordDefaultDirtyHack();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.escape-default-keyword", "true");
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testTransform() {
        {
            String sql = "select count(*) from default.test_kylin_fact";
            String s = kwDefaultHack.transform(sql, null, "DEFAULT");
            Assert.assertEquals("select count(*) from \"DEFAULT\".test_kylin_fact", s);
        }
        {
            String sql = "select count(*) from DEFAULT.test_kylin_fact";
            String s = kwDefaultHack.transform(sql, null, "DEFAULT");
            Assert.assertEquals("select count(*) from \"DEFAULT\".test_kylin_fact", s);
        }
        {
            String sql = "select count(*) from defaulT.test_kylin_fact";
            String s = kwDefaultHack.transform(sql, null, "DEFAULT");
            Assert.assertEquals("select count(*) from \"DEFAULT\".test_kylin_fact", s);
        }
        {
            String sql = "select count(*) from defaultCatalog.default.test_kylin_fact";
            String s = kwDefaultHack.transform(sql, null, "DEFAULT");
            Assert.assertEquals("select count(*) from \"DEFAULT\".test_kylin_fact", s);
        }
        {
            String sql = "select count(*) from \"defaultCatalog\".default.test_kylin_fact";
            String s = kwDefaultHack.transform(sql, null, "DEFAULT");
            Assert.assertEquals("select count(*) from \"DEFAULT\".test_kylin_fact", s);
        }
    }
}
