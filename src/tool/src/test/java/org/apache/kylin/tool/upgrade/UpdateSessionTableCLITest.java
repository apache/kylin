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

import javax.sql.DataSource;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LogOutputTestCase;
import org.apache.kylin.tool.util.MetadataUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

public class UpdateSessionTableCLITest extends LogOutputTestCase {

    private UpdateSessionTableCLI updateSessionTableCLI = Mockito.spy(new UpdateSessionTableCLI());

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testSessionTableNotExist() throws Exception {
        KylinConfig config = getTestConfig();
        DataSource dataSource = MetadataUtil.getDataSource(config);
        ReflectionTestUtils.setField(updateSessionTableCLI, "dataSource", dataSource);
        updateSessionTableCLI.affectedRowsWhenTruncate("not exist");
        updateSessionTableCLI.truncateSessionTable("not exist");
        updateSessionTableCLI.isSessionTableNeedUpgrade("not exist");
        Assert.assertTrue(containsLog("Table not exist is not exist, affected rows is zero."));
        Assert.assertTrue(containsLog("Table not exist is not exist, skip truncate."));
        Assert.assertTrue(containsLog("Table not exist is not exist, no need to upgrade."));
    }

}
