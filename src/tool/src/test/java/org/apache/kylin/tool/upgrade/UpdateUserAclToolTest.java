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

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.security.AclManager;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.UserAclManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

public class UpdateUserAclToolTest extends NLocalFileMetadataTestCase {
    private UpdateUserAclTool tool = Mockito.spy(new UpdateUserAclTool());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        createTestMetadata("src/test/resources/ut_upgrade_tool");
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testUpgrade() {
        String[] args = new String[] { "-s=migrate", "-v=4.5.10", "-h=." };
        Mockito.when(tool.matchUpgradeCondition(args)).thenReturn(true);
        tool.execute(args);
        Assert.assertTrue(tool.isAdminUserUpgraded());
        Assert.assertTrue(tool.isUpgraded());
        val userAclManager = UserAclManager.getInstance(getTestConfig());
        Assert.assertTrue(userAclManager.get("admin_user").hasPermission(AclPermission.DATA_QUERY.getMask()));
        val aclManager = createAclManager(tool);
        Assert.assertTrue(userAclManager.get("admin_user").hasPermission(AclPermission.DATA_QUERY.getMask()));
        val aclRecord = aclManager.get("71d38540-253b-410c-8291-f0dbc7079eaa");
        Assert.assertEquals(160, aclRecord.getEntries().get(0).getPermission().getMask());
        //upgrade repeatedly
        tool.execute(args);
        Assert.assertTrue(userAclManager.get("admin_user").hasPermission(AclPermission.DATA_QUERY.getMask()));
    }

    @Test
    public void testRollback() {
        testUpgrade();
        tool.execute(new String[] { "-r", "-s=migrate", "-v=4.5.10", "-h=." });
        val manager = UserAclManager.getInstance(getTestConfig());
        Assert.assertFalse(manager.exists("admin_user"));
        val aclManager = createAclManager(tool);
        val aclRecord = aclManager.get("71d38540-253b-410c-8291-f0dbc7079eaa");
        Assert.assertEquals(32, aclRecord.getEntries().get(0).getPermission().getMask());
    }

    @Test
    public void testExecute() {
        getTestConfig().setProperty("kylin.server.mode", "query");
        Assert.assertThrows(RuntimeException.class, () -> tool.execute(new String[] {}));
    }

    @Test
    public void testUnknownProfileUpgrade() {
        getTestConfig().setProperty("kylin.security.profile", "unknown");
        thrown.expect(RuntimeException.class);
        tool.execute(new String[] {});
    }

    @Test
    public void testUpdateUserAcl() {
        getTestConfig().setProperty("kylin.security.profile", "custom");
        tool.execute(new String[] { "-f", "-s=migrate", "-v=4.5.10", "-h=." });
        Assert.assertTrue(tool.isUpgraded());
    }

    @Test
    public void testMatchUpgradeCondition() {
        getTestConfig().setProperty("kylin.server.mode", "query");
        Assert.assertFalse(tool.matchUpgradeCondition(new String[] { "-s=migrate", "-v=4.5.10", "-h=." }));
        getTestConfig().setProperty("kylin.server.mode", "all");
        Mockito.when(ReflectionTestUtils.invokeMethod(tool, "isDataPermissionSeparateVersion", ".")).thenReturn(true);
        Assert.assertTrue(tool.matchUpgradeCondition(new String[] { "-s=migrate", "-v=4.5.10", "-h=." }));
        Assert.assertFalse(tool.matchUpgradeCondition(new String[] { "-s=upgrade", "-v=4.5.10", "-h=." }));
        Assert.assertThrows(KylinException.class, () -> tool.matchUpgradeCondition(new String[] { "" }));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> tool.matchUpgradeCondition(new String[] { "-s=migrate", "-v=xe", "-h=." }));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> tool.matchUpgradeCondition(new String[] { "-s=test", "-h=." }));
    }

    @Test
    public void testParseVersion() {
        Assert.assertEquals("4.5.9", tool.parseVersion("Kyligence Enterprise 4.5.9"));
        Assert.assertEquals("4.5.9", tool.parseVersion("Kyligence Enterprise 4.5.9-dev"));
        Assert.assertEquals("4.5.9.0", tool.parseVersion("Kyligence Enterprise 4.5.9.0"));
        Assert.assertEquals("4.5.9.3299", tool.parseVersion("Kyligence Enterprise 4.5.9.3299"));
    }

    @Test
    public void testCompareVersion() {
        Assert.assertEquals(-1, tool.compareVersion("", "4.5.9"));
        Assert.assertEquals(1, tool.compareVersion("4.5.16", ""));
        Assert.assertEquals(1, tool.compareVersion("4.5.16", "4.5.9"));
        Assert.assertEquals(-1, tool.compareVersion("4.1", "4.5.9.1"));
        Assert.assertEquals(0, tool.compareVersion("4.5.9.1", "4.5.9.1"));
        Assert.assertEquals(0, tool.compareVersion("4.5.9.0", "4.5.9"));
        Assert.assertEquals(-1, tool.compareVersion("4.5.7", "4.5.12"));
    }

    private AclManager createAclManager(UpdateUserAclTool tool) {
        return ReflectionTestUtils.invokeMethod(tool, "createAclManager");
    }
}
