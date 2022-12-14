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

package org.apache.kylin.rest.security;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.security.acls.model.Permission;

import com.google.common.collect.Lists;

public class UserAclManagerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testCRUD() {
        UserAclManager userAclManager = UserAclManager.getInstance(getTestConfig());

        userAclManager.add("user1");
        userAclManager.add("user2");
        userAclManager.add("user3");

        Assert.assertTrue(userAclManager.get("user1").hasPermission(128));
        Assert.assertTrue(userAclManager.exists("user1"));
        Assert.assertFalse(userAclManager.exists("user4"));
        Assert.assertEquals(Lists.newArrayList("user1", "user2", "user3"), userAclManager.listAclUsernames());
        Assert.assertNull(userAclManager.get(""));
        Assert.assertNull(userAclManager.get("not_exist"));
        userAclManager.add("user1");

        userAclManager.delete("user1");
        Assert.assertFalse(userAclManager.exists("user1"));
        userAclManager.delete("user");
        Assert.assertFalse(userAclManager.exists("user"));
    }

    @Test
    public void testCRUDCaseInsensitive() {
        UserAclManager userAclManager = UserAclManager.getInstance(getTestConfig());

        userAclManager.add("user1");
        userAclManager.add("user2");
        userAclManager.add("user3");
        Assert.assertTrue(userAclManager.exists("USER1"));
        userAclManager.add("USER1");
        Assert.assertEquals(3, userAclManager.listAclUsernames().size());
        userAclManager.delete("User1");
        Assert.assertFalse(userAclManager.exists("user1"));

        userAclManager.delete("user1");
        Assert.assertEquals(2, userAclManager.listAclUsernames().size());
        Assert.assertTrue(userAclManager.get("user2").hasPermission(AclPermission.DATA_QUERY));
        userAclManager.deletePermission("user2", AclPermission.DATA_QUERY);
        Assert.assertFalse(userAclManager.get("user2").hasPermission(AclPermission.DATA_QUERY));
        userAclManager.addDataQueryProject("user2", "default");
        Assert.assertTrue(userAclManager.get("user2").getDataQueryProjects().contains("default"));
        userAclManager.deleteDataQueryProject("user2", "default");
        Assert.assertFalse(userAclManager.get("user2").getDataQueryProjects().contains("default"));
    }

    @Test
    public void testUserAcl() {
        UserAcl userAcl = createUserAcl("admin", AclPermission.MANAGEMENT);
        Assert.assertEquals(32, userAcl.getPermissionMasks().stream().mapToInt(p -> p).sum());
        Assert.assertTrue(userAcl.hasPermission(32));
        Assert.assertFalse(userAcl.hasPermission(64));
        Assert.assertNotEquals(userAcl, createUserAcl("admin1", AclPermission.MANAGEMENT));
        Assert.assertEquals(userAcl, createUserAcl("admin", AclPermission.OPERATION));

        userAcl.deletePermission(AclPermission.DATA_QUERY);
        userAcl.deletePermission(AclPermission.MANAGEMENT);
        Assert.assertEquals(0, userAcl.getPermissionMasks().stream().mapToInt(p -> p).sum());
        Assert.assertFalse(userAcl.hasPermission(32));

        userAcl.addDataQueryProject("");
        Assert.assertTrue(CollectionUtils.isEmpty(userAcl.getDataQueryProjects()));
        userAcl.deleteDataQueryProject("");
        userAcl.deleteDataQueryProject("default");
        Assert.assertTrue(CollectionUtils.isEmpty(userAcl.getDataQueryProjects()));
        userAcl.addDataQueryProject("default");
        userAcl.addDataQueryProject("default");
        Assert.assertEquals(1, userAcl.getDataQueryProjects().size());

        Assert.assertNotEquals(userAcl, null);
        Assert.assertNotEquals(userAcl, new Object());

        UserAclManager userAclManager = UserAclManager.getInstance(getTestConfig());
        userAclManager.add("user1");
        userAclManager.add("user2");
        Assert.assertEquals(userAclManager.get("user1"), userAclManager.get("user1"));
        Assert.assertNotEquals(userAclManager.get("user1"), userAclManager.get("user2"));

        Assert.assertEquals(userAclManager.get("user1").hashCode(), userAclManager.get("user1").hashCode());
        Assert.assertNotEquals(userAclManager.get("user1").hashCode(), userAclManager.get("user2").hashCode());


    }

    private UserAcl createUserAcl(String userName, Permission permission) {
        UserAcl userAcl = new UserAcl();
        userAcl.setUsername(userName);
        userAcl.addPermission(permission);
        return userAcl;
    }
}
