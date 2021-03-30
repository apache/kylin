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

package org.apache.kylin.metadata.acl;

import java.util.HashSet;
import java.util.Set;

import org.apache.kylin.metadata.MetadataConstants;
import org.junit.Assert;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class TableACLTest {
    private static Set<String> EMPTY_GROUP_SET = new HashSet<>();

    @Test
    public void testCaseInsensitive() {
        TableACL tableACL = new TableACL();
        tableACL.add("u1", "t1", MetadataConstants.TYPE_USER);
        try {
            tableACL.add("u1", "T1", MetadataConstants.TYPE_USER);
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals(
                    "Operation fail, can not revoke user's table query permission.Table ACL T1:u1 already exists!",
                        e.getMessage());
        }

        Assert.assertEquals(1, tableACL.getTableBlackList("u1", EMPTY_GROUP_SET).size());
        Assert.assertTrue(tableACL.getTableBlackList("u1", EMPTY_GROUP_SET).contains("T1"));
        tableACL.delete("u1", "T1", MetadataConstants.TYPE_USER);
        Assert.assertEquals(0, tableACL.getTableBlackList("u1", EMPTY_GROUP_SET).size());
    }

   @Test
    public void testDelTableACLByTable() {
        //delete specific table's ACL
        TableACL tableACL = new TableACL();
        tableACL.add("u1", "t1", MetadataConstants.TYPE_USER);
        tableACL.add("u2", "t1", MetadataConstants.TYPE_USER);
        tableACL.add("u2", "t2", MetadataConstants.TYPE_USER);
        tableACL.add("u2", "t3", MetadataConstants.TYPE_USER);
        tableACL.add("u3", "t3", MetadataConstants.TYPE_USER);

        tableACL.add("g1", "t1", MetadataConstants.TYPE_GROUP);
        tableACL.add("g2", "t1", MetadataConstants.TYPE_GROUP);
        tableACL.add("g3", "t2", MetadataConstants.TYPE_GROUP);
        tableACL.deleteByTbl("t1");

        Assert.assertEquals(2, tableACL.size(MetadataConstants.TYPE_USER));
        Assert.assertEquals(1, tableACL.size(MetadataConstants.TYPE_GROUP));
        Assert.assertEquals(0, tableACL.getTableBlackList("u1", EMPTY_GROUP_SET).size());
        Assert.assertEquals(0, tableACL.getTableBlackList("u1", Sets.newHashSet("g1")).size());
        Assert.assertEquals(0, tableACL.getTableBlackList("u1", Sets.newHashSet("g2")).size());
        Assert.assertEquals(1, tableACL.getTableBlackList("u1", Sets.newHashSet("g3")).size());
        Assert.assertEquals(Sets.newHashSet("t2", "t3"), tableACL.getTableBlackList("u2", EMPTY_GROUP_SET));
        Assert.assertEquals(Sets.newHashSet("t3"), tableACL.getTableBlackList("u3", EMPTY_GROUP_SET));
        Assert.assertEquals(1, tableACL.size(MetadataConstants.TYPE_GROUP));
    }

    @Test
    public void testDeleteToEmpty() {
        TableACL tableACL = new TableACL();
        tableACL.add("u1", "t1", MetadataConstants.TYPE_USER);
        tableACL.delete("u1", "t1", MetadataConstants.TYPE_USER);
        Assert.assertNotNull(tableACL.getTableBlackList("u1", EMPTY_GROUP_SET));
        Assert.assertTrue(tableACL.getTableBlackList("u1", EMPTY_GROUP_SET).isEmpty());
        tableACL.add("u1", "t2", MetadataConstants.TYPE_USER);
        Assert.assertEquals(1, tableACL.getTableBlackList("u1", EMPTY_GROUP_SET).size());
    }

    @Test
    public void testGetTableBlackList() {
        TableACL tableACL = new TableACL();
        tableACL.add("u1", "t1", MetadataConstants.TYPE_USER);
        tableACL.add("u1", "t2", MetadataConstants.TYPE_USER);
        tableACL.add("u2", "t1", MetadataConstants.TYPE_USER);
        tableACL.add("g1", "t3", MetadataConstants.TYPE_GROUP);
        tableACL.add("g1", "t4", MetadataConstants.TYPE_GROUP);
        tableACL.add("g1", "t5", MetadataConstants.TYPE_GROUP);
        tableACL.add("g2", "t6", MetadataConstants.TYPE_GROUP);
        Set<String> tableBlackList = tableACL.getTableBlackList("u1", Sets.newHashSet("g1", "g2"));
        Assert.assertEquals(Sets.newHashSet("t1", "t2", "t3", "t4", "t5", "t6"), tableBlackList);
    }

    @Test
    public void testTableACL() {
        TableACL empty = new TableACL();
        try {
            empty.delete("a", "DB.TABLE1", MetadataConstants.TYPE_USER);
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals("Operation fail, can not grant user table query permission.Table ACL DB.TABLE1:a is not found!",
                    e.getMessage());
        }

        //add
        TableACL tableACL = new TableACL();
        tableACL.add("user1", "DB.TABLE1", MetadataConstants.TYPE_USER);
        Assert.assertEquals(1, tableACL.size());

        //add duplicated
        try {
            tableACL.add("user1", "DB.TABLE1", MetadataConstants.TYPE_USER);
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals(
                    "Operation fail, can not revoke user's table query permission.Table ACL DB.TABLE1:user1 already exists!",
                    e.getMessage());
        }

        //add the same name but the type is group.
        tableACL.add("user1", "DB.TABLE1", MetadataConstants.TYPE_GROUP);
        Assert.assertEquals(1, tableACL.size(MetadataConstants.TYPE_GROUP));

        //add
        tableACL.add("user2", "DB.TABLE1", MetadataConstants.TYPE_USER);
        Assert.assertEquals(2, tableACL.size(MetadataConstants.TYPE_USER));

        //delete
        Assert.assertEquals(Sets.newHashSet("DB.TABLE1"), tableACL.getTableBlackList("user2", EMPTY_GROUP_SET));
        tableACL.delete("user2", "DB.TABLE1", MetadataConstants.TYPE_USER);
        Assert.assertEquals(0, tableACL.getTableBlackList("user2", EMPTY_GROUP_SET).size());

        //delete user's all table
        tableACL.delete("user1", MetadataConstants.TYPE_USER);
        Assert.assertEquals(0, tableACL.getTableBlackList("user1", EMPTY_GROUP_SET).size());
    }
}
