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

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TableACLTest {

    @Test
    public void testCaseInsensitive() {
        TableACL tableACL = new TableACL();
        tableACL.add("u1", "t1");
        try {
            tableACL.add("u1", "T1");
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals(
                    "Operation fail, can not revoke user's table query permission.Table ACL T1:u1 already exists!",
                    e.getMessage());
        }
        Assert.assertEquals(1, tableACL.getTableBlackList("u1").size());
        Assert.assertTrue(tableACL.getTableBlackList("u1").contains("T1"));
        tableACL.delete("u1", "T1");
        Assert.assertEquals(0, tableACL.getTableBlackList("u1").size());
    }

    @Test
    public void testDelTableACLByTable() {
        //delete specific table's ACL
        TableACL tableACL = new TableACL();
        tableACL.add("u1", "t1");
        tableACL.add("u2", "t1");
        tableACL.add("u2", "t2");
        tableACL.add("u2", "t3");
        tableACL.add("u3", "t3");
        tableACL.deleteByTbl("t1");

        TableACL expected = new TableACL();
        expected.add("u2", "t2");
        expected.add("u2", "t3");
        expected.add("u3", "t3");
        Assert.assertEquals(expected, tableACL);
    }

    @Test
    public void testDeleteToEmpty() {
        TableACL tableACL = new TableACL();
        tableACL.add("u1", "t1");
        tableACL.delete("u1", "t1");
        Assert.assertNotNull(tableACL.getTableBlackList("u1"));
        Assert.assertTrue(tableACL.getTableBlackList("u1").isEmpty());
        tableACL.add("u1", "t2");
        Assert.assertEquals(1, tableACL.getTableBlackList("u1").size());
    }

    @Test
    public void testTableACL() {
        TableACL empty = new TableACL();
        try {
            empty.delete("a", "DB.TABLE1");
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals("Operation fail, can not grant user table query permission.Table ACL DB.TABLE1:a is not found!",
                    e.getMessage());
        }

        //add
        TableACL tableACL = new TableACL();
        tableACL.add("user1", "DB.TABLE1");
        Assert.assertEquals(1, tableACL.getUserTableBlackList().size());

        //add duplicated
        try {
            tableACL.add("user1", "DB.TABLE1");
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals(
                    "Operation fail, can not revoke user's table query permission.Table ACL DB.TABLE1:user1 already exists!",
                    e.getMessage());
        }

        //add
        tableACL.add("user2", "DB.TABLE1");
        Assert.assertEquals(2, tableACL.getUserTableBlackList().size());

        //delete
        Assert.assertEquals(Sets.newHashSet("DB.TABLE1"), tableACL.getUserTableBlackList().get("user2").getTables());
        tableACL.delete("user2", "DB.TABLE1");
        Assert.assertNull(tableACL.getUserTableBlackList().get("user2"));

        //delete user's all table
        tableACL.delete("user1");
        Assert.assertNull(tableACL.getUserTableBlackList().get("user1"));
    }
}
