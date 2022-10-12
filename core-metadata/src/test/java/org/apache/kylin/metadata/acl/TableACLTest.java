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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class TableACLTest {
    private static Set<String> EMPTY_GROUP_SET = new HashSet<>();

    @Test
    void testCaseInsensitive() {
        TableACL tableACL = new TableACL();
        tableACL.add("u1", "t1", MetadataConstants.TYPE_USER);
        try {
            tableACL.add("u1", "T1", MetadataConstants.TYPE_USER);
            Assertions.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assertions.assertEquals(
                    "Operation fail, can not revoke user's table query permission.Table ACL T1:u1 already exists!",
                        e.getMessage());
        }

        Assertions.assertEquals(1, tableACL.getTableBlackList("u1", EMPTY_GROUP_SET).size());
        Assertions.assertTrue(tableACL.getTableBlackList("u1", EMPTY_GROUP_SET).contains("T1"));
        tableACL.delete("u1", "T1", MetadataConstants.TYPE_USER);
        Assertions.assertEquals(0, tableACL.getTableBlackList("u1", EMPTY_GROUP_SET).size());
    }

   @Test
    void testDelTableACLByTable() {
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

        Assertions.assertEquals(2, tableACL.size(MetadataConstants.TYPE_USER));
        Assertions.assertEquals(1, tableACL.size(MetadataConstants.TYPE_GROUP));
        Assertions.assertEquals(0, tableACL.getTableBlackList("u1", EMPTY_GROUP_SET).size());
        Assertions.assertEquals(0, tableACL.getTableBlackList("u1", Sets.newHashSet("g1")).size());
        Assertions.assertEquals(0, tableACL.getTableBlackList("u1", Sets.newHashSet("g2")).size());
        Assertions.assertEquals(1, tableACL.getTableBlackList("u1", Sets.newHashSet("g3")).size());
        Assertions.assertEquals(Sets.newHashSet("t2", "t3"), tableACL.getTableBlackList("u2", EMPTY_GROUP_SET));
        Assertions.assertEquals(Sets.newHashSet("t3"), tableACL.getTableBlackList("u3", EMPTY_GROUP_SET));
        Assertions.assertEquals(1, tableACL.size(MetadataConstants.TYPE_GROUP));
    }

    @Test
    void testDeleteToEmpty() {
        TableACL tableACL = new TableACL();
        tableACL.add("u1", "t1", MetadataConstants.TYPE_USER);
        tableACL.delete("u1", "t1", MetadataConstants.TYPE_USER);
        Assertions.assertNotNull(tableACL.getTableBlackList("u1", EMPTY_GROUP_SET));
        Assertions.assertTrue(tableACL.getTableBlackList("u1", EMPTY_GROUP_SET).isEmpty());
        tableACL.add("u1", "t2", MetadataConstants.TYPE_USER);
        Assertions.assertEquals(1, tableACL.getTableBlackList("u1", EMPTY_GROUP_SET).size());
    }

    @Test
    void testGetTableBlackList() {
        TableACL tableACL = new TableACL();
        tableACL.add("u1", "t1", MetadataConstants.TYPE_USER);
        tableACL.add("u1", "t2", MetadataConstants.TYPE_USER);
        tableACL.add("u2", "t1", MetadataConstants.TYPE_USER);
        tableACL.add("g1", "t3", MetadataConstants.TYPE_GROUP);
        tableACL.add("g1", "t4", MetadataConstants.TYPE_GROUP);
        tableACL.add("g1", "t5", MetadataConstants.TYPE_GROUP);
        tableACL.add("g2", "t6", MetadataConstants.TYPE_GROUP);
        Set<String> tableBlackList = tableACL.getTableBlackList("u1", Sets.newHashSet("g1", "g2"));
        Assertions.assertEquals(Sets.newHashSet("t1", "t2", "t3", "t4", "t5", "t6"), tableBlackList);
    }

    @Test
    void testTableACL() {
        TableACL empty = new TableACL();
        try {
            empty.delete("a", "DB.TABLE1", MetadataConstants.TYPE_USER);
            Assertions.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assertions.assertEquals("Operation fail, can not grant user table query permission.Table ACL DB.TABLE1:a is not found!",
                    e.getMessage());
        }

        //add
        TableACL tableACL = new TableACL();
        tableACL.add("user1", "DB.TABLE1", MetadataConstants.TYPE_USER);
        Assertions.assertEquals(1, tableACL.size());

        //add duplicated
        try {
            tableACL.add("user1", "DB.TABLE1", MetadataConstants.TYPE_USER);
            Assertions.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assertions.assertEquals(
                    "Operation fail, can not revoke user's table query permission.Table ACL DB.TABLE1:user1 already exists!",
                    e.getMessage());
        }

        //add the same name but the type is group.
        tableACL.add("user1", "DB.TABLE1", MetadataConstants.TYPE_GROUP);
        Assertions.assertEquals(1, tableACL.size(MetadataConstants.TYPE_GROUP));

        //add
        tableACL.add("user2", "DB.TABLE1", MetadataConstants.TYPE_USER);
        Assertions.assertEquals(2, tableACL.size(MetadataConstants.TYPE_USER));

        //delete
        Assertions.assertEquals(Sets.newHashSet("DB.TABLE1"), tableACL.getTableBlackList("user2", EMPTY_GROUP_SET));
        tableACL.delete("user2", "DB.TABLE1", MetadataConstants.TYPE_USER);
        Assertions.assertEquals(0, tableACL.getTableBlackList("user2", EMPTY_GROUP_SET).size());

        //delete user's all table
        tableACL.delete("user1", MetadataConstants.TYPE_USER);
        Assertions.assertEquals(0, tableACL.getTableBlackList("user1", EMPTY_GROUP_SET).size());
    }
}
