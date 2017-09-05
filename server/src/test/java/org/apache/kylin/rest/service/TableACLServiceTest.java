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

package org.apache.kylin.rest.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.metadata.acl.TableACL;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

public class TableACLServiceTest extends ServiceTestBase {
    private final static String PROJECT = "learn_kylin";

    @Autowired
    @Qualifier("TableAclService")
    private TableACLService tableACLService;

    @Test
    public void testTableACL() throws IOException {
        TableACL emptyBlackList = tableACLService.getTableACLByProject(PROJECT);
        Assert.assertEquals(0, emptyBlackList.getUserTableBlackList().size());

        tableACLService.addToTableBlackList(PROJECT, "ADMIN", "DB.TABLE");
        tableACLService.addToTableBlackList(PROJECT, "ADMIN", "DB.TABLE1");
        tableACLService.addToTableBlackList(PROJECT, "ADMIN", "DB.TABLE2");
        tableACLService.addToTableBlackList(PROJECT, "MODELER", "DB.TABLE4");
        tableACLService.addToTableBlackList(PROJECT, "MODELER", "DB.TABLE1");
        tableACLService.addToTableBlackList(PROJECT, "MODELER", "DB.TABLE");
        tableACLService.addToTableBlackList(PROJECT, "ANALYST", "DB.TABLE");
        tableACLService.addToTableBlackList(PROJECT, "ANALYST", "DB.TABLE1");
        tableACLService.addToTableBlackList(PROJECT, "ANALYST", "DB.TABLE2");
        tableACLService.addToTableBlackList(PROJECT, "ANALYST", "DB.TABLE4");
        List<String> tableBlackList = tableACLService.getBlockedUserByTable(PROJECT, "DB.TABLE1");
        Assert.assertEquals(3, tableBlackList.size());

        //test get black/white list
        List<String> allUsers = new ArrayList<>();
        allUsers.add("ADMIN");
        allUsers.add("MODELER");
        allUsers.add("ANALYST");
        allUsers.add("user4");
        allUsers.add("user5");
        allUsers.add("user6");
        allUsers.add("user7");
        List<String> tableWhiteList = tableACLService.getTableWhiteListByTable(PROJECT, "DB.TABLE1", allUsers);
        Assert.assertEquals(4, tableWhiteList.size());

        List<String> emptyTableBlackList = tableACLService.getBlockedUserByTable(PROJECT, "DB.T");
        Assert.assertEquals(0, emptyTableBlackList.size());

        List<String> tableWhiteList1 = tableACLService.getTableWhiteListByTable(PROJECT, "DB.T", allUsers);
        Assert.assertEquals(7, tableWhiteList1.size());

        //test add
        tableACLService.addToTableBlackList(PROJECT, "user7", "DB.T7");
        List<String> tableBlackList2 = tableACLService.getBlockedUserByTable(PROJECT, "DB.T7");
        Assert.assertTrue(tableBlackList2.contains("user7"));

        //test delete
        tableACLService.deleteFromTableBlackList(PROJECT, "user7", "DB.T7");
        List<String> tableBlackList3 = tableACLService.getBlockedUserByTable(PROJECT, "DB.T7");
        Assert.assertFalse(tableBlackList3.contains("user7"));

    }

}
