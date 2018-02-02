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
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.metadata.MetadataConstants;
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
    public void testCaseIns() throws IOException {
        tableACLService.addToTableACL(PROJECT, "ADMIN", "DB.TABLE", MetadataConstants.TYPE_USER);
        List<String> noAccessList = tableACLService.getNoAccessList(PROJECT, "db.table", MetadataConstants.TYPE_USER);
        System.out.println(noAccessList);
    }

    @Test
    public void testTableACL() throws IOException {
        tableACLService.addToTableACL(PROJECT, "ADMIN", "DB.TABLE", MetadataConstants.TYPE_USER);
        tableACLService.addToTableACL(PROJECT, "ADMIN", "DB.TABLE1", MetadataConstants.TYPE_USER);
        tableACLService.addToTableACL(PROJECT, "ADMIN", "DB.TABLE2", MetadataConstants.TYPE_USER);
        tableACLService.addToTableACL(PROJECT, "MODELER", "DB.TABLE4", MetadataConstants.TYPE_USER);
        tableACLService.addToTableACL(PROJECT, "MODELER", "DB.TABLE1", MetadataConstants.TYPE_USER);
        tableACLService.addToTableACL(PROJECT, "MODELER", "DB.TABLE", MetadataConstants.TYPE_USER);
        tableACLService.addToTableACL(PROJECT, "ANALYST", "DB.TABLE", MetadataConstants.TYPE_USER);
        tableACLService.addToTableACL(PROJECT, "ANALYST", "DB.TABLE1", MetadataConstants.TYPE_USER);
        tableACLService.addToTableACL(PROJECT, "ANALYST", "DB.TABLE2", MetadataConstants.TYPE_USER);
        tableACLService.addToTableACL(PROJECT, "ANALYST", "DB.TABLE4", MetadataConstants.TYPE_USER);
        List<String> tableBlackList = tableACLService.getNoAccessList(PROJECT, "DB.TABLE1", MetadataConstants.TYPE_USER);
        Assert.assertEquals(3, tableBlackList.size());

        //test get black/white list
        Set<String> allUsers = new TreeSet<>();
        allUsers.add("ADMIN");
        allUsers.add("MODELER");
        allUsers.add("ANALYST");
        allUsers.add("user4");
        allUsers.add("user5");
        allUsers.add("user6");
        allUsers.add("user7");

        List<String> tableWhiteList = tableACLService.getCanAccessList(PROJECT, "DB.TABLE1", allUsers, MetadataConstants.TYPE_USER);
        Assert.assertEquals(4, tableWhiteList.size());

        List<String> emptyTableBlackList = tableACLService.getNoAccessList(PROJECT, "DB.T", MetadataConstants.TYPE_USER);
        Assert.assertEquals(0, emptyTableBlackList.size());

        List<String> tableWhiteList1 = tableACLService.getCanAccessList(PROJECT, "DB.T", allUsers, MetadataConstants.TYPE_USER);
        Assert.assertEquals(7, tableWhiteList1.size());

        //test add
        tableACLService.addToTableACL(PROJECT, "user7", "DB.T7", MetadataConstants.TYPE_USER);
        List<String> tableBlackList2 = tableACLService.getNoAccessList(PROJECT, "DB.T7", MetadataConstants.TYPE_USER);
        Assert.assertTrue(tableBlackList2.contains("user7"));

        //test delete
        tableACLService.deleteFromTableACL(PROJECT, "user7", "DB.T7", "user");
        List<String> tableBlackList3 = tableACLService.getNoAccessList(PROJECT, "DB.T7", MetadataConstants.TYPE_USER);
        Assert.assertFalse(tableBlackList3.contains("user7"));

        //test delete
        Assert.assertEquals(3, tableACLService.getNoAccessList(PROJECT, "DB.TABLE1", MetadataConstants.TYPE_USER).size());
        tableACLService.deleteFromTableACL(PROJECT, "ADMIN", MetadataConstants.TYPE_USER);
        Assert.assertEquals(2, tableACLService.getNoAccessList(PROJECT, "DB.TABLE1", MetadataConstants.TYPE_USER).size());
    }

}
