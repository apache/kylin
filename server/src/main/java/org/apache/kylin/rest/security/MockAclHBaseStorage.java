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

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.kylin.rest.service.AclService;
import org.apache.kylin.rest.service.UserService;

/**
 */
public class MockAclHBaseStorage implements AclHBaseStorage {

    private HTableInterface mockedAclTable;
    private HTableInterface mockedUserTable;
    private static final String aclTableName = "MOCK-ACL-TABLE";
    private static final String userTableName = "MOCK-USER-TABLE";

    @Override
    public String prepareHBaseTable(Class clazz) throws IOException {

        if (clazz == AclService.class) {
            mockedAclTable = new MockHTable(aclTableName, ACL_INFO_FAMILY, ACL_ACES_FAMILY);
            return aclTableName;
        } else if (clazz == UserService.class) {
            mockedUserTable = new MockHTable(userTableName, USER_AUTHORITY_FAMILY);
            return userTableName;
        } else {
            throw new IllegalStateException("prepareHBaseTable for unknown class: " + clazz);
        }
    }

    @Override
    public HTableInterface getTable(String tableName) throws IOException {
        if (StringUtils.equals(tableName, aclTableName)) {
            return mockedAclTable;
        } else if (StringUtils.equals(tableName, userTableName)) {
            return mockedUserTable;
        } else {
            throw new IllegalStateException("getTable failed" + tableName);
        }
    }
}
