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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.service.AclService;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.storage.hbase.HBaseConnection;

/**
 */
public class RealAclHBaseStorage implements AclHBaseStorage {

    private String hbaseUrl;
    private String aclTableName;
    private String userTableName;

    @Override
    public String prepareHBaseTable(Class<?> clazz) throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String metadataUrl = kylinConfig.getMetadataUrl();
        int cut = metadataUrl.indexOf('@');
        hbaseUrl = cut < 0 ? metadataUrl : metadataUrl.substring(cut + 1);
        String tableNameBase = kylinConfig.getMetadataUrlPrefix();

        if (clazz == AclService.class) {
            aclTableName = tableNameBase + ACL_TABLE_NAME;
            HBaseConnection.createHTableIfNeeded(hbaseUrl, aclTableName, ACL_INFO_FAMILY, ACL_ACES_FAMILY);
            return aclTableName;
        } else if (clazz == UserService.class) {
            userTableName = tableNameBase + USER_TABLE_NAME;
            HBaseConnection.createHTableIfNeeded(hbaseUrl, userTableName, USER_AUTHORITY_FAMILY, QueryService.USER_QUERY_FAMILY);
            return userTableName;
        } else {
            throw new IllegalStateException("prepareHBaseTable for unknown class: " + clazz);
        }
    }

    @Override
    public Table getTable(String tableName) throws IOException {
        if (StringUtils.equals(tableName, aclTableName)) {
            return HBaseConnection.get(hbaseUrl).getTable(TableName.valueOf(aclTableName));
        } else if (StringUtils.equals(tableName, userTableName)) {
            return HBaseConnection.get(hbaseUrl).getTable(TableName.valueOf(userTableName));
        } else {
            throw new IllegalStateException("getTable failed" + tableName);
        }
    }
}
