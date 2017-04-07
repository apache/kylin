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


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.IOUtil;

import java.io.IOException;

public class AclTableMigrationTool {

    public static final String ACL_TABLE_NAME = "_acl";

    public static final String USER_TABLE_NAME = "_user";

    private static final Logger logger = LoggerFactory.getLogger(AclTableMigrationTool.class);

    public static void migrate(ResourceStore store) throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String aclTableName = kylinConfig.getMetadataUrlPrefix() + ACL_TABLE_NAME;
        String userTableName = kylinConfig.getMetadataUrl() + USER_TABLE_NAME;


    }

    public static void convertToResourceStore(KylinConfig kylinConfig, String tableName, ResourceStore store, ResultConverter converter) throws IOException {
        Configuration conf = HBaseConnection.getCurrentHBaseConfiguration();
        Admin hbaseAdmin = new HBaseAdmin(conf);
        if (hbaseAdmin.tableExists(TableName.valueOf(tableName))) {
            Table table = null;
            ResultScanner rs = null;
            Scan scan = new Scan();
            try {
                table = HBaseConnection.get(kylinConfig.getStorageUrl()).getTable(TableName.valueOf(tableName));
                rs = table.getScanner(scan);
                converter.converter(rs, store);
            } finally {
                rs.close();
                IOUtils.closeQuietly(table);
            }
        } else {

        }
    }

    interface ResultConverter {
        void converter(ResultScanner rs, ResourceStore store);
    }
}
