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

package org.apache.kylin.storage.hbase.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.storage.hbase.HBaseConnection;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class HBaseUsage {

    public static void main(String[] args) throws IOException {
        show();
    }

    private static void show() throws IOException {
        Map<String, List<String>> envs = Maps.newHashMap();

        // get all kylin hbase tables
        Connection conn = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl());
        Admin hbaseAdmin = conn.getAdmin();
        String tableNamePrefix = IRealizationConstants.SharedHbaseStorageLocationPrefix;
        HTableDescriptor[] tableDescriptors = hbaseAdmin.listTables(tableNamePrefix + ".*");
        for (HTableDescriptor desc : tableDescriptors) {
            String host = desc.getValue(IRealizationConstants.HTableTag);
            if (StringUtils.isEmpty(host)) {
                add("unknown", desc.getNameAsString(), envs);
            } else {
                add(host, desc.getNameAsString(), envs);
            }
        }

        for (Map.Entry<String, List<String>> entry : envs.entrySet()) {
            System.out.println(entry.getKey() + " has htable count: " + entry.getValue().size());
        }
        hbaseAdmin.close();
    }

    private static void add(String tag, String tableName, Map<String, List<String>> envs) {
        if (!envs.containsKey(tag)) {
            envs.put(tag, Lists.<String> newArrayList());
        }
        envs.get(tag).add(tableName);
    }
}
