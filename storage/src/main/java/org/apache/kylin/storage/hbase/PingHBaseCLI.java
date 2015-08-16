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

package org.apache.kylin.storage.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;

/**
 * @author yangli9
 * 
 */
public class PingHBaseCLI {

    public static void main(String[] args) throws IOException, InterruptedException {
        String hbaseTable = args[0];

        System.out.println("Hello friend.");

        Configuration hconf = HadoopUtil.getCurrentHBaseConfiguration();
        if (User.isHBaseSecurityEnabled(hconf)) {
            Connection conn = ConnectionFactory.createConnection(hconf);
            try {
                UserProvider userProvider = UserProvider.instantiate(hconf);
                TokenUtil.obtainAndCacheToken(conn, userProvider.create(UserGroupInformation.getCurrentUser()));
            } finally {
                conn.close();
            }
        }

        Scan scan = new Scan();
        int limit = 20;

        Connection conn = null;
        Table table = null;
        ResultScanner scanner = null;
        try {
            conn = ConnectionFactory.createConnection(hconf);
            table = conn.getTable(TableName.valueOf(hbaseTable));
            scanner = table.getScanner(scan);
            int count = 0;
            for (Result r : scanner) {
                byte[] rowkey = r.getRow();
                System.out.println(Bytes.toStringBinary(rowkey));
                count++;
                if (count == limit)
                    break;
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            if (table != null) {
                table.close();
            }
            if (conn != null) {
                conn.close();
            }
        }

    }
}
