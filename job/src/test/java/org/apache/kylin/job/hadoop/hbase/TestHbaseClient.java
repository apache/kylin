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

package org.apache.kylin.job.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kylin.common.util.Bytes;

/**
 * Created by hongbin on 5/15/14.
 */
public class TestHbaseClient {

    private static boolean reverse = false;

    public static void foo(int n, int k) {
        int t = k;
        if (n - k < k) {
            t = n - k;
            reverse = true;
        }
        boolean[] flags = new boolean[n];
        inner(flags, 0, t);
    }

    private static void print(boolean[] flags) {
        for (int i = 0; i < flags.length; i++) {
            if (!reverse) {
                if (flags[i])
                    System.out.print("0");
                else
                    System.out.print("1");
            } else {
                if (flags[i])
                    System.out.print("1");
                else
                    System.out.print("0");

            }
        }
        System.out.println();

    }

    private static void inner(boolean[] flags, int start, int remaining) {
        if (remaining <= 0) {
            print(flags);
            return;
        }

        if (flags.length - start < remaining) {
            return;
        }

        // write at flags[start]
        flags[start] = true;
        inner(flags, start + 1, remaining - 1);

        // not write at flags[start]
        flags[start] = false;
        inner(flags, start + 1, remaining);
    }

    public static void main(String[] args) throws IOException {
        foo(6, 5);
        foo(5, 2);
        foo(3, 0);

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hbase_host");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("test1"));
        Put put = new Put(Bytes.toBytes("row1"));

        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));

        table.put(put);
        table.close();
        connection.close();
    }
}
