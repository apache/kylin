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

package org.apache.kylin.common.persistence;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yangli9
 * 
 */
public class HBaseConnection {

    private static final Logger logger = LoggerFactory.getLogger(HBaseConnection.class);

    private static final Map<String, Configuration> ConfigCache = new ConcurrentHashMap<String, Configuration>();
    private static final Map<String, Connection> ConnPool = new ConcurrentHashMap<String, Connection>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (Connection conn : ConnPool.values()) {
                    try {
                        conn.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }
    
    public static Connection get() {
        return get(KylinConfig.getInstanceFromEnv().getStorageUrl());
    }

    public static Connection get(String url) {
        // find configuration
        Configuration conf = ConfigCache.get(url);
        if (conf == null) {
            conf = HadoopUtil.newHBaseConfiguration(url);
            ConfigCache.put(url, conf);
        }

        Connection connection = ConnPool.get(url);
        try {
            // I don't use DCL since recreate a connection is not a big issue.
            if (connection == null) {
                connection = ConnectionFactory.createConnection(conf);
                ConnPool.put(url, connection);
            }
        } catch (Throwable t) {
            throw new StorageException("Error when open connection " + url, t);
        }

        return connection;
    }

    public static void createHTableIfNeeded(String hbaseUrl, String tableName, String... families) throws IOException {
        createHTableIfNeeded(HBaseConnection.get(hbaseUrl), tableName, families);
    }

    public static void createHTableIfNeeded(Connection conn, String tableName, String... families) throws IOException {
        Admin admin = conn.getAdmin();

        try {
            boolean tableExist = false;
            try {
                admin.getTableDescriptor(TableName.valueOf(tableName));
                tableExist = true;
            } catch (TableNotFoundException e) {
            }

            if (tableExist) {
                logger.debug("HTable '" + tableName + "' already exists");
                return;
            }

            logger.debug("Creating HTable '" + tableName + "'");

            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));

            if (null != families && families.length > 0) {
                for (String family : families) {
                    HColumnDescriptor fd = new HColumnDescriptor(family);
                    fd.setInMemory(true); // metadata tables are best in memory
                    desc.addFamily(fd);
                }
            }
            admin.createTable(desc);

            logger.debug("HTable '" + tableName + "' created");
        } finally {
            admin.close();
        }
    }
}
