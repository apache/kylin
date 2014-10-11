/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.common.persistence;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import com.kylinolap.common.util.HadoopUtil;

/**
 * @author yangli9
 * 
 */
public class HBaseConnection {

    private static final Map<String, Configuration> ConfigCache = new ConcurrentHashMap<String, Configuration>();

    private static final Map<String, HConnection> ConnPool = new ConcurrentHashMap<String, HConnection>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (HConnection conn : ConnPool.values()) {
                    try {
                        conn.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    public static HConnection get(String url) {
        // find configuration
        Configuration conf = ConfigCache.get(url);
        if (conf == null) {
            conf = HadoopUtil.newHBaseConfiguration(url);
            ConfigCache.put(url, conf);
        }

        HConnection connection = ConnPool.get(url);
        try {
            // I don't use DCL since recreate a connection is not a big issue.
            if (connection == null) {
                connection = HConnectionManager.createConnection(conf);
                ConnPool.put(url, connection);
            }
        } catch (Throwable t) {
            throw new StorageException("Error when open connection " + url, t);
        }

        return connection;
    }

}
