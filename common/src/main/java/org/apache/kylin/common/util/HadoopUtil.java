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

package org.apache.kylin.common.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopUtil {
    private static final Logger logger = LoggerFactory.getLogger(HadoopUtil.class);

    private static ThreadLocal<Configuration> hadoopConfig = new ThreadLocal<>();

    public static void setCurrentConfiguration(Configuration conf) {
        hadoopConfig.set(conf);
    }

    public static Configuration getCurrentConfiguration() {
        if (hadoopConfig.get() == null) {
            hadoopConfig.set(new Configuration());
        }
        return hadoopConfig.get();
    }

    public static FileSystem getFileSystem(String path) throws IOException {
        return FileSystem.get(makeURI(path), getCurrentConfiguration());
    }

    public static URI makeURI(String filePath) {
        try {
            return new URI(filePath);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Cannot create FileSystem from URI: " + filePath, e);
        }
    }

    public static Configuration newHadoopJobConfiguration() {
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY, "8");
        return conf;
    }

    /**
     * e.g.
     * 0. hbase (recommended way)
     * 1. hbase:zk-1.hortonworks.com,zk-2.hortonworks.com,zk-3.hortonworks.com:2181:/hbase-unsecure
     * 2. hbase:zk-1.hortonworks.com,zk-2.hortonworks.com,zk-3.hortonworks.com:2181
     * 3. hbase:zk-1.hortonworks.com:2181:/hbase-unsecure
     * 4. hbase:zk-1.hortonworks.com:2181
     */
    public static Configuration newHBaseConfiguration(String url) {
        Configuration conf = HBaseConfiguration.create();
        // reduce rpc retry
        conf.set(HConstants.HBASE_CLIENT_PAUSE, "3000");
        conf.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "5");
        conf.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "60000");
        // conf.set(ScannerCallable.LOG_SCANNER_ACTIVITY, "true");
        if (StringUtils.isEmpty(url)) {
            return conf;
        }

        // chop off "hbase"
        if (url.startsWith("hbase") == false) {
            throw new IllegalArgumentException("hbase url must start with 'hbase' -- " + url);
        }

        url = StringUtils.substringAfter(url, "hbase");
        if (StringUtils.isEmpty(url)) {
            return conf;
        }

        // case of "hbase:domain.com:2181:/hbase-unsecure"
        Pattern urlPattern = Pattern.compile("[:]((?:[\\w\\-.]+)(?:\\,[\\w\\-.]+)*)[:](\\d+)(?:[:](.+))");
        Matcher m = urlPattern.matcher(url);
        if (m.matches() == false)
            throw new IllegalArgumentException("HBase URL '" + url + "' is invalid, expected url is like '" + "hbase:domain.com:2181:/hbase-unsecure" + "'");

        logger.debug("Creating hbase conf by parsing -- " + url);

        String quorums = m.group(1);
        String quorum = null;
        try {
            String[] tokens = quorums.split(",");
            for (String s : tokens) {
                quorum = s;
                InetAddress.getByName(quorum);
            }
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Zookeeper quorum is invalid: " + quorum + "; urlString=" + url, e);
        }
        conf.set(HConstants.ZOOKEEPER_QUORUM, quorums);

        String port = m.group(2);
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, port);

        String znodePath = m.group(3);
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, znodePath);



        return conf;
    }

    /**
     * 
     * @param table the identifier of hive table, in format <db_name>.<table_name>
     * @return a string array with 2 elements: {"db_name", "table_name"}
     */
    public static String[] parseHiveTableName(String table) {
        int cut = table.indexOf('.');
        String database = cut >= 0 ? table.substring(0, cut).trim() : "DEFAULT";
        String tableName = cut >= 0 ? table.substring(cut + 1).trim() : table.trim();
        
        return new String[] {database, tableName};
    }
}
