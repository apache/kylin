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
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.kylin.common.KylinConfig;

public class HadoopUtil {

    private static ThreadLocal<Configuration> hadoopConfig = new ThreadLocal<>();

    private static ThreadLocal<Configuration> hbaseConfig = new ThreadLocal<>();

    public static void setCurrentConfiguration(Configuration conf) {
        hadoopConfig.set(conf);
    }

    public static void setCurrentHBaseConfiguration(Configuration conf) {
        hbaseConfig.set(conf);
    }

    public static Configuration getCurrentConfiguration() {
        if (hadoopConfig.get() == null) {
            hadoopConfig.set(newConfiguration());
        }
        return hadoopConfig.get();
    }

    public static Configuration getCurrentHBaseConfiguration() {
        if (hbaseConfig.get() == null) {
            String storageUrl = KylinConfig.getInstanceFromEnv().getStorageUrl();
            hbaseConfig.set(newHBaseConfiguration(storageUrl));
        }
        return hbaseConfig.get();
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

    public static String makeQualifiedPathInHadoopCluster(String path) {
        try {
            FileSystem fs = FileSystem.get(getCurrentConfiguration());
            return fs.makeQualified(new Path(path)).toString();
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot create FileSystem from current hadoop cluster conf", e);
        }
    }

    public static String makeQualifiedPathInHBaseCluster(String path) {
        try {
            FileSystem fs = FileSystem.get(getCurrentHBaseConfiguration());
            return fs.makeQualified(new Path(path)).toString();
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot create FileSystem from current hbase cluster conf", e);
        }
    }

    public static Configuration newConfiguration() {
        Configuration conf = new Configuration();
        return healSickConfig(conf);
    }
    
    public static Configuration newHBaseConfiguration(String url) {
        Configuration conf = HBaseConfiguration.create(getCurrentConfiguration());
        
        // using a hbase:xxx URL is deprecated, instead hbase config is always loaded from hbase-site.xml in classpath
        assert (StringUtils.isEmpty(url) || "hbase".equals(url)) : "for hbase storage, pls set 'kylin.storage.url=hbase' in kylin.properties";

        // support hbase using a different FS
        String hbaseClusterFs = KylinConfig.getInstanceFromEnv().getHBaseClusterFs();
        if (StringUtils.isNotEmpty(hbaseClusterFs)) {
            conf.set(FileSystem.FS_DEFAULT_NAME_KEY, hbaseClusterFs);
        }
        
        // reduce rpc retry
        conf.set(HConstants.HBASE_CLIENT_PAUSE, "3000");
        conf.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "5");
        conf.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "60000");
        // conf.set(ScannerCallable.LOG_SCANNER_ACTIVITY, "true");

        return healSickConfig(conf);
    }

    private static Configuration healSickConfig(Configuration conf) {
        // https://issues.apache.org/jira/browse/KYLIN-953
        if (StringUtils.isBlank(conf.get("hadoop.tmp.dir"))) {
            conf.set("hadoop.tmp.dir", "/tmp");
        }
        if (StringUtils.isBlank(conf.get("hbase.fs.tmp.dir"))) {
            conf.set("hbase.fs.tmp.dir", "/tmp");
        }
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

        return new String[] { database, tableName };
    }
}
