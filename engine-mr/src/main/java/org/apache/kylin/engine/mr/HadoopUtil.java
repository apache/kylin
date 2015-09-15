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

package org.apache.kylin.engine.mr;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopUtil {
    private static final Logger logger = LoggerFactory.getLogger(HadoopUtil.class);

    private static ThreadLocal<Configuration> hadoopConfig = new ThreadLocal<>();

    private static ThreadLocal<Configuration> hbaseConfig = new ThreadLocal<>();

    public static void setCurrentConfiguration(Configuration conf) {
        hadoopConfig.set(conf);
    }

    public static Configuration getCurrentConfiguration() {
        if (hadoopConfig.get() == null) {
            hadoopConfig.set(new Configuration());
        }
        return hadoopConfig.get();
    }

    public static Configuration getCurrentHBaseConfiguration() {
        if (hbaseConfig.get() == null) {
            Configuration configuration = HBaseConfiguration.create(new Configuration());
            String hbaseClusterFs = KylinConfig.getInstanceFromEnv().getHBaseClusterFs();
            if (StringUtils.isNotEmpty(hbaseClusterFs)) {
                configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, hbaseClusterFs);
            }
            hbaseConfig.set(configuration);
        }
        return hbaseConfig.get();
    }

    public static FileSystem getFileSystem(String path) throws IOException {
        return FileSystem.get(makeURI(path), getCurrentConfiguration());
    }

    public static URI makeURI(String filePath) {
        try {
            return new URI(fixWindowsPath(filePath));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Cannot create FileSystem from URI: " + filePath, e);
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

    public static String fixWindowsPath(String path) {
        // fix windows path
        if (path.startsWith("file://") && !path.startsWith("file:///") && path.contains(":\\")) {
            path = path.replace("file://", "file:///");
        }
        if (path.startsWith("file:///")) {
            path = path.replace('\\', '/');
        }
        return path;
    }

    public static Configuration newHadoopJobConfiguration() {
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY, "8");
        return conf;
    }

    /**
     * @param table the identifier of hive table, in format <db_name>.<table_name>
     * @return a string array with 2 elements: {"db_name", "table_name"}
     */
    public static String[] parseHiveTableName(String table) {
        int cut = table.indexOf('.');
        String database = cut >= 0 ? table.substring(0, cut).trim() : "DEFAULT";
        String tableName = cut >= 0 ? table.substring(cut + 1).trim() : table.trim();

        return new String[] { database, tableName };
    }

    public static void deletePath(Configuration conf, Path path) throws IOException {
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }

    public static byte[] toBytes(Writable writable) {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bout);
            writable.write(out);
            out.close();
            bout.close();
            return bout.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
