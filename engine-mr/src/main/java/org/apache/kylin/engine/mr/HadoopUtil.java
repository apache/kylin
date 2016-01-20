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
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.io.Writable;
import org.apache.kylin.common.KylinConfig;
import org.apache.log4j.Logger;

public class HadoopUtil {
    private static final ThreadLocal<Configuration> hadoopConfig = new ThreadLocal<>();
    private static final Logger logger = Logger.getLogger(HadoopUtil.class);

    public static void setCurrentConfiguration(Configuration conf) {
        hadoopConfig.set(conf);
    }

    public static Configuration getCurrentConfiguration() {
        if (hadoopConfig.get() == null) {
            Configuration conf = healSickConfig(new Configuration());
            
            hadoopConfig.set(conf);
        }
        return hadoopConfig.get();
    }

    private static Configuration healSickConfig(Configuration conf) {
        // why we have this hard code?
        conf.set(DFSConfigKeys.DFS_CLIENT_BLOCK_WRITE_LOCATEFOLLOWINGBLOCK_RETRIES_KEY, "8");

        // https://issues.apache.org/jira/browse/KYLIN-953
        if (StringUtils.isBlank(conf.get("hadoop.tmp.dir"))) {
            conf.set("hadoop.tmp.dir", "/tmp");
        }
        if (StringUtils.isBlank(conf.get("hbase.fs.tmp.dir"))) {
            conf.set("hbase.fs.tmp.dir", "/tmp");
        }
        return conf;
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
    
    public static Configuration convertCurrentConfig(String path) {
        Configuration currentConfig = getCurrentConfiguration();
        if(path == null)
            return currentConfig;
        String nameService = currentConfig.get(FileSystem.FS_DEFAULT_NAME_KEY);
        logger.debug("Current convert path " + path);
        logger.debug("Current default name service " + nameService);
        try {
            URI pathUri = new URI(path);
            String host = pathUri.getHost();
            //do not transform path with default name service.
            if(nameService != null) {
                URI defaultUri = new URI(nameService);
                if(pathUri.getScheme().equalsIgnoreCase(defaultUri.getScheme()) && host.equalsIgnoreCase(defaultUri.getHost())) {
                    return currentConfig;
                }
            }
            //get namespace to real name node map..
            Map<String, Map<String, InetSocketAddress>> map = DFSUtil.getHaNnRpcAddresses(currentConfig);
            Map<String, InetSocketAddress> addressesInNN = map.get(host);
            //if do not exist this namespace, such as we use real name node
            if(addressesInNN == null)
                return currentConfig;
            for(InetSocketAddress addr : addressesInNN.values()) {
                String name = addr.getHostName();
                int port = addr.getPort();
                String target = String.format("%s://%s:%d/", HdfsConstants.HDFS_URI_SCHEME, name, port);
                Configuration tmpConfig = new Configuration();
                tmpConfig.set(FileSystem.FS_DEFAULT_NAME_KEY, target);
                FileSystem tmpFs = FileSystem.get(tmpConfig);
                try {
                	//try every name node address test whether it is standby server.
                    tmpFs.listFiles(new Path("/"), false);
                    logger.debug("Transform path nameservice " + host + " to real name node " + target);
                    return tmpConfig;
                } catch (Exception e) {
                    logger.warn(String.format("Transforming hadoop namenode %s, real host %s is standby server !", 
                            nameService, target));
                    continue;
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot create FileSystem from current hbase cluster conf", e);
        } catch (URISyntaxException e1) {
            throw new IllegalArgumentException("Cannot create path to URI", e1);
        }
        return currentConfig;
    }

    /*
     * transform a path without default name service(such as path in hbase hdfs, hfile location) 
     * to path point to master name node.
     * @param path to transform, such as hbase location.
     * @return a equal path with physical domain name which is master namenode.
     */
    public static Path transformPathToNN(Path path) {        
        //without schema, using default config
        if(path == null || !path.toString().startsWith(HdfsConstants.HDFS_URI_SCHEME))
            return path;
        
        try {
            URI uri = new URI(path.toString());
            String rawPath = uri.getRawPath();
            //try to convert default filesystem to master nn.
            Configuration newConfig = convertCurrentConfig(path.toString());
            if(newConfig == null) {
                newConfig = getCurrentConfiguration();
            }
            FileSystem fs = FileSystem.get(newConfig);
            return fs.makeQualified(new Path(rawPath));
        } catch (Exception e) {
            logger.warn("Transform path " + path + " error !", e);
            return null;
        }
    }
}
