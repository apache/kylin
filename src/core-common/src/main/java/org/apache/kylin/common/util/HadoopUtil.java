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

import static org.apache.kylin.common.exception.ServerErrorCode.FILE_NOT_EXIST;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.storage.IStorageProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;
import lombok.var;

public class HadoopUtil {

    public static final String JOB_TMP_ROOT = "/job_tmp";
    public static final String PARQUET_STORAGE_ROOT = "/parquet";
    public static final String DICT_STORAGE_ROOT = "/dict";
    public static final String GLOBAL_DICT_STORAGE_ROOT = DICT_STORAGE_ROOT + "/global_dict";
    public static final String SNAPSHOT_STORAGE_ROOT = "/table_snapshot";
    public static final String FLAT_TABLE_STORAGE_ROOT = "/flat_table";
    public static final String FAST_BITMAP_SUFFIX = "_fast_bitmap";
    public static final String TABLE_EXD_STORAGE_ROOT = ResourceStore.TABLE_EXD_RESOURCE_ROOT;

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HadoopUtil.class);
    private static final transient ThreadLocal<Configuration> hadoopConfig = new ThreadLocal<>();

    public static void setCurrentConfiguration(Configuration conf) {
        hadoopConfig.set(conf);
    }

    public static final String FILE_PREFIX = "file://";
    public static final String MAPR_FS_PREFIX = "maprfs://";

    public static Configuration getCurrentConfiguration() {
        if (hadoopConfig.get() == null) {
            Configuration conf = healSickConfig(new Configuration());
            // do not cache this conf, or will affect following mr jobs
            return conf;
        }
        Configuration conf = hadoopConfig.get();
        return conf;
    }

    public static Configuration newLocalConfiguration() {
        Configuration conf = new Configuration(false);
        conf.set("fs.default.name", "file:///");
        return conf;
    }

    public static Configuration healSickConfig(Configuration conf) {
        // https://issues.apache.org/jira/browse/KYLIN-953
        if (StringUtils.isBlank(conf.get("hadoop.tmp.dir"))) {
            conf.set("hadoop.tmp.dir", "/tmp");
        }
        //  https://issues.apache.org/jira/browse/KYLIN-3064
        conf.set("yarn.timeline-service.enabled", "false");

        return conf;
    }

    /**
     * extract hadoop properties form kylin spark engine
     * for example :
     * get yarn.client.failover-max-attempts from kylin.engine.spark-conf.spark.hadoop.yarn.client.failover-max-attempts
     * @return
     */
    public static Configuration getHadoopConfFromSparkEngine() {
        val conf = new Configuration(false);
        val hadoopConfDir = getHadoopConfDir();

        conf.addResource(new Path(Paths.get(hadoopConfDir, "core-site.xml").toFile().getAbsolutePath()));
        conf.addResource(new Path(Paths.get(hadoopConfDir, "hdfs-site.xml").toFile().getAbsolutePath()));
        conf.addResource(new Path(Paths.get(hadoopConfDir, "yarn-site.xml").toFile().getAbsolutePath()));
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.getSparkConfigOverride().forEach((key, value) -> {
            val keyPrefix = "spark.hadoop.";
            if (key.startsWith(keyPrefix)) {
                conf.set(key.substring(keyPrefix.length()), value);
            }
        });
        return conf;
    }

    public static String getHadoopConfDir() {
        val config = KylinConfig.getInstanceFromEnv();
        String hadoopConf = System.getProperty("kylin.hadoop.conf.dir");
        if (!config.getBuildConf().isEmpty()) {
            logger.info("write hadoop conf is {} ", config.getBuildConf());
            hadoopConf = config.getBuildConf();
        }

        //used for Driver
        String hadoopConfDirEnv = System.getenv("HADOOP_CONF_DIR");
        if (StringUtils.isEmpty(hadoopConf) && StringUtils.isNotEmpty(hadoopConfDirEnv)) {
            hadoopConf = hadoopConfDirEnv;
        }

        if (StringUtils.isEmpty(hadoopConf) && !config.isUTEnv()) {
            throw new RuntimeException(
                    "kylin_hadoop_conf_dir is empty, check if there's error in the output of 'kylin.sh start'");
        }
        return hadoopConf;
    }

    //add sonar rule:  filesystem.get forbidden
    public static FileSystem getWorkingFileSystem() {
        return getFileSystem(KylinConfig.readSystemKylinConfig().getHdfsWorkingDirectory(null));
    }

    public static FileSystem getWorkingFileSystem(Configuration conf) {
        Path workingPath = new Path(KylinConfig.readSystemKylinConfig().getHdfsWorkingDirectory(null));
        return getFileSystem(workingPath, conf);
    }

    public static FileSystem getWorkingFileSystem(Path path) {
        return getFileSystem(path);
    }

    public static FileSystem getFileSystem(String path) {
        return getFileSystem(new Path(makeURI(path)));
    }

    public static FileSystem getFileSystem(Path path) {
        Configuration conf = getCurrentConfiguration();
        return getFileSystem(path, conf);
    }

    public static FileSystem getFileSystem(Path path, Configuration conf) {
        try {
            return path.getFileSystem(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        if (path.startsWith("C:\\") || path.startsWith("D:\\")) {
            path = "file:///" + path;
        } else if (path.startsWith("C:/") || path.startsWith("D:/")) {
            path = "file:///" + path;
        } else if (path.startsWith(FILE_PREFIX) && !path.startsWith("file:///") && path.contains(":\\")) {
            path = path.replace(FILE_PREFIX, "file:///");
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

    public static boolean deletePath(Configuration conf, Path path) throws IOException {
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        if (fs.exists(path)) {
            return fs.delete(path, true);
        }

        return false;
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

    public static Path getFilterOnlyPath(FileSystem fs, Path baseDir, final String filter) throws IOException {
        if (!fs.exists(baseDir)) {
            return null;
        }

        FileStatus[] fileStatus = fs.listStatus(baseDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(filter);
            }
        });

        if (fileStatus.length == 1) {
            return fileStatus[0].getPath();
        } else {
            return null;
        }
    }

    public static String getBackupFolder(KylinConfig kylinConfig) {
        return kylinConfig.getHdfsWorkingDirectory() + "_backup";
    }

    public static String getPathWithoutScheme(String path) {
        if (path.startsWith(FILE_PREFIX))
            return path;

        if (path.startsWith("file:")) {
            path = path.replace("file:", FILE_PREFIX);
        } else if (path.startsWith(MAPR_FS_PREFIX)) {
            path = path.replace(MAPR_FS_PREFIX, "");
        } else {
            path = Path.getPathWithoutSchemeAndAuthority(new Path(path)).toString() + "/";
        }
        return path;
    }

    public static boolean isHdfsCompatibleSchema(String path, KylinConfig kylinConfig) {
        var schemas = kylinConfig.getHdfsMetaStoreFileSystemSchemas();
        return Arrays.stream(schemas).anyMatch(s -> path.startsWith(s + "://"));
    }

    public static ContentSummary getContentSummary(FileSystem fileSystem, Path path) throws IOException {
        IStorageProvider provider = (IStorageProvider) ClassUtil
                .newInstance(KylinConfig.getInstanceFromEnv().getStorageProvider());
        logger.trace("Use provider:{}", provider.getClass().getCanonicalName());
        return provider.getContentSummary(fileSystem, path);
    }

    public static List<FileStatus> getFileStatusPathsFromHDFSDir(String resPath, boolean isFile) {
        try {
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            Path path = new Path(resPath);
            FileStatus[] fileStatus = fs.listStatus(path);
            if (isFile) {
                return Stream.of(fileStatus).filter(FileStatus::isFile).collect(Collectors.toList());
            } else {
                return Stream.of(fileStatus).filter(FileStatus::isDirectory).collect(Collectors.toList());
            }
        } catch (IOException e) {
            throw new KylinException(FILE_NOT_EXIST,
                    String.format(Locale.ROOT, "get file paths from hdfs [%s] failed!", resPath), e);
        }
    }

    public static void mkdirIfNotExist(String resPath) {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        Path path = new Path(resPath);
        try {
            if (!fs.exists(path)) {
                fs.mkdirs(path);
            }
        } catch (IOException e) {
            ExceptionUtils.rethrow(new IOException(String.format(Locale.ROOT, "mkdir %s error", resPath), e));
        }

    }
}
