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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.storage.IStorageProvider;
import org.apache.kylin.common.threadlocal.InternalThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class HadoopUtil {

    public static final String JOB_TMP_ROOT = "/job_tmp";
    public static final String PARQUET_STORAGE_ROOT = "/parquet";
    public static final String DICT_STORAGE_ROOT = "/dict";
    public static final String GLOBAL_DICT_STORAGE_ROOT = DICT_STORAGE_ROOT + "/global_dict";
    public static final String SNAPSHOT_STORAGE_ROOT = "/table_snapshot";
    public static final String TABLE_EXD_STORAGE_ROOT = ResourceStore.TABLE_EXD_RESOURCE_ROOT;
    
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HadoopUtil.class);
    private static final transient InternalThreadLocal<Configuration> hadoopConfig = new InternalThreadLocal<>();
    private HadoopUtil() {
        throw new IllegalStateException("Class HadoopUtil is an utility class !");
    }

    public static void setCurrentConfiguration(Configuration conf) {
        hadoopConfig.set(conf);
    }

    public static Configuration getCurrentConfiguration() {
        if (hadoopConfig.get() == null) {
            Configuration conf = healSickConfig(new Configuration());
            // do not cache this conf, or will affect following mr jobs
            return conf;
        }
        Configuration conf = hadoopConfig.get();
        return conf;
    }

    public static Configuration healSickConfig(Configuration conf) {
        // https://issues.apache.org/jira/browse/KYLIN-953
        if (StringUtils.isBlank(conf.get("hadoop.tmp.dir"))) {
            conf.set("hadoop.tmp.dir", "/tmp");
        }
        if (StringUtils.isBlank(conf.get("hbase.fs.tmp.dir"))) {
            conf.set("hbase.fs.tmp.dir", "/tmp");
        }
        //  https://issues.apache.org/jira/browse/KYLIN-3064
        conf.set("yarn.timeline-service.enabled", "false");
        return conf;
    }

    public static FileSystem getWorkingFileSystem() {
        return getFileSystem(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory());
    }

    public static FileSystem getWorkingFileSystem(Configuration conf) throws IOException {
        Path workingPath = new Path(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory());
        return getFileSystem(workingPath, conf);
    }

    public static FileSystem getReadFileSystem() throws IOException {
        return getFileSystem(KylinConfig.getInstanceFromEnv().getReadHdfsWorkingDirectory());
    }

    public static FileSystem getFileSystem(String path) {
        return getFileSystem(new Path(makeURI(path)));
    }

    public static FileSystem getFileSystem(String path, Configuration conf) throws IOException {
        return getFileSystem(new Path(makeURI(path)), conf);
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

    public static Path getFilterOnlyPath(FileSystem fs, Path baseDir, final String filter) throws IOException {
        if (fs.exists(baseDir) == false) {
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

    public static Path[] getFilteredPath(FileSystem fs, Path baseDir, final String prefix) throws IOException {
        if (fs.exists(baseDir) == false) {
            return null;
        }

        FileStatus[] fileStatus = fs.listStatus(baseDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(prefix);
            }
        });

        Path[] result = new Path[fileStatus.length];
        for (int i = 0; i < fileStatus.length; i++) {
            result[i] = fileStatus[i].getPath();
        }
        return result;
    }

    public static void deleteHDFSMeta(String metaUrl) throws IOException {
        String realHdfsPath = StorageURL.valueOf(metaUrl).getParameter("path");
        HadoopUtil.getFileSystem(realHdfsPath).delete(new Path(realHdfsPath), true);
        logger.info("Delete metadata in HDFS for this job: " + realHdfsPath);
    }

    @SuppressWarnings("deprecation")
    public static void writeToSequenceFile(Configuration conf, String outputPath, Map<String, String> counterMap) throws IOException {
        try (SequenceFile.Writer writer = SequenceFile.createWriter(getWorkingFileSystem(conf), conf, new Path(outputPath), Text.class, Text.class)) {
            for (Map.Entry<String, String> counterEntry : counterMap.entrySet()) {
                writer.append(new Text(counterEntry.getKey()), new Text(counterEntry.getValue()));
            }
        }
    }

    @SuppressWarnings("deprecation")
    public static Map<String, String> readFromSequenceFile(Configuration conf, String inputPath) throws IOException {
        try (SequenceFile.Reader reader = new SequenceFile.Reader(getWorkingFileSystem(conf), new Path(inputPath), conf)) {
            Map<String, String> map = Maps.newHashMap();

            Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            while (reader.next(key, value)) {
                map.put(key.toString(), value.toString());
            }

            return map;
        }
    }

    @SuppressWarnings("deprecation")
    public static List<String> readDistinctColumnValues(Configuration conf, String inputPath) throws IOException {
        try (SequenceFile.Reader reader = new SequenceFile.Reader(HadoopUtil.getWorkingFileSystem(conf), new Path(inputPath), conf)) {
            List<String> values = Lists.newArrayList();

            NullWritable key = (NullWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            while (reader.next(key, value)) {
                values.add(value.toString());
            }

            return values;
        }
    }

    @SuppressWarnings("deprecation")
    public static List<String> readDistinctColumnValues(String inputPath) throws IOException {
        return readDistinctColumnValues(HadoopUtil.getCurrentConfiguration(), inputPath);
    }

    public static Map<String, String> readFromSequenceFile(String inputPath) throws IOException {
        return readFromSequenceFile(getCurrentConfiguration(), inputPath);
    }

    public static boolean isSequenceFile(Configuration conf, Path filePath) {
        try (SequenceFile.Reader reader = new SequenceFile.Reader(getWorkingFileSystem(conf), filePath, conf)) {
            return true;
        } catch (Exception e) {
            logger.warn("Read sequence file {} failed.", filePath.getName(), e);
            return false;
        }
    }

    public static boolean isSequenceDir(Configuration conf, Path fileDir) throws IOException {
        FileSystem fs = getWorkingFileSystem(conf);
        FileStatus[] fileStatuses = fs.listStatus(fileDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return !"_SUCCESS".equals(path.getName());
            }
        });

        if (fileStatuses != null && fileStatuses.length > 0) {
            return isSequenceFile(conf, fileStatuses[0].getPath());
        } else {
            return true;
        }
    }

    public static ContentSummary getContentSummary(FileSystem fileSystem, Path path) throws IOException {
        IStorageProvider provider = (IStorageProvider) ClassUtil
                .newInstance(KylinConfig.getInstanceFromEnv().getStorageProvider());
        logger.debug("Use provider:{}", provider.getClass().getCanonicalName());
        return provider.getContentSummary(fileSystem, path);
    }
}
