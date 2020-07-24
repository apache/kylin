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

package org.apache.kylin.tool.migration;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.KylinUserService;
import org.apache.kylin.storage.hbase.HBaseResourceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

public abstract class ClusterUtil {
    private static final Logger logger = LoggerFactory.getLogger(ClusterUtil.class);

    protected final KylinConfig kylinConfig;
    protected final RestClient restClient;
    protected final String hdfsWorkingDirectory;

    protected final Configuration hbaseConf;
    protected final Connection hbaseConn;
    protected final ResourceStore resourceStore;
    protected final Admin hbaseAdmin;

    final Configuration jobConf;
    final FileSystem jobFS;
    final String jobHdfsWorkingDirectoryQualified;
    final FileSystem hbaseFS;
    final String hbaseHdfsWorkingDirectoryQualified;

    public ClusterUtil(String configURI, boolean ifJobFSHAEnabled, boolean ifHBaseFSHAEnabled) throws IOException {
        this.kylinConfig = KylinConfig.createInstanceFromUri(configURI);
        this.restClient = new RestClient(configURI);
        Path hdfsWorkingPath = Path.getPathWithoutSchemeAndAuthority(new Path(kylinConfig.getHdfsWorkingDirectory()));
        String tmpHdfsWorkingDirectory = hdfsWorkingPath.toString();
        this.hdfsWorkingDirectory = tmpHdfsWorkingDirectory.endsWith("/") ? tmpHdfsWorkingDirectory
                : tmpHdfsWorkingDirectory + "/";

        this.jobConf = KylinConfig.getConfigFromString(restClient.getHDFSConfiguration());
        this.jobFS = FileSystem.get(jobConf);
        this.jobHdfsWorkingDirectoryQualified = getQualifiedPath(jobConf, hdfsWorkingDirectory, ifJobFSHAEnabled);

        this.hbaseConf = KylinConfig.getConfigFromString(restClient.getHBaseConfiguration());
        this.hbaseFS = FileSystem.get(hbaseConf);
        this.hbaseHdfsWorkingDirectoryQualified = getQualifiedPath(hbaseConf, hdfsWorkingDirectory, ifHBaseFSHAEnabled);

        this.hbaseConn = ConnectionFactory.createConnection(hbaseConf);
        this.resourceStore = new HBaseResourceStore(kylinConfig) {
            @Override
            protected Connection getConnection() {
                return hbaseConn;
            }

            @Override
            protected Configuration getCurrentHBaseConfiguration() {
                return hbaseConf;
            }
        };
        this.hbaseAdmin = hbaseConn.getAdmin();
    }

    public abstract ProjectInstance getProject(String projName) throws IOException;

    public abstract DictionaryInfo getDictionaryInfo(String dictPath) throws IOException;

    public abstract SnapshotTable getSnapshotTable(String snapshotPath) throws IOException;

    public abstract String getRootDirQualifiedOfHTable(String tableName);

    public ManagedUser getUserDetails(String userKey) throws IOException {
        return resourceStore.getResource(userKey, KylinUserService.SERIALIZER);
    }

    public final RawResource getResource(String resPath) throws IOException {
        return resourceStore.getResource(resPath);
    }

    public String getJobWorkingDirQualified(String jobId) {
        return JobBuilderSupport.getJobWorkingDir(jobHdfsWorkingDirectoryQualified, jobId);
    }

    private static String getQualifiedPath(Configuration conf, String path, boolean ifHAEnabled) throws IOException {
        String hdfsSchema = getReplacedDefaultFS(conf, !ifHAEnabled);
        return hdfsSchema + path;
    }

    private static String getReplacedDefaultFS(Configuration conf, boolean ifNeedReplace) throws IOException {
        String defaultFS = conf.get(FileSystem.FS_DEFAULT_NAME_KEY);
        if (!ifNeedReplace) {
            return defaultFS;
        }

        String nameServices = conf.get("dfs.nameservices");
        if (Strings.isNullOrEmpty(nameServices)) {
            return defaultFS;
        }

        // check whether name service is defined for the default fs
        Set<String> nameServiceSet = Sets.newHashSet(nameServices.split(","));
        String defaultNameService = URI.create(defaultFS).getHost();
        if (!nameServiceSet.contains(defaultNameService)) {
            logger.info("name service {} is not defined among {}", defaultNameService, nameServices);
            return defaultFS;
        }

        // select one usable node as the default fs
        String haHostNames = conf.get("dfs.ha.namenodes." + defaultNameService);
        if (!Strings.isNullOrEmpty(haHostNames)) {
            conf = new Configuration(conf);
            for (String oneNodeAlias : haHostNames.split(",")) {
                String rpcNode = conf.get("dfs.namenode.rpc-address." + defaultNameService + "." + oneNodeAlias);
                String replaced = "hdfs://" + rpcNode;
                conf.set(FileSystem.FS_DEFAULT_NAME_KEY, replaced);

                Path rootPath = new Path(replaced + "/");
                FileSystem fs = FileSystem.get(conf);
                try {
                    fs.getStatus(rootPath);
                } catch (Exception e) {
                    logger.warn("cannot use {} as default fs due to ", replaced, e);
                    continue;
                }
                logger.info("replaced the default fs {} by {}", defaultFS, replaced);
                return replaced;
            }
            throw new IllegalArgumentException("fail to replace the default fs " + defaultFS);
        }
        throw new IllegalArgumentException("dfs.ha.namenodes." + defaultNameService + " is not set");
    }
}
