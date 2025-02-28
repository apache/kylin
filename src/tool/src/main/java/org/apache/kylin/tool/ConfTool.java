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
package org.apache.kylin.tool;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.cluster.NacosClusterManager;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.kylin.tool.restclient.RestClient;
import org.apache.kylin.tool.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;

public class ConfTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private static final Set<String> KYLIN_BIN_INCLUSION = Sets.newHashSet("kylin.sh");

    private ConfTool() {
    }

    public static void extractK8sConf(HttpHeaders headers, File exportDir, String targetServerId) {
        try {
            File confFolder = new File(exportDir, "conf");
            FileUtils.forceMkdir(confFolder);
            ClusterManager clusterManager = SpringContext.getApplicationContext().getBean(ClusterManager.class);
            for (String serverId : NacosClusterManager.SERVER_IDS) {
                if (targetServerId != null && !serverId.equals(targetServerId)) {
                    continue;
                }
                ServerInfoResponse server = clusterManager.getServerById(serverId);
                if (server != null) {
                    Properties properties = fetchConf(headers, server.getHost());
                    File propertiesFile = new File(confFolder, serverId + ".kylin.properties");
                    if (propertiesFile.createNewFile()) {
                        try (FileOutputStream conf = new FileOutputStream(propertiesFile)) {
                            properties.store(conf, "");
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to copy /conf, ", e);
        }
    }

    public static Properties fetchConf(HttpHeaders headers, String host) throws IOException {
        RestClient client = new RestClient(host);
        HttpResponse response = client.forwardGet(headers, "/config/all", false);
        String content = EntityUtils.toString(response.getEntity());
        Properties properties = new Properties();
        if (content != null) {
            properties.putAll((Map<?, ?>) JsonUtil.readValue(content, EnvelopeResponse.class).getData());
        }
        return properties;
    }

    public static void extractConf(File exportDir) {
        try {
            File confDir = new File(ToolUtil.getConfFolder());
            if (confDir.exists()) {
                FileUtils.copyDirectoryToDirectory(confDir, exportDir);
            } else {
                logger.error("Can not find the /conf dir: {}!", confDir.getAbsolutePath());
            }
        } catch (Exception e) {
            logger.warn("Failed to copy /conf, ", e);
        }
    }

    public static void extractHadoopConf(File exportDir) {
        try {
            File hadoopConfDir = new File(ToolUtil.getHadoopConfFolder());
            if (hadoopConfDir.exists()) {
                FileUtils.copyDirectoryToDirectory(hadoopConfDir, exportDir);
            } else {
                logger.error("Can not find the hadoop_conf: {}!", hadoopConfDir.getAbsolutePath());
            }
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            String buildHadoopConf = kylinConfig.getBuildConf();
            if (StringUtils.isNotEmpty(buildHadoopConf)) {
                File buildHadoopConfDir = new File(buildHadoopConf);
                if (buildHadoopConfDir.exists()) {
                    FileUtils.copyDirectoryToDirectory(buildHadoopConfDir, exportDir);
                } else {
                    logger.error("Can not find the write hadoop_conf: {}!", buildHadoopConfDir.getAbsolutePath());
                }
            }
        } catch (Exception e) {
            logger.error("Failed to copy /hadoop_conf, ", e);
        }
    }

    public static void extractBin(File exportDir) {
        File destBinDir = new File(exportDir, "bin");

        try {
            FileUtils.forceMkdir(destBinDir);

            File srcBinDir = new File(ToolUtil.getBinFolder());
            if (srcBinDir.exists()) {
                File[] binFiles = srcBinDir.listFiles();
                if (null != binFiles) {
                    for (File binFile : binFiles) {
                        String binFileName = binFile.getName();
                        if (KYLIN_BIN_INCLUSION.contains(binFileName)) {
                            Files.copy(binFile.toPath(), new File(destBinDir, binFile.getName()).toPath());
                            logger.info("copy file: {} {}", binFiles, destBinDir);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Failed to export bin.", e);
        }
    }

}
