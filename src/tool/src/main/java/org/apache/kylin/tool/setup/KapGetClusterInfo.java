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

package org.apache.kylin.tool.setup;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.cluster.SchedulerInfoCmdHelper;
import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.tool.util.HadoopConfExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.val;

public class KapGetClusterInfo {
    private static final Logger logger = LoggerFactory.getLogger(KapGetClusterInfo.class);

    private static final String YARN_METRICS_SUFFIX = "/ws/v1/cluster/metrics";
    private static final String AVAILABLE_VIRTUAL_CORE = "availableVirtualCores";
    private static final String AVAILABLE_MEMORY = "availableMB";
    private String fileName = "cluster.info";
    private String queueName = "";
    private Configuration configuration = null;
    private String yarnMasterUrlBase;

    private Map<String, Integer> clusterMetricsMap = new HashMap<>();

    public KapGetClusterInfo() {
    }

    public KapGetClusterInfo(String fileName) {
        this.fileName = fileName;
    }

    public KapGetClusterInfo(String fileName, String queue) {
        this.fileName = fileName;
        this.queueName = queue;
        String confPath = System.getProperty("kylin.hadoop.conf.dir", HadoopUtil.getHadoopConfDir());
        configuration = new Configuration();
        configuration.addResource(new Path(confPath + File.separator + "core-site.xml"));
        configuration.addResource(new Path(confPath + File.separator + "hdfs-site.xml"));
        configuration.addResource(new Path(confPath + File.separator + "yarn-site.xml"));
    }

    public static void main(String[] args) throws IOException, ShellException, YarnException {
        if (args.length < 1) {
            logger.error("Usage: KapGetClusterInfo fileName [queue]");
            Unsafe.systemExit(1);
        }
        KapGetClusterInfo kapSetupConcurrency = args.length >= 2 ? new KapGetClusterInfo(args[0], args[1])
                : new KapGetClusterInfo(args[0]);
        kapSetupConcurrency.getYarnMetrics();
        kapSetupConcurrency.saveToFile();
        Unsafe.systemExit(0);
    }

    public void extractYarnMasterHost() {
        if (yarnMasterUrlBase != null) {
            Matcher m = HadoopConfExtractor.URL_PATTERN.matcher(yarnMasterUrlBase);
            if (m.matches()) {
                return;
            }
        }
        if (this.configuration != null) {
            yarnMasterUrlBase = HadoopConfExtractor.extractYarnMasterUrl(this.configuration);
        } else {
            yarnMasterUrlBase = HadoopConfExtractor.extractYarnMasterUrl(HadoopUtil.getCurrentConfiguration());
        }

    }

    public void getYarnMetrics() throws IOException, ShellException, YarnException {
        extractYarnMasterHost();
        String url = yarnMasterUrlBase + YARN_METRICS_SUFFIX;
        if (StringHelper.validateUrl(url)) {
            throw new IllegalArgumentException("Url contains disallowed chars, url: " + url);
        }
        String command = "curl -s -k --negotiate -u : " + url;
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val patternedLogger = new BufferedLogger(logger);
        val response = config.getCliCommandExecutor().execute(command, patternedLogger).getCmd();
        logger.info("yarn metrics response: {}", response);
        Map<String, Integer> clusterMetricsInfos = null;
        if (response == null) {
            throw new IllegalStateException(
                    "Cannot get yarn metrics with url: " + yarnMasterUrlBase + YARN_METRICS_SUFFIX);
        }

        JsonNode clusterMetrics;
        try {
            clusterMetrics = new ObjectMapper().readTree(response).path("clusterMetrics");
        } catch (Exception e) {
            logger.warn("Failed to get clusterMetrics from cluster.", e);
            try {
                clusterMetrics = new ObjectMapper().readTree(SchedulerInfoCmdHelper.metricsInfo())
                        .path("clusterMetrics");
            } catch (IOException | RuntimeException exception) {
                // If all previous attempts to access RM's REST API through curl cmd have failed
                // try to use YARN SDK to obtain yarn cluster information
                logger.warn("Failed to get clusterMetrics from cluster via SchedulerInfoCmdHelper.", exception);
                YarnResourceInfoTool yarnClusterMetrics = new YarnResourceInfoTool();
                if (this.queueName.equals("")) {
                    clusterMetricsInfos = yarnClusterMetrics.getYarnResourceInfo();
                } else {
                    clusterMetricsInfos = yarnClusterMetrics.getYarnResourceInfoByQueueName(this.queueName);
                }

                if (clusterMetricsInfos == null || clusterMetricsInfos.isEmpty()) {
                    logger.error("The queue:{} is invalid, please check kylin.properties", this.queueName);
                    Unsafe.systemExit(101);
                    return;
                }

                clusterMetricsMap.put(AVAILABLE_VIRTUAL_CORE, clusterMetricsInfos.get(AVAILABLE_VIRTUAL_CORE));
                clusterMetricsMap.put(AVAILABLE_MEMORY, clusterMetricsInfos.get(AVAILABLE_MEMORY));
                return;
            }
        }
        clusterMetricsMap.put(AVAILABLE_VIRTUAL_CORE, clusterMetrics.path(AVAILABLE_VIRTUAL_CORE).intValue());
        clusterMetricsMap.put(AVAILABLE_MEMORY, clusterMetrics.path(AVAILABLE_MEMORY).intValue());
    }

    public void saveToFile() throws IOException {
        File dest = new File(fileName);
        StringBuilder buf = new StringBuilder();
        for (Map.Entry<String, Integer> element : clusterMetricsMap.entrySet()) {
            String input = element.getKey() + "=" + element.getValue();
            input += "\n";
            buf.append(input);
        }
        FileUtils.writeStringToFile(dest, buf.toString(), Charset.defaultCharset());
    }
}
