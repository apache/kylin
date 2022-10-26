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

import com.alibaba.nacos.common.JustForTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class YarnResourceInfoTool {

    private static final Logger logger = LoggerFactory.getLogger(YarnResourceInfoTool.class);
    private static final String AVAILABLE_VIRTUAL_CORE = "availableVirtualCores";
    private static final String AVAILABLE_MEMORY = "availableMB";

    private Configuration configuration;
    private YarnClient yarnClient;

    public YarnResourceInfoTool() {
        this.configuration = initConfiguration(HadoopUtil.getHadoopConfDir());
        initKerberosENV(configuration);
        this.yarnClient = YarnClient.createYarnClient();
        this.yarnClient.init(configuration);
        this.yarnClient.start();
    }

    @JustForTest
    public YarnResourceInfoTool(YarnClient yarnClient) {
        this.configuration = initConfiguration(HadoopUtil.getHadoopConfDir());
        this.yarnClient = yarnClient;
    }

    public Map<String, Integer> getYarnResourceInfoByQueueName(String queue) throws IOException, YarnException {

        int availableMB = 0;
        int availableVirtualCores = 0;

        Map<String, Integer> clusterMetricsMap = new HashMap<>();

        QueueInfo queueInfo = yarnClient.getQueueInfo(queue);
        if (queueInfo == null) {
            return Collections.emptyMap();
        }

        availableMB += queueInfo.getQueueStatistics().getAvailableMemoryMB();
        availableVirtualCores += queueInfo.getQueueStatistics().getAvailableVCores();

        clusterMetricsMap.put(AVAILABLE_MEMORY, availableMB);
        clusterMetricsMap.put(AVAILABLE_VIRTUAL_CORE, availableVirtualCores);

        logger.info("yarn-client get Queue:{} metrics response: availableMB:{} availableVirtualCores:{}", queue,
                availableMB, availableVirtualCores);
        yarnClient.close();
        return clusterMetricsMap;
    }

    public Map<String, Integer> getYarnResourceInfo() throws IOException, YarnException {

        int availableMB = 0;
        int availableVirtualCores = 0;

        Map<String, Integer> clusterMetricsMap = new HashMap<>();

        List<QueueInfo> rootQueueInfos = yarnClient.getRootQueueInfos();
        for (QueueInfo rootInfo : rootQueueInfos) {
            availableMB += rootInfo.getQueueStatistics().getAvailableMemoryMB();
            availableVirtualCores += rootInfo.getQueueStatistics().getAvailableVCores();
        }
        clusterMetricsMap.put(AVAILABLE_MEMORY, availableMB);
        clusterMetricsMap.put(AVAILABLE_VIRTUAL_CORE, availableVirtualCores);

        logger.info("yarn-client get the whole cluster metrics response: availableMB:{} availableVirtualCores:{}",
                availableMB, availableVirtualCores);
        yarnClient.close();
        return clusterMetricsMap;
    }

    public static Configuration initConfiguration(String confPath) {
        confPath = System.getProperty("kylin.hadoop.conf.dir", confPath);
        logger.info("-------------> hadoop conf dir: {}", confPath);
        Configuration configuration = new Configuration();
        configuration.addResource(new Path(Paths.get(confPath, "core-site.xml").toString()));
        configuration.addResource(new Path(Paths.get(confPath, "hdfs-site.xml").toString()));
        configuration.addResource(new Path(Paths.get(confPath, "yarn-site.xml").toString()));
        return configuration;
    }

    public static void initKerberosENV(Configuration conf) {
        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        System.setProperty("java.security.krb5.conf", kapConfig.getKerberosKrb5ConfPath());
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("sun.security.krb5.debug", "false");
        logger.info("Init Kerberos with Principal:{}, Krb5Conf:{} and KeytabPath:{}", kapConfig.getKerberosPrincipal(),
                kapConfig.getKerberosKrb5ConfPath(), kapConfig.getKerberosKeytabPath());
        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(kapConfig.getKerberosPrincipal(),
                    kapConfig.getKerberosKeytabPath());
        } catch (IOException e) {
            logger.error("Failed init Kerberos", e);
        }
    }
}
