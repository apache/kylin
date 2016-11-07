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
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.RMHAUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MrJobInfoExtractor extends AbstractInfoExtractor {
    private String mrJobId;
    private String jobUrlPrefix;

    private static final Logger logger = LoggerFactory.getLogger(MrJobInfoExtractor.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_COUNTERS = OptionBuilder.withArgName("includeCounters").hasArg().isRequired(false).withDescription("Specify whether to include mr task counters to extract. Default false.").create("includeCounters");

    private final int HTTP_RETRY = 3;

    public MrJobInfoExtractor(String mrJobId) {
        this.mrJobId = mrJobId;
        String historyServerUrl = getRestCheckUrl();
        this.jobUrlPrefix = historyServerUrl + "/ws/v1/history/mapreduce/jobs/" + mrJobId;
    }

    private String getRestCheckUrl() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        final String yarnStatusCheckUrl = config.getYarnStatusCheckUrl();
        Pattern pattern = Pattern.compile("(http://)(.*):.*");
        if (yarnStatusCheckUrl != null) {
            Matcher m = pattern.matcher(yarnStatusCheckUrl);
            if (m.matches()) {
                return m.group(1) + m.group(2) + ":19888";
            }
        }
        logger.info("kylin.job.yarn.app.rest.check.status.url" + " is not set read from hadoop configuration");

        Configuration conf = HadoopUtil.getCurrentConfiguration();
        String rmWebHost = HAUtil.getConfValueForRMInstance(YarnConfiguration.RM_WEBAPP_ADDRESS, YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS, conf);
        if (HAUtil.isHAEnabled(conf)) {
            YarnConfiguration yarnConf = new YarnConfiguration(conf);
            String active = RMHAUtils.findActiveRMHAId(yarnConf);
            rmWebHost = HAUtil.getConfValueForRMInstance(HAUtil.addSuffix(YarnConfiguration.RM_WEBAPP_ADDRESS, active), YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS, yarnConf);
        }
        if (StringUtils.isEmpty(rmWebHost)) {
            return null;
        }
        if (rmWebHost.startsWith("http://") || rmWebHost.startsWith("https://")) {
            //do nothing
        } else {
            rmWebHost = "http://" + rmWebHost;
        }
        Matcher m = pattern.matcher(rmWebHost);
        m.matches();
        return m.group(1) + m.group(2) + ":19888";
    }

    private String getHttpResponse(String url) {
        HttpClient client = new HttpClient();
        String response = null;
        int retry_times = 0;
        while (response == null && retry_times < HTTP_RETRY) {
            retry_times++;

            HttpMethod get = new GetMethod(url);
            try {
                get.addRequestHeader("accept", "application/json");
                client.executeMethod(get);
                response = get.getResponseBodyAsString();
            } catch (Exception e) {
                logger.warn("Failed to fetch http response. Retry={}", retry_times, e);
            } finally {
                get.releaseConnection();
            }
        }
        return response;
    }

    private void extractTaskCounter(String taskId, File exportDir, String taskUrl) throws IOException {
        try {
            String response = getHttpResponse(taskUrl + taskId + "/counters");
            FileUtils.writeStringToFile(new File(exportDir, taskId + ".json"), response, Charset.defaultCharset());
        } catch (Exception e) {
            logger.warn("Failed to get task counters rest response" + e);
        }
    }

    private void extractJobConf(File exportDir) throws IOException {
        try {
            String jobResponse = getHttpResponse(jobUrlPrefix);
            JsonNode job = new ObjectMapper().readTree(jobResponse).path("job").get("state");
            String confUrl = jobUrlPrefix + "/conf/";
            String response = getHttpResponse(confUrl);
            FileUtils.writeStringToFile(new File(exportDir, "job_conf.json"), response, Charset.defaultCharset());
        } catch (Exception e) {
            logger.warn("Failed to get job conf rest response.", e);
        }
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        try {
            boolean includeTaskCounter = optionsHelper.hasOption(OPTION_INCLUDE_COUNTERS) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_COUNTERS)) : false;
            if (includeTaskCounter) {
                extractTaskCounters(exportDir);
            }
            extractJobConf(exportDir);
        } catch (Exception e) {
            logger.warn("Failed to get mr tasks rest response.", e);
        }
    }

    private void extractTaskCounters(File exportDir) {
        try {
            String tasksUrl = jobUrlPrefix + "/tasks/";
            String tasksResponse = getHttpResponse(tasksUrl);
            JsonNode tasks = new ObjectMapper().readTree(tasksResponse).path("tasks").path("task");

            String maxReduceId = null;
            String maxMapId = null;
            long maxMapElapsedTime = 0L;
            long maxReduceElapsedTime = 0L;

            for (JsonNode node : tasks) {
                if (node.get("type").textValue().equals("MAP")) {
                    if (node.get("elapsedTime").longValue() >= maxMapElapsedTime) {
                        maxMapElapsedTime = node.get("elapsedTime").longValue();
                        maxMapId = node.get("id").textValue();
                    }
                }
                if (node.get("type").textValue().equals("REDUCE")) {
                    if (node.get("elapsedTime").longValue() >= maxReduceElapsedTime) {
                        maxReduceElapsedTime = node.get("elapsedTime").longValue();
                        maxReduceId = node.get("id").textValue();
                    }
                }
            }
            File counterDir = new File(exportDir, "counters");
            FileUtils.forceMkdir(counterDir);
            extractTaskCounter(maxMapId, counterDir, tasksUrl);
            extractTaskCounter(maxReduceId, counterDir, tasksUrl);
        } catch (Exception e) {
            logger.warn("Failed to get mr tasks rest response" + e);
        }
    }
}
