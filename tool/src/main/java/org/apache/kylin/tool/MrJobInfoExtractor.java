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
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.RMHAUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

public class MrJobInfoExtractor extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(MrJobInfoExtractor.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_COUNTERS = OptionBuilder.withArgName("includeCounters").hasArg().isRequired(false).withDescription("Specify whether to include mr task counters to extract. Default true.").create("includeCounters");

    @SuppressWarnings("static-access")
    private static final Option OPTION_MR_JOB_ID = OptionBuilder.withArgName("mrJobId").hasArg().isRequired(false).withDescription("Specify MR Job Id").create("mrJobId");

    private static final int HTTP_RETRY = 3;

    public MrJobInfoExtractor() {
        packageType = "MR";

        options.addOption(OPTION_INCLUDE_COUNTERS);
        options.addOption(OPTION_MR_JOB_ID);
    }

    public static void main(String[] args) {
        MrJobInfoExtractor extractor = new MrJobInfoExtractor();
        extractor.execute(args);
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
        logger.info("kylin.engine.mr.yarn-check-status-url" + " is not set read from hadoop configuration");

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
        if (!rmWebHost.startsWith("http://") && !rmWebHost.startsWith("https://")) {
            rmWebHost = "http://" + rmWebHost;
        }
        Matcher m = pattern.matcher(rmWebHost);
        Preconditions.checkArgument(m.matches(), "Yarn master URL not found.");
        return m.group(1) + m.group(2) + ":19888";
    }

    private String getHttpResponse(String url) {
        DefaultHttpClient client = new DefaultHttpClient();
        String msg = null;
        int retry_times = 0;
        while (msg == null && retry_times < HTTP_RETRY) {
            retry_times++;

            HttpGet request = new HttpGet(url);
            try {
                request.addHeader("accept", "application/json");
                HttpResponse response = client.execute(request);
                msg = EntityUtils.toString(response.getEntity());
            } catch (Exception e) {
                logger.warn("Failed to fetch http response. Retry={}", retry_times, e);
            } finally {
                request.releaseConnection();
            }
        }
        return msg;
    }

    private void extractTaskCounter(String taskId, File exportDir, String taskUrl, String id) throws IOException {
        try {
            String response = getHttpResponse(taskUrl + taskId + "/counters");
            FileUtils.writeStringToFile(new File(exportDir, id + "_" + taskId + ".json"), response, Charset.defaultCharset());
        } catch (Exception e) {
            logger.warn("Failed to get task counters rest response" + e);
        }
    }

    private void extractJobConf(File exportDir, String jobUrlPrefix) throws IOException {
        try {
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
            boolean includeTaskCounter = optionsHelper.hasOption(OPTION_INCLUDE_COUNTERS) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_COUNTERS)) : true;
            String mrJobId = optionsHelper.getOptionValue(OPTION_MR_JOB_ID);
            String jobUrlPrefix = getRestCheckUrl() + "/ws/v1/history/mapreduce/jobs/" + mrJobId;

            if (includeTaskCounter) {
                extractTaskCounters(exportDir, jobUrlPrefix);
            }
            extractJobCounters(exportDir, jobUrlPrefix);
            extractJobConf(exportDir, jobUrlPrefix);
        } catch (Exception e) {
            logger.warn("Failed to get mr tasks rest response.", e);
        }
    }

    private void extractJobCounters(File exportDir, String jobUrlPrefix) {
        String url = jobUrlPrefix + "/counters";
        String response = getHttpResponse(url);
        try {
            File counterDir = new File(exportDir, "counters");
            FileUtils.forceMkdir(counterDir);
            FileUtils.writeStringToFile(new File(exportDir, "job_counters.json"), response, Charset.defaultCharset());
        } catch (Exception e) {
            logger.warn("Failed to get mr counters rest response.", e);
        }
    }

    private void extractTaskCounters(File exportDir, String jobUrlPrefix) {
        try {
            String tasksUrl = jobUrlPrefix + "/tasks/";
            String tasksResponse = getHttpResponse(tasksUrl);
            JsonNode tasks = new ObjectMapper().readTree(tasksResponse).path("tasks").path("task");

            // find the max map and reduce duation
            String maxReduceId = null;
            String maxMapId = null;
            long maxMapElapsedTime = 0L;
            long maxReduceElapsedTime = 0L;

            // find the min map and reduce duration
            String minReduceId = null;
            String minMapId = null;
            long minMapElapsedTime = Integer.MAX_VALUE;
            long minReduceElapsedTime = Integer.MAX_VALUE;

            // find a normal map and reduce duration (the first one)
            String normReduceId = null;
            String normMapId = null;
            long normMapElapsedTime = 0;
            long normReduceElapsedTime = 0;
            for (JsonNode node : tasks) {
                if (node.get("type").textValue().equals("MAP")) {
                    if (node.get("elapsedTime").longValue() >= maxMapElapsedTime) {
                        maxMapElapsedTime = node.get("elapsedTime").longValue();
                        maxMapId = node.get("id").textValue();
                    }

                    if (node.get("elapsedTime").longValue() <= minMapElapsedTime) {
                        minMapElapsedTime = node.get("elapsedTime").longValue();
                        minMapId = node.get("id").textValue();
                    }

                    if (normMapElapsedTime == 0) {
                        normMapElapsedTime = node.get("elapsedTime").longValue();
                        normMapId = node.get("id").textValue();
                    }
                }
                if (node.get("type").textValue().equals("REDUCE")) {
                    if (node.get("elapsedTime").longValue() >= maxReduceElapsedTime) {
                        maxReduceElapsedTime = node.get("elapsedTime").longValue();
                        maxReduceId = node.get("id").textValue();
                    }

                    if (node.get("elapsedTime").longValue() <= minReduceElapsedTime) {
                        minReduceElapsedTime = node.get("elapsedTime").longValue();
                        minReduceId = node.get("id").textValue();
                    }

                    if (normReduceElapsedTime == 0) {
                        normReduceElapsedTime = node.get("elapsedTime").longValue();
                        normReduceId = node.get("id").textValue();
                    }
                }
            }
            File counterDir = new File(exportDir, "counters");
            FileUtils.forceMkdir(counterDir);
            extractTaskCounter(maxMapId, counterDir, tasksUrl, "max");
            extractTaskCounter(maxReduceId, counterDir, tasksUrl, "max");
            extractTaskCounter(minMapId, counterDir, tasksUrl, "min");
            extractTaskCounter(minReduceId, counterDir, tasksUrl, "min");
            extractTaskCounter(normMapId, counterDir, tasksUrl, "norm");
            extractTaskCounter(normReduceId, counterDir, tasksUrl, "norm");
        } catch (Exception e) {
            logger.warn("Failed to get mr tasks rest response.", e);
        }
    }
}