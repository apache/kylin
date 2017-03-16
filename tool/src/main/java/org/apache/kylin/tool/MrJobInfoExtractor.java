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
import java.util.Map;
import java.util.Set;
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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class MrJobInfoExtractor extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(MrJobInfoExtractor.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_DETAILS = OptionBuilder.withArgName("includeTasks").hasArg().isRequired(false).withDescription("Specify whether to include mr task details to extract. Default true.").create("includeTasks");

    @SuppressWarnings("static-access")
    private static final Option OPTION_MR_JOB_ID = OptionBuilder.withArgName("mrJobId").hasArg().isRequired(false).withDescription("Specify MR Job Id").create("mrJobId");

    private static final int HTTP_RETRY = 3;

    private Map<String, String> nodeInfoMap = Maps.newHashMap();

    private String jobHistoryUrlBase;
    private String yarnMasterUrlBase;

    public MrJobInfoExtractor() {
        packageType = "MR";

        options.addOption(OPTION_INCLUDE_DETAILS);
        options.addOption(OPTION_MR_JOB_ID);
    }

    public static void main(String[] args) {
        MrJobInfoExtractor extractor = new MrJobInfoExtractor();
        extractor.execute(args);
    }

    private void extractRestCheckUrl() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        final String yarnStatusCheckUrl = config.getYarnStatusCheckUrl();
        Pattern pattern = Pattern.compile("(http://)([^:]*):([^/])*.*");
        if (yarnStatusCheckUrl != null) {
            Matcher m = pattern.matcher(yarnStatusCheckUrl);
            if (m.matches()) {
                jobHistoryUrlBase = m.group(1) + m.group(2) + ":19888";
                yarnMasterUrlBase = m.group(1) + m.group(2) + ":" + m.group(3);
            }
        }
        logger.info("kylin.engine.mr.yarn-check-status-url" + " is not set, read from hadoop configuration");

        Configuration conf = HadoopUtil.getCurrentConfiguration();
        String rmWebHost = HAUtil.getConfValueForRMInstance(YarnConfiguration.RM_WEBAPP_ADDRESS, YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS, conf);
        if (HAUtil.isHAEnabled(conf)) {
            YarnConfiguration yarnConf = new YarnConfiguration(conf);
            String active = RMHAUtils.findActiveRMHAId(yarnConf);
            rmWebHost = HAUtil.getConfValueForRMInstance(HAUtil.addSuffix(YarnConfiguration.RM_WEBAPP_ADDRESS, active), YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS, yarnConf);
        }
        if (StringUtils.isEmpty(rmWebHost)) {
            return;
        }
        if (!rmWebHost.startsWith("http://") && !rmWebHost.startsWith("https://")) {
            rmWebHost = "http://" + rmWebHost;
        }
        Matcher m = pattern.matcher(rmWebHost);
        Preconditions.checkArgument(m.matches(), "Yarn master URL not found.");
        yarnMasterUrlBase = rmWebHost;
        jobHistoryUrlBase = m.group(1) + HAUtil.getConfValueForRMInstance("mapreduce.jobhistory.webapp.address", m.group(2) + ":19888", conf);
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

    private void extractTaskDetail(String taskId, String user, File exportDir, String taskUrl, String urlBase) throws IOException {
        try {
            if (StringUtils.isEmpty(taskId)) {
                return;
            }

            String taskUrlBase = taskUrl + taskId;
            File destDir = new File(exportDir, taskId);

            // get task basic info
            String taskInfo = saveHttpResponseQuietly(new File(destDir, "task.json"), taskUrlBase);
            JsonNode taskAttempt = new ObjectMapper().readTree(taskInfo).path("task").path("successfulAttempt");
            String succAttemptId = taskAttempt.textValue();

            String attemptInfo = saveHttpResponseQuietly(new File(destDir, "task_attempts.json"), taskUrlBase + "/attempts/" + succAttemptId);
            JsonNode attemptAttempt = new ObjectMapper().readTree(attemptInfo).path("taskAttempt");
            String containerId = attemptAttempt.get("assignedContainerId").textValue();
            String nodeId = nodeInfoMap.get(attemptAttempt.get("nodeHttpAddress").textValue());

            // save task counters
            saveHttpResponseQuietly(new File(destDir, "task_counters.json"), taskUrlBase + "/counters");

            // save task logs
            String logUrl = urlBase + "/jobhistory/logs/" + nodeId + "/" + containerId + "/" + succAttemptId + "/" + user + "/syslog/?start=0";
            logger.debug("Fetch task log from url: " + logUrl);

            saveHttpResponseQuietly(new File(destDir, "task_log.txt"), logUrl);
        } catch (Exception e) {
            logger.warn("Failed to get task counters rest response" + e);
        }
    }

    private String saveHttpResponseQuietly(File dest, String url) {
        String response = null;

        try {
            response = getHttpResponse(url);
            FileUtils.forceMkdir(dest.getParentFile());
            FileUtils.writeStringToFile(dest, response, Charset.defaultCharset());
            return response;
        } catch (Exception e) {
            logger.warn("Failed to get http response from {}.", url, e);
        }

        return response;
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        try {
            boolean includeTaskDetails = optionsHelper.hasOption(OPTION_INCLUDE_DETAILS) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_DETAILS)) : true;
            String mrJobId = optionsHelper.getOptionValue(OPTION_MR_JOB_ID);
            extractRestCheckUrl();

            Preconditions.checkNotNull(jobHistoryUrlBase);
            Preconditions.checkNotNull(yarnMasterUrlBase);

            String jobUrlPrefix = jobHistoryUrlBase + "/ws/v1/history/mapreduce/jobs/" + mrJobId;

            // cache node info
            String nodeUrl = yarnMasterUrlBase + "/ws/v1/cluster/nodes";
            String nodeResponse = getHttpResponse(nodeUrl);
            JsonNode nodes = new ObjectMapper().readTree(nodeResponse).path("nodes").path("node");
            for (JsonNode node : nodes) {
                nodeInfoMap.put(node.path("nodeHTTPAddress").textValue(), node.path("id").textValue());
            }

            // save mr job stats
            String jobResponse = saveHttpResponseQuietly(new File(exportDir, "job.json"), jobUrlPrefix);
            String user = new ObjectMapper().readTree(jobResponse).path("job").path("user").textValue();

            // save mr job conf
            saveHttpResponseQuietly(new File(exportDir, "job_conf.json"), jobUrlPrefix + "/conf");

            // save mr job counters
            saveHttpResponseQuietly(new File(exportDir, "job_counters.json"), jobUrlPrefix + "/counters");

            // save task details
            if (includeTaskDetails) {
                extractTaskDetails(exportDir, jobUrlPrefix, jobHistoryUrlBase, user);
            }

        } catch (Exception e) {
            logger.warn("Failed to get mr tasks rest response.", e);
        }
    }

    private void extractTaskDetails(File exportDir, String jobUrlPrefix, String jobUrlBase, String user) {
        try {
            String tasksUrl = jobUrlPrefix + "/tasks/";
            String tasksResponse = saveHttpResponseQuietly(new File(exportDir, "job_tasks.json"), tasksUrl);
            JsonNode tasks = new ObjectMapper().readTree(tasksResponse).path("tasks").path("task");

            // find the first start map and reduce
            String firstStartMapId = null;
            String firstStartReduceId = null;
            long firstStartMapTime = Long.MAX_VALUE;
            long firstStartReduceTime = Long.MAX_VALUE;

            // find the first end map and reduce
            String firstEndMapId = null;
            String firstEndReduceId = null;
            long firstEndMapTime = Long.MAX_VALUE;
            long firstEndReduceTime = Long.MAX_VALUE;

            // find the last start map and reduce
            String lastStartMapId = null;
            String lastStartReduceId = null;
            long lastStartMapTime = 0L;
            long lastStartReduceTime = 0L;

            // find the last end map and reduce
            String lastEndMapId = null;
            String lastEndReduceId = null;
            long lastEndMapTime = 0L;
            long lastEndReduceTime = 0L;

            // find the max map and reduce duation
            String maxReduceId = null;
            String maxMapId = null;
            long maxMapElapsedTime = 0L;
            long maxReduceElapsedTime = 0L;

            // find the min map and reduce duration
            String minReduceId = null;
            String minMapId = null;
            long minMapElapsedTime = Long.MAX_VALUE;
            long minReduceElapsedTime = Long.MAX_VALUE;

            Set<String> selectedTaskIds = Sets.newHashSet();
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

                    if (node.get("startTime").longValue() <= firstStartMapTime) {
                        firstStartMapTime = node.get("startTime").longValue();
                        firstStartMapId = node.get("id").textValue();
                    }

                    if (node.get("startTime").longValue() >= lastStartMapTime) {
                        lastStartMapTime = node.get("startTime").longValue();
                        lastStartMapId = node.get("id").textValue();
                    }

                    if (node.get("finishTime").longValue() <= firstEndMapTime) {
                        firstEndMapTime = node.get("finishTime").longValue();
                        firstEndMapId = node.get("id").textValue();
                    }

                    if (node.get("finishTime").longValue() >= lastEndMapTime) {
                        lastEndMapTime = node.get("finishTime").longValue();
                        lastEndMapId = node.get("id").textValue();
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

                    if (node.get("startTime").longValue() <= firstStartReduceTime) {
                        firstStartReduceTime = node.get("startTime").longValue();
                        firstStartReduceId = node.get("id").textValue();
                    }

                    if (node.get("startTime").longValue() >= lastStartReduceTime) {
                        lastStartReduceTime = node.get("startTime").longValue();
                        lastStartReduceId = node.get("id").textValue();
                    }

                    if (node.get("finishTime").longValue() <= firstEndReduceTime) {
                        firstEndReduceTime = node.get("finishTime").longValue();
                        firstEndReduceId = node.get("id").textValue();
                    }

                    if (node.get("finishTime").longValue() >= lastEndReduceTime) {
                        lastEndReduceTime = node.get("finishTime").longValue();
                        lastEndReduceId = node.get("id").textValue();
                    }
                }
            }

            selectedTaskIds.add(maxMapId);
            selectedTaskIds.add(maxReduceId);
            selectedTaskIds.add(minMapId);
            selectedTaskIds.add(minReduceId);
            selectedTaskIds.add(firstStartMapId);
            selectedTaskIds.add(firstStartReduceId);
            selectedTaskIds.add(lastStartMapId);
            selectedTaskIds.add(lastStartReduceId);
            selectedTaskIds.add(firstEndMapId);
            selectedTaskIds.add(firstEndReduceId);
            selectedTaskIds.add(lastEndMapId);
            selectedTaskIds.add(lastEndReduceId);

            File tasksDir = new File(exportDir, "tasks");
            FileUtils.forceMkdir(tasksDir);
            for (String taskId : selectedTaskIds) {
                extractTaskDetail(taskId, user, tasksDir, tasksUrl, jobUrlBase);
            }
        } catch (Exception e) {
            logger.warn("Failed to get mr tasks rest response.", e);
        }
    }
}