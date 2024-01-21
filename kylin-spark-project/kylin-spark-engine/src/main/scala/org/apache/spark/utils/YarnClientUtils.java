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

package org.apache.spark.utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * @author zhaoliu4
 * @date 2022/11/13
 */
public class YarnClientUtils {
    private static final Logger logger = LoggerFactory.getLogger(YarnClientUtils.class);

    public static void killApplication(String jobId) {
        String applicationId = null;
        try (YarnClient yarnClient = YarnClient.createYarnClient()) {
            Configuration yarnConfiguration = new YarnConfiguration();
            // bug of yarn : https://issues.apache.org/jira/browse/SPARK-15343
            yarnConfiguration.set("yarn.timeline-service.enabled", "false");
            yarnClient.init(yarnConfiguration);
            yarnClient.start();

            Set<String> types = Sets.newHashSet("SPARK");
            EnumSet<YarnApplicationState> states = EnumSet.of(YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
                    YarnApplicationState.SUBMITTED, YarnApplicationState.ACCEPTED, YarnApplicationState.RUNNING);
            List<ApplicationReport> applicationReports = yarnClient.getApplications(types, states);

            if (CollectionUtils.isEmpty(applicationReports)) {
                return;
            }

            for (ApplicationReport report : applicationReports) {
                if (report.getName().equalsIgnoreCase("job_step_" + jobId +  "-01")) {
                    ApplicationId application = report.getApplicationId();
                    applicationId = application.toString();
                    logger.info("try to kill yarn application {} of job {}", applicationId, jobId);
                    yarnClient.killApplication(application);
                    logger.info("kill yarn application {} of job {} SUCCEED.", applicationId, jobId);
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("kill yarn application {} of job {} FAILED.", applicationId, jobId, e);
        }
    }
}
