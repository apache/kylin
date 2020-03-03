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

import org.apache.kylin.engine.spark.utils.BuildUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

public class YarnInfoFetcherUtils {

    private YarnInfoFetcherUtils() {
    }

    public static String getTrackingUrl(String applicationId) throws IOException, YarnException {
        try (YarnClient yarnClient = YarnClient.createYarnClient()) {
            yarnClient.init(BuildUtils.getCurrentYarnConfiguration());
            yarnClient.start();

            String[] array = applicationId.split("_");
            if (array.length < 3) {
                return null;
            }

            ApplicationId appId = ApplicationId.newInstance(Long.valueOf(array[1]), Integer.valueOf(array[2]));
            ApplicationReport applicationReport = yarnClient.getApplicationReport(appId);

            return null == applicationReport ? null : applicationReport.getTrackingUrl();
        } catch (IOException | YarnException e) {
            throw e;
        }
    }

}
