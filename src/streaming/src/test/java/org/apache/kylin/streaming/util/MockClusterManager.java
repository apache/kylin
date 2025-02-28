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
package org.apache.kylin.streaming.util;

import java.util.List;
import java.util.Set;

import org.apache.kylin.cluster.AvailableResource;
import org.apache.kylin.cluster.IClusterManager;
import org.apache.kylin.cluster.ResourceInfo;
import org.apache.spark.sql.SparkSession;

public class MockClusterManager implements IClusterManager {

    @Override
    public ResourceInfo fetchMaximumResourceAllocation() {
        return null;
    }

    @Override
    public AvailableResource fetchQueueAvailableResource(String queueName) {
        return null;
    }

    @Override
    public String getBuildTrackingUrl(SparkSession sparkSession) {
        return null;
    }

    @Override
    public void killApplication(String jobStepId) {

    }

    @Override
    public void killApplication(String jobStepPrefix, String jobStepId) {

    }

    @Override
    public boolean isApplicationBeenKilled(String applicationId) {
        return false;
    }

    @Override
    public List<String> getRunningJobs(Set<String> queues) {
        return null;
    }

    @Override
    public ResourceInfo fetchQueueStatistics(String queueName) {
        return null;
    }

    @Override
    public boolean applicationExisted(String jobId) {
        return false;
    }

    @Override
    public String getApplicationNameById(int yarnAppId) {
        return "";
    }

}
