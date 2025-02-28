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

package org.apache.kylin.rest.config.initialize;

import java.util.Map;

import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.metrics.MetricsTag;
import org.apache.kylin.common.scheduler.JobAddedNotifier;
import org.apache.kylin.common.scheduler.JobDiscardNotifier;
import org.apache.kylin.common.scheduler.JobReadyNotifier;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.eventbus.Subscribe;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobSchedulerListener {
    @Subscribe
    public void onJobIsReady(JobReadyNotifier notifier) {
        //TODO schedule job immediately
        //        NDefaultScheduler.getInstance(notifier.getProject()).fetchJobsImmediately();
    }

    @Subscribe
    public void onJobAdded(JobAddedNotifier notifier) {
        String project = notifier.getProject();
        MetricsGroup.hostTagCounterInc(MetricsName.JOB, MetricsCategory.PROJECT, project);
        Map<String, String> tags = Maps.newHashMap();
        tags.put(MetricsTag.HOST.getVal(), AddressUtil.getZkLocalInstance());
        tags.put(MetricsTag.JOB_TYPE.getVal(), notifier.getJobType());
        MetricsGroup.counterInc(MetricsName.JOB_COUNT, MetricsCategory.PROJECT, project, tags);
    }

    @Subscribe
    public void onJobDiscard(JobDiscardNotifier notifier) {
        String project = notifier.getProject();
        MetricsGroup.hostTagCounterInc(MetricsName.JOB_DISCARDED, MetricsCategory.PROJECT, project);
        Map<String, String> tags = Maps.newHashMap();
        tags.put(MetricsTag.HOST.getVal(), AddressUtil.getZkLocalInstance());
        tags.put(MetricsTag.JOB_TYPE.getVal(), notifier.getJobType());
        MetricsGroup.counterInc(MetricsName.TERMINATED_JOB_COUNT, MetricsCategory.PROJECT, project, tags);
    }

}
