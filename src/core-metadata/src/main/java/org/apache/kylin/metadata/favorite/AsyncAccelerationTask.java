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

package org.apache.kylin.metadata.favorite;

import java.util.Map;

import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.asynctask.AbstractAsyncTask;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Delegate;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class AsyncAccelerationTask extends AbstractAsyncTask {

    @Delegate
    private AccelerationTaskAttributes taskAttributesDelegate;

    public AsyncAccelerationTask(boolean alreadyRunning, Map<String, Boolean> userRefreshedTagMap, String taskType) {
        super(taskType);
        this.setTaskAttributes(new AccelerationTaskAttributes(alreadyRunning, userRefreshedTagMap));
    }

    public static AsyncAccelerationTask copyFromAbstractTask(AbstractAsyncTask task) {
        return JsonUtil.convert(task, AsyncAccelerationTask.class);
    }

    @Override
    public void setTaskAttributes(TaskAttributes taskAttributes) {
        this.taskAttributes = taskAttributes;
        this.taskAttributesDelegate = (AccelerationTaskAttributes) taskAttributes;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AccelerationTaskAttributes extends TaskAttributes {

        public AccelerationTaskAttributes(boolean alreadyRunning, Map<String, Boolean> userRefreshedTagMap) {
            this.alreadyRunning = alreadyRunning;
            this.userRefreshedTagMap = userRefreshedTagMap;
        }

        @JsonProperty("already_running")
        private boolean alreadyRunning;

        @JsonProperty("user_refreshed_Tag")
        private Map<String, Boolean> userRefreshedTagMap;

        //record last task time in RecommendationTopNUpdateScheduler
        @JsonProperty("last_update_topn_time")
        private long lastUpdateTonNTime;
    }
}
