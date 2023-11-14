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

package org.apache.kylin.common.mail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.KylinConfig;

import lombok.Getter;

public enum MailNotificationType {

    JOB_LOAD_EMPTY_DATA("Job Load Empty Data", "JOB_LOAD_EMPTY_DATA", null) {
        @Override
        public boolean needNotify(KylinConfig kylinconfig) {
            return kylinconfig.getJobDataLoadEmptyNotificationEnabled();
        }
    },
    JOB_ERROR("Job Error", "JOB_ERROR", "error") {
        @Override
        public boolean needNotify(KylinConfig kylinconfig) {
            return needNotifyStates(kylinconfig).stream()
                    .anyMatch(item -> getCorrespondingJobStates().equalsIgnoreCase(item));
        }
    },
    JOB_FINISHED("Job Finished", "JOB_FINISHED", "finished") {
        @Override
        public boolean needNotify(KylinConfig kylinconfig) {
            return needNotifyStates(kylinconfig).stream()
                    .anyMatch(item -> getCorrespondingJobStates().equalsIgnoreCase(item));
        }
    },
    JOB_DISCARDED("Job Discarded", "JOB_DISCARDED", "discarded") {
        @Override
        public boolean needNotify(KylinConfig kylinconfig) {
            return needNotifyStates(kylinconfig).stream()
                    .anyMatch(item -> getCorrespondingJobStates().equalsIgnoreCase(item));
        }
    },
    OVER_LICENSE_CAPACITY_THRESHOLD("Over License Capacity Threshold", "OVER_LICENSE_CAPACITY_THRESHOLD", null) {
        @Override
        public boolean needNotify(KylinConfig kylinconfig) {
            return kylinconfig.isOverCapacityNotificationEnabled();
        }
    };

    @Getter
    private final String displayName;

    @Getter
    private final String correspondingJobStates;

    @Getter
    private final String correspondingTemplateName;

    MailNotificationType(String displayName, String correspondingTemplateName, String correspondingJobStates) {
        this.displayName = displayName;
        this.correspondingTemplateName = correspondingTemplateName;
        this.correspondingJobStates = correspondingJobStates;
    }

    public abstract boolean needNotify(KylinConfig kylinconfig);

    public List<String> needNotifyStates(KylinConfig kylinConfig) {
        List<String> needNotifyStates = new ArrayList<>();

        // forward compatible
        if (kylinConfig.getJobErrorNotificationEnabled()) {
            needNotifyStates.add("error");
        }

        String[] specifiedStates = kylinConfig.getJobNotificationStates();
        if (specifiedStates != null) {
            needNotifyStates.addAll(Arrays.asList(specifiedStates));
        }

        return needNotifyStates;
    }
}
