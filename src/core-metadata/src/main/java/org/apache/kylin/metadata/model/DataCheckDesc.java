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

package org.apache.kylin.metadata.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;

@Getter
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataCheckDesc implements Serializable {

    @JsonProperty("check_options")
    private long checkOptions;

    @JsonProperty("fault_threshold")
    private long faultThreshold;

    @JsonProperty("fault_actions")
    private long faultActions;

    public static DataCheckDesc valueOf(long checkOptions, long faultThreshold, long faultActions) {
        DataCheckDesc instance = new DataCheckDesc();
        instance.checkOptions = checkOptions;
        instance.faultThreshold = faultThreshold;
        instance.faultActions = faultActions;
        return instance;
    }

    public boolean checkDuplicatePK() {
        return CheckOptions.PK_DUPLICATE.match(checkOptions);
    }

    public boolean checkDataSkew() {
        return CheckOptions.DATA_SKEW.match(checkOptions);
    }

    public boolean checkNullOrBlank() {
        return CheckOptions.NULL_OR_BLANK_VALUE.match(checkOptions);
    }

    public boolean checkForceAnalysisLookup() {
        return CheckOptions.FORCE_ANALYSIS_LOOKUP.match(checkOptions);
    }

    public boolean isContinue(int count) {
        return count > faultThreshold && ActionOptions.CONTINUE.match(faultActions);
    }

    public boolean isFailed(int count) {
        return count > faultThreshold && ActionOptions.FAILED.match(faultActions);
    }

    enum CheckOptions {

        PK_DUPLICATE(1), DATA_SKEW(1 << 1), NULL_OR_BLANK_VALUE(1 << 2), FORCE_ANALYSIS_LOOKUP(1 << 3);

        private int value;

        CheckOptions(int value) {
            this.value = value;
        }

        boolean match(long checkOptions) {
            return (checkOptions & value) != 0;
        }
    }

    enum ActionOptions {

        FAILED(1), CONTINUE(1 << 1);

        private int value;

        ActionOptions(int value) {
            this.value = value;
        }

        boolean match(long actionOptions) {
            return (actionOptions & value) != 0;
        }
    }
}
