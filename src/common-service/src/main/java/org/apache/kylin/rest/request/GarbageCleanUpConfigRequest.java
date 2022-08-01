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

package org.apache.kylin.rest.request;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

import org.apache.kylin.common.exception.KylinException;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class GarbageCleanUpConfigRequest {
    @JsonProperty("frequency_time_window")
    private FrequencyTimeWindowEnum frequencyTimeWindow;
    @JsonProperty("low_frequency_threshold")
    private Long lowFrequencyThreshold;

    public enum FrequencyTimeWindowEnum {
        DAY, WEEK, MONTH
    }

    public int getFrequencyTimeWindow() {
        if (frequencyTimeWindow == null) {
            throw new KylinException(INVALID_PARAMETER, "parameter 'frequency_time_window' is not set");
        }
        switch (frequencyTimeWindow) {
        case DAY:
            return 1;
        case WEEK:
            return 7;
        case MONTH:
            return 30;
        default:
            throw new KylinException(INVALID_PARAMETER, "Illegal parameter 'frequency_time_window'!");
        }
    }

}
