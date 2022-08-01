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

package org.apache.kylin.common.persistence.metadata;

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@SuppressWarnings("serial")
@AllArgsConstructor
@NoArgsConstructor
public class Epoch extends RootPersistentEntity {

    @JsonProperty("epoch_id")
    @Getter
    @Setter
    private long epochId;

    @JsonProperty("epoch_target")
    @Getter
    @Setter
    private String epochTarget;

    @JsonProperty("current_epoch_owner")
    @Getter
    @Setter
    private String currentEpochOwner;

    @JsonProperty("last_epoch_renew_time")
    @Getter
    @Setter
    private long lastEpochRenewTime;

    @JsonProperty("server_mode")
    @Getter
    @Setter
    private String serverMode;

    @JsonProperty("maintenance_mode_reason")
    @Getter
    @Setter
    private String maintenanceModeReason;

    @JsonProperty("mvcc")
    @Getter
    @Setter
    private long mvcc;

    @Override
    public String toString() {
        return "Epoch{" + "epochId=" + epochId + ", epochTarget='" + epochTarget + '\'' + ", currentEpochOwner='"
                + currentEpochOwner + '\'' + ", lastEpochRenewTime=" + lastEpochRenewTime + ", serverMode='"
                + serverMode + '\'' + ", maintenanceModeReason='" + maintenanceModeReason + '\'' + ", mvcc=" + mvcc
                + '}';
    }
}
