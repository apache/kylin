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

package org.apache.kylin.common.persistence.transaction;

import org.apache.kylin.common.scheduler.SchedulerEventNotifier;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import lombok.Getter;
import lombok.Setter;

/**
 *
 * if you extend a new broadcast event, please clear about:
 * 1. aboutBroadcast Scope
 * 2. is need to be handled by itself?
 * 3. For all current broadcast event, when last same broadcast haven't been sent to other nodes,
 * current broadcast event will be ignored. therefore, you have to override equals and hashcode methods.
 *
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
@Getter
@Setter
public class BroadcastEventReadyNotifier extends SchedulerEventNotifier {

    public BroadcastScopeEnum getBroadcastScope() {
        return BroadcastScopeEnum.WHOLE_NODES;
    }

    public boolean needBroadcastSelf() {
        return true;
    }

    public enum BroadcastScopeEnum {
        /**
         * All、Job、Query
         */
        WHOLE_NODES,

        /**
         * All、Job
         */
        LEADER_NODES,

        /**
         * All、Query
         */
        QUERY_AND_ALL,

        ALL_NODES, JOB_NODES, QUERY_NODES
    }
}
