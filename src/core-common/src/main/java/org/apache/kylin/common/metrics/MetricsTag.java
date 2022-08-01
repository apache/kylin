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

package org.apache.kylin.common.metrics;

public enum MetricsTag {

    PROJECT("project"), //
    MODEL("model"), //
    JOB_TYPE("job_type"), //
    STATE("state"), //
    PUSH_DOWN("push_down"), //
    CACHE("cache"), //
    HIT_INDEX("hit_index"), //
    HIT_EXACTLY_INDEX("hit_exactly_index"), //
    SUCCEED("succeed"), //
    HIT_SNAPSHOT("hit_snapshot"), //
    TYPE("type"), //
    POOL("pool"), //
    PENDING("pending"), //
    RUNNING("running"), //
    JOB_CATEGORY("category"), //
    HOST("host"), //
    HIT_SECOND_STORAGE("hit_second_storage");

    private final String value;

    MetricsTag(String value) {
        this.value = value;
    }

    public String getVal() {
        return this.value;
    }
}
