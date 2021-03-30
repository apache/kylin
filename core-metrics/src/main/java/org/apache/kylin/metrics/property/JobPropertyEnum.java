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

package org.apache.kylin.metrics.property;

import org.apache.kylin.shaded.com.google.common.base.Strings;

/**
 * Definition of Metrics dimension and measure for Cube building job
 */
public enum JobPropertyEnum {

    ID_CODE("JOB_ID"),
    USER("KUSER"),
    PROJECT("PROJECT"),
    CUBE("CUBE_NAME"),
    TYPE("JOB_TYPE"),
    ALGORITHM("CUBING_TYPE"),
    STATUS("JOB_STATUS"),
    EXCEPTION("EXCEPTION"),
    EXCEPTION_MSG("EXCEPTION_MSG"),

    SOURCE_SIZE("TABLE_SIZE"),
    CUBE_SIZE("CUBE_SIZE"),
    BUILD_DURATION("DURATION"),
    WAIT_RESOURCE_TIME("WAIT_RESOURCE_TIME"),
    PER_BYTES_TIME_COST("PER_BYTES_TIME_COST"),
    STEP_DURATION_DISTINCT_COLUMNS("STEP_DURATION_DISTINCT_COLUMNS"),
    STEP_DURATION_DICTIONARY("STEP_DURATION_DICTIONARY"),
    STEP_DURATION_INMEM_CUBING("STEP_DURATION_INMEM_CUBING"),
    STEP_DURATION_HFILE_CONVERT("STEP_DURATION_HFILE_CONVERT");

    private final String propertyName;

    JobPropertyEnum(String name) {
        this.propertyName = name;
    }

    public static JobPropertyEnum getByName(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return null;
        }
        for (JobPropertyEnum property : JobPropertyEnum.values()) {
            if (property.propertyName.equalsIgnoreCase(name)) {
                return property;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return propertyName;
    }
}