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

public enum QuerySparkExecutionEnum {
    ID_CODE("QUERY_HASH_CODE"),
    SQL("QUERY_SQL"),
    PROJECT("PROJECT"),
    TYPE("QUERY_TYPE"),
    REALIZATION("REALIZATION"),
    REALIZATION_TYPE("REALIZATION_TYPE"),
    CUBOID_IDS("CUBOID_IDS"),
    QUERY_ID("QUERY_ID"),
    EXECUTION_ID("EXECUTION_ID"),
    USER("KUSER"),
    SPARDER_NAME("SPARDER_NAME"),
    EXCEPTION("EXCEPTION"),
    START_TIME("START_TIME"),
    END_TIME("END_TIME"),


    TIME_COST("QUERY_TIME_COST"),
    TOTAL_SCAN_COUNT("TOTAL_SCAN_COUNT"),
    TOTAL_SCAN_BYTES("TOTAL_SCAN_BYTES"),
    RESULT_COUNT("RESULT_COUNT"),

    EXECUTION_DURATION("EXECUTION_DURATION"),
    RESULT_SIZE("RESULT_SIZE"),
    EXECUTOR_DESERIALIZE_TIME("EXECUTOR_DESERIALIZE_TIME"),
    EXECUTOR_DESERIALIZE_CPU_TIME("EXECUTOR_DESERIALIZE_CPU_TIME"),
    EXECUTOR_RUN_TIME("EXECUTOR_RUN_TIME"),
    EXECUTOR_CPU_TIME("EXECUTOR_CPU_TIME"),
    JVM_GC_TIME("JVM_GC_TIME"),
    RESULT_SERIALIZATION_TIME("RESULT_SERIALIZATION_TIME"),
    MEMORY_BYTE_SPILLED("MEMORY_BYTE_SPILLED"),
    DISK_BYTES_SPILLED("DISK_BYTES_SPILLED"),
    PEAK_EXECUTION_MEMORY("PEAK_EXECUTION_MEMORY");

    private final String propertyName;

    QuerySparkExecutionEnum(String name) {
        this.propertyName = name;
    }

    public static QuerySparkExecutionEnum getByName(String name) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("Name should not be empty");
        }
        for (QuerySparkExecutionEnum property : QuerySparkExecutionEnum.values()) {
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
