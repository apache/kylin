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

import java.util.Locale;

import org.apache.kylin.shaded.com.google.common.base.Strings;

/**
 * Definition of Metrics dimension and measure for Query Advanced
 */
public enum QueryCubePropertyEnum {

    PROJECT("PROJECT"),
    CUBE("CUBE_NAME"),
    SEGMENT("SEGMENT_NAME"),
    CUBOID_SOURCE("CUBOID_SOURCE"),
    CUBOID_TARGET("CUBOID_TARGET"),
    IF_MATCH("IF_MATCH"),
    FILTER_MASK("FILTER_MASK"),
    IF_SUCCESS("IF_SUCCESS"),

    TIME_SUM("STORAGE_CALL_TIME_SUM"),
    TIME_MAX("STORAGE_CALL_TIME_MAX"),
    WEIGHT_PER_HIT("WEIGHT_PER_HIT"),
    CALL_COUNT("STORAGE_CALL_COUNT"),
    SKIP_COUNT("STORAGE_COUNT_SKIP"),
    SCAN_COUNT("STORAGE_COUNT_SCAN"),
    RETURN_COUNT("STORAGE_COUNT_RETURN"),
    AGGR_FILTER_COUNT("STORAGE_COUNT_AGGREGATE_FILTER"),
    AGGR_COUNT("STORAGE_COUNT_AGGREGATE");

    private final String propertyName;

    QueryCubePropertyEnum(String name) {
        this.propertyName = name;
    }

    public static QueryCubePropertyEnum getByName(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return null;
        }
        for (QueryCubePropertyEnum property : QueryCubePropertyEnum.values()) {
            if (property.propertyName.equals(name.toUpperCase(Locale.ROOT))) {
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
