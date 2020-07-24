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
 * Definition of Metrics dimension and measure for Query Basic
 */
public enum QueryPropertyEnum {

    ID_CODE("QUERY_HASH_CODE"),
    SQL("QUERY_SQL"),
    TYPE("QUERY_TYPE"),
    USER("KUSER"),
    PROJECT("PROJECT"),
    REALIZATION("REALIZATION"),
    REALIZATION_TYPE("REALIZATION_TYPE"),
    EXCEPTION("EXCEPTION"),

    TIME_COST("QUERY_TIME_COST"),
    CALCITE_RETURN_COUNT("CALCITE_COUNT_RETURN"),
    STORAGE_RETURN_COUNT("STORAGE_COUNT_RETURN"),
    AGGR_FILTER_COUNT("CALCITE_COUNT_AGGREGATE_FILTER");

    private final String propertyName;

    QueryPropertyEnum(String name) {
        this.propertyName = name;
    }

    public static QueryPropertyEnum getByName(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return null;
        }
        for (QueryPropertyEnum property : QueryPropertyEnum.values()) {
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
