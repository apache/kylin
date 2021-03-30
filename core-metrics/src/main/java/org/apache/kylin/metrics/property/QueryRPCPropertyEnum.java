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
 * Definition of Metrics dimension and measure for HBase RPC
 */
public enum QueryRPCPropertyEnum {

    PROJECT("PROJECT"),
    REALIZATION("REALIZATION"),
    RPC_SERVER("RPC_SERVER"),
    EXCEPTION("EXCEPTION"),

    CALL_TIME("CALL_TIME"),
    SKIP_COUNT("COUNT_SKIP"),
    SCAN_COUNT("COUNT_SCAN"),
    RETURN_COUNT("COUNT_RETURN"),
    AGGR_FILTER_COUNT("COUNT_AGGREGATE_FILTER"),
    AGGR_COUNT("COUNT_AGGREGATE");

    private final String propertyName;

    QueryRPCPropertyEnum(String name) {
        this.propertyName = name;
    }

    public static QueryRPCPropertyEnum getByName(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return null;
        }
        for (QueryRPCPropertyEnum property : QueryRPCPropertyEnum.values()) {
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
