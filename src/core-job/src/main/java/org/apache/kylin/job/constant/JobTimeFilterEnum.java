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

package org.apache.kylin.job.constant;

public enum JobTimeFilterEnum {
    LAST_ONE_DAY(0), LAST_ONE_WEEK(1), LAST_ONE_MONTH(2), LAST_ONE_YEAR(3), ALL(4);

    private final int code;

    private JobTimeFilterEnum(int code) {
        this.code = code;
    }

    public static JobTimeFilterEnum getByCode(int code) {
        for (JobTimeFilterEnum timeFilter : values()) {
            if (timeFilter.getCode() == code) {
                return timeFilter;
            }
        }

        return null;
    }

    public int getCode() {
        return code;
    }
}
