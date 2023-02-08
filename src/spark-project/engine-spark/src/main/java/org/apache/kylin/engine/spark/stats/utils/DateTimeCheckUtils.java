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

package org.apache.kylin.engine.spark.stats.utils;

import org.apache.commons.lang3.StringUtils;

public class DateTimeCheckUtils {

    /**
     * Detect if the given value is a date type. <br>
     *
     * @param value the value to be detected.
     * @return true if the value is a date type, false otherwise.
     */
    public static boolean isDate(String value) {
        if (checkDatesPreconditions(value)) {
            return checkDatesPattern(value);
        }
        return false;
    }

    /**
     * Detect if the given value is a time type.
     *
     * @param value
     * @return
     */
    public static boolean isTime(String value) {
        // The length of date strings must not be less than 4, and must not exceed 24.
        return StringUtils.isNotEmpty(value) && value.length() >= 4 && value.length() <= 24 && checkEnoughDigits(value)
                && checkDatesPattern(value);
    }

    private static boolean checkDatesPreconditions(String value) {
        return (StringUtils.isNotEmpty(value) && value.length() >= 6 && value.length() <= 64
                && checkEnoughDigits(value));
    }

    private static boolean checkDatesPattern(String value) {
        // TODO pattern match
        return true;
    }

    /**
     * The value must have at least 3 digits
     *
     * @param value
     * @return true is the value contains at least 3 digits
     */
    private static boolean checkEnoughDigits(String value) {
        int digitCount = 0;
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch >= '0' && ch <= '9') {
                digitCount++;
                if (digitCount > 2) {
                    return true;
                }
            }
        }
        return false;
    }
}
