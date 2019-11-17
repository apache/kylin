/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.stats.utils;


import org.apache.commons.lang.StringUtils;

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
