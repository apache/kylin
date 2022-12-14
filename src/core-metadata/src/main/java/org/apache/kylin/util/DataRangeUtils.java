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
package org.apache.kylin.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.model.PartitionDesc;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_END_LESS_THAN_EQUALS_START;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_LESS_THAN_ZERO;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_NOT_CONSISTENT;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_NOT_FORMAT_MS;

public final class DataRangeUtils {
    private DataRangeUtils() {
    }

    public static void validateRange(String start, String end) {
        validateRange(Long.parseLong(start), Long.parseLong(end));
    }

    private static void validateRange(long start, long end) {
        if (start < 0 || end < 0) {
            throw new KylinException(TIME_INVALID_RANGE_LESS_THAN_ZERO);
        }
        if (start >= end) {
            throw new KylinException(TIME_INVALID_RANGE_END_LESS_THAN_EQUALS_START);
        }
    }

    public static void validateDataRange(String start, String end) {
        validateDataRange(start, end, null);
    }

    public static void validateDataRange(String start, String end, String partitionColumnFormat) {
        if (StringUtils.isEmpty(start) && StringUtils.isEmpty(end)) {
            return;
        }
        if (StringUtils.isNotEmpty(start) && StringUtils.isNotEmpty(end)) {
            long startLong = 0;
            long endLong = 0;

            try {
                startLong = Long.parseLong(start);
                endLong = Long.parseLong(end);
            } catch (Exception e) {
                throw new KylinException(TIME_INVALID_RANGE_NOT_FORMAT_MS, e);
            }

            if (startLong < 0 || endLong < 0) {
                throw new KylinException(TIME_INVALID_RANGE_LESS_THAN_ZERO);
            }

            try {
                startLong = DateFormat.getFormatTimeStamp(start, transformTimestamp2Format(partitionColumnFormat));
                endLong = DateFormat.getFormatTimeStamp(end, transformTimestamp2Format(partitionColumnFormat));
            } catch (Exception e) {
                throw new KylinException(TIME_INVALID_RANGE_NOT_FORMAT_MS);
            }

            if (startLong >= endLong) {
                throw new KylinException(TIME_INVALID_RANGE_END_LESS_THAN_EQUALS_START);
            }

        } else {
            throw new KylinException(TIME_INVALID_RANGE_NOT_CONSISTENT);
        }

    }

    private static String transformTimestamp2Format(String columnFormat) {
        for (PartitionDesc.TimestampType timestampType : PartitionDesc.TimestampType.values()) {
            if (timestampType.name.equals(columnFormat)) {
                return timestampType.format;
            }
        }
        return columnFormat;
    }

}
