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
package io.kyligence.kap.clickhouse;

import java.util.Locale;

import io.kyligence.kap.secondstorage.NameUtil;

public class ClickHouseNameUtil {
    private static final int REAL_JOB_ID_LENGTH = 36;
    private static final String TEMP_TABLE_FORMAT = "%s_%s_%s";
    private static final String FILE_SOURCE_TABLE_FORMAT = "%s_src_%05d";

    private ClickHouseNameUtil() {
        // not need new
    }

    public static String getInsertTempTableName(String jobId, String segmentId, long layoutId) {
        if (jobId.length() > REAL_JOB_ID_LENGTH) {
            jobId = jobId.substring(0, REAL_JOB_ID_LENGTH);
        }
        return String.format(Locale.ROOT, TEMP_TABLE_FORMAT, jobId, segmentId, layoutId).replace("-", "_") + "_"
                + NameUtil.TEMP_TABLE_FLAG;
    }

    public static String getDestTempTableName(String jobId, String segmentId, long layoutId) {
        if (jobId.length() > REAL_JOB_ID_LENGTH) {
            jobId = jobId.substring(0, REAL_JOB_ID_LENGTH);
        }
        return String.format(Locale.ROOT, TEMP_TABLE_FORMAT, jobId, segmentId, layoutId).replace("-", "_") + "_"
                + NameUtil.TEMP_TABLE_FLAG + "_tmp";
    }

    public static String getLikeTempTableName(String jobId, String segmentId, long layoutId) {
        if (jobId.length() > REAL_JOB_ID_LENGTH) {
            jobId = jobId.substring(0, REAL_JOB_ID_LENGTH);
        }
        return String.format(Locale.ROOT, TEMP_TABLE_FORMAT, jobId, segmentId, layoutId).replace("-", "_") + "_"
                + NameUtil.TEMP_TABLE_FLAG + "_ke_like";
    }

    public static String getFileSourceTableName(String insertTempTableName, int index) {
        return String.format(Locale.ROOT, FILE_SOURCE_TABLE_FORMAT, insertTempTableName, index);
    }

    public static String getSkippingIndexName(String tableName, String columnName) {
        return tableName + "_" + columnName;
    }
}
