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

package org.apache.kylin.query.udf.formatUdf;

import java.sql.Timestamp;

import org.apache.calcite.linq4j.function.Parameter;

/**
 *  Refer to to_timestamp() on spark SQL.
 */
public class ToTimestampUDF {

    public Timestamp TO_TIMESTAMP(@Parameter(name = "str1") String timestampStr, @Parameter(name = "str2") String fmt) {
        if (timestampStr == null) {
            return null;
        }
        Timestamp timestamp = null;
        switch (fmt) {
        case "yyyy-MM-dd hh:mm:ss":
            timestamp = Timestamp.valueOf(timestampStr);
            break;
        case "yyyy-MM-dd hh:mm":
            timestampStr = timestampStr.substring(0, 16) + ":00";
            timestamp = Timestamp.valueOf(timestampStr);
            break;
        case "yyyy-MM-dd hh":
            timestampStr = timestampStr.substring(0, 13) + ":00:00";
            timestamp = Timestamp.valueOf(timestampStr);
            break;
        case "yyyy-MM-dd":
            timestampStr = timestampStr.substring(0, 10) + " 00:00:00";
            timestamp = Timestamp.valueOf(timestampStr);
            break;
        case "yyyy-MM":
            timestampStr = timestampStr.substring(0, 7) + "-01 00:00:00";
            timestamp = Timestamp.valueOf(timestampStr);
            break;
        case "yyyy":
        case "y":
            timestampStr = timestampStr.substring(0, 4) + "-01-01 00:00:00";
            timestamp = Timestamp.valueOf(timestampStr);
            break;
        default:
            //timestamp = null;
        }
        return timestamp;
    }

    public Timestamp TO_TIMESTAMP(@Parameter(name = "str1") String timestamp) {
        return Timestamp.valueOf(timestamp);
    }
}
