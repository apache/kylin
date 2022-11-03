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

import java.sql.Date;

import org.apache.calcite.linq4j.function.Parameter;

public class ToDateUDF {

    public Date TO_DATE(@Parameter(name = "str1") String dateStr, @Parameter(name = "str2") String fmt) {
        if (dateStr == null) {
            return null;
        }
        Date date = null;
        switch (fmt) {
        case "yyyy-MM-dd":
            dateStr = dateStr.substring(0, 10);
            date = Date.valueOf(dateStr);
            break;
        case "yyyy-MM":
            dateStr = dateStr.substring(0, 7) + "-01";
            date = Date.valueOf(dateStr);
            break;
        case "yyyy":
        case "y":
            dateStr = dateStr.substring(0, 4) + "-01-01";
            date = Date.valueOf(dateStr);
            break;
        default:
            //date = null
        }
        return date;
    }

    public Date TO_DATE(@Parameter(name = "str1") String date) {
        return Date.valueOf(date);
    }
}
