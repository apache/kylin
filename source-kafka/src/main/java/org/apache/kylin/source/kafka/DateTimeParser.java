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

package org.apache.kylin.source.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kylin.common.util.DateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

/**
 */
public class DateTimeParser extends AbstractTimeParser {

    private static final Logger logger = LoggerFactory.getLogger(DateTimeParser.class);
    private String tsPattern = DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS;

    private FastDateFormat formatter = null;

    //call by reflection
    public DateTimeParser(String[] properties) {
        super(properties);
        for (String prop : properties) {
            try {
                String[] parts = prop.split("=");
                if (parts.length == 2) {
                    switch (parts[0]) {
                    case "tsPattern":
                        this.tsPattern = parts[1];
                        break;
                    default:
                        break;
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to parse property " + prop);
                //ignore
            }
        }

        if (!StringUtils.isEmpty(tsPattern)) {
            try {
                formatter = org.apache.kylin.common.util.DateFormat.getDateFormat(tsPattern);
            } catch (Throwable e) {
                throw new IllegalStateException("Invalid tsPattern: '" + tsPattern + "'.");
            }
        } else {
            throw new IllegalStateException("Invalid tsPattern: '" + tsPattern + "'.");
        }
    }

    /**
     * Parse a string time to a long value (epoch time)
     *
     * @param timeStr
     * @return
     */
    public long parseTime(String timeStr) throws IllegalArgumentException {

        try {
            return formatter.parse(timeStr).getTime();
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid value : pattern: '" + tsPattern + "', value: '" + timeStr + "'" , e);
        }
    }
}
