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

package org.apache.kylin.parser;

import java.time.ZoneId;
import java.util.Calendar;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;

/**
 */
public class DefaultTimeParser extends AbstractTimeParser {
    private String tsTimezone = null;

    public DefaultTimeParser(Map<String, String> properties) {
        super(properties);
        tsTimezone = properties.get(StreamingParser.PROPERTY_TS_TIMEZONE);
    }

    /**
     * Parse a string time to a long value (epoch time)
     * @param time
     * @return
     */
    public long parseTime(String time) throws IllegalArgumentException {
        long t;
        if (StringUtils.isEmpty(time)) {
            t = 0;
        } else {
            try {
                ZoneId zoneId = ZoneId.of(tsTimezone);
                TimeZone timeZone = TimeZone.getTimeZone(zoneId);
                Calendar calendar = Calendar.getInstance(timeZone, Locale.ROOT);
                int offsetMilli = calendar.get(Calendar.ZONE_OFFSET);
                t = Long.parseLong(time) + offsetMilli;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(e);
            }
        }
        return t;
    }
}
