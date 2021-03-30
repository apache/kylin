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

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import java.nio.ByteBuffer;
import java.util.TimeZone;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.StreamingMessageRow;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.metadata.model.TblColRef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * By convention stream parsers should have a constructor with (List<TblColRef> allColumns, Map properties) as params
 */
public abstract class StreamingParser {

    private static final Logger logger = LoggerFactory.getLogger(StreamingParser.class);
    public static final String PROPERTY_TS_COLUMN_NAME = "tsColName";
    public static final String PROPERTY_TS_PARSER = "tsParser";
    public static final String PROPERTY_TS_PATTERN = "tsPattern";
    public static final String PROPERTY_EMBEDDED_SEPARATOR = "separator";
    public static final String PROPERTY_STRICT_CHECK = "strictCheck"; // whether need check each column strictly, default be false (fault tolerant).

    public static final Map<String, String> defaultProperties = Maps.newHashMap();
    public static final Map<String, Integer> derivedTimeColumns = Maps.newHashMap();
    static {
        derivedTimeColumns.put("minute_start", 1);
        derivedTimeColumns.put("hour_start", 2);
        derivedTimeColumns.put("day_start", 3);
        derivedTimeColumns.put("week_start", 4);
        derivedTimeColumns.put("month_start", 5);
        derivedTimeColumns.put("quarter_start", 6);
        derivedTimeColumns.put("year_start", 7);
        defaultProperties.put(PROPERTY_TS_COLUMN_NAME, "timestamp");
        defaultProperties.put(PROPERTY_TS_PARSER, "org.apache.kylin.source.kafka.DefaultTimeParser");
        defaultProperties.put(PROPERTY_TS_PATTERN, DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
        defaultProperties.put(PROPERTY_EMBEDDED_SEPARATOR, "_");
        defaultProperties.put(PROPERTY_STRICT_CHECK, "false");
    }

    /**
     * @param message
     * @return List<StreamingMessageRow> must not be NULL
     */
    abstract public List<StreamingMessageRow> parse(ByteBuffer message);

    abstract public boolean filter(StreamingMessageRow streamingMessageRow);

    public static StreamingParser getStreamingParser(String parserName, String parserProperties, List<TblColRef> columns) throws ReflectiveOperationException {
        if (!StringUtils.isEmpty(parserName)) {
            logger.info("Construct StreamingParse {} with properties {}", parserName, parserProperties);
            Class clazz = Class.forName(parserName);
            Map<String, String> properties = parseProperties(parserProperties);
            Constructor constructor = clazz.getConstructor(List.class, Map.class);
            return (StreamingParser) constructor.newInstance(columns, properties);
        } else {
            throw new IllegalStateException("Invalid StreamingConfig, parserName " + parserName + ", parserProperties " + parserProperties + ".");
        }
    }

    public static Map<String, String> parseProperties(String propertiesStr) {

        Map<String, String> result = Maps.newHashMap(defaultProperties);
        if (!StringUtils.isEmpty(propertiesStr)) {
            String[] properties = propertiesStr.split(";");
            for (String prop : properties) {
                String[] parts = prop.split("=");
                if (parts.length == 2) {
                    result.put(parts[0], parts[1]);
                } else {
                    logger.warn("Ignored invalid property expression '" + prop + "'");
                }
            }
        }

        return result;
    }

    /**
     * Calculate the derived time column value and put to the result list.
     * @param columnName the column name, should be in lower case
     * @param result the string list which representing a row
     * @param t the timestamp that to calculate the derived time
     * @return true if the columnName is a derived time column; otherwise false;
     */
    public static final boolean populateDerivedTimeColumns(String columnName, List<String> result, long t) {
        String timeZoneStr = KylinConfig.getInstanceFromEnv().getTimeZone();
        TimeZone timeZone = TimeZone.getTimeZone(timeZoneStr);

        Integer derivedTimeColumn = derivedTimeColumns.get(columnName);
        if (derivedTimeColumn == null) {
            return false;
        }

        long normalized = 0;
        switch (derivedTimeColumn) {
        case 1:
            normalized = TimeUtil.getMinuteStart(t);
            result.add(DateFormat.formatToTimeStrWithTimeZone(timeZone, normalized));
            break;
        case 2:
            normalized = TimeUtil.getHourStart(t);
            result.add(DateFormat.formatToTimeStrWithTimeZone(timeZone, normalized));
            break;
        case 3:
            normalized = TimeUtil.getDayStartWithTimeZone(timeZone, t);
            result.add(DateFormat.formatToDateStrWithTimeZone(timeZone, normalized));
            break;
        case 4:
            normalized = TimeUtil.getWeekStartWithTimeZone(timeZone, t);
            result.add(DateFormat.formatToDateStrWithTimeZone(timeZone, normalized));
            break;
        case 5:
            normalized = TimeUtil.getMonthStartWithTimeZone(timeZone, t);
            result.add(DateFormat.formatToDateStrWithTimeZone(timeZone, normalized));
            break;
        case 6:
            normalized = TimeUtil.getQuarterStartWithTimeZone(timeZone, t);
            result.add(DateFormat.formatToDateStrWithTimeZone(timeZone, normalized));
            break;
        case 7:
            normalized = TimeUtil.getYearStartWithTimeZone(timeZone, t);
            result.add(DateFormat.formatToDateStrWithTimeZone(timeZone, normalized));
            break;
        default:
            throw new IllegalStateException();

        }

        return true;
    }

}
