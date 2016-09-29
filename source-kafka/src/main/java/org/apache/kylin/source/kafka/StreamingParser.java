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
import java.util.Set;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import java.nio.ByteBuffer;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.StreamingMessage;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Sets;
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
    public static final String EMBEDDED_PROPERTY_SEPARATOR = "separator";

    public static final Map<String, String> defaultProperties = Maps.newHashMap();
    public static final Set derivedTimeColumns = Sets.newHashSet();
    static {
        derivedTimeColumns.add("minute_start");
        derivedTimeColumns.add("hour_start");
        derivedTimeColumns.add("day_start");
        derivedTimeColumns.add("week_start");
        derivedTimeColumns.add("month_start");
        derivedTimeColumns.add("quarter_start");
        derivedTimeColumns.add("year_start");
        defaultProperties.put(PROPERTY_TS_COLUMN_NAME, "timestamp");
        defaultProperties.put(PROPERTY_TS_PARSER, "org.apache.kylin.source.kafka.DefaultTimeParser");
        defaultProperties.put(PROPERTY_TS_PATTERN, DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
        defaultProperties.put(EMBEDDED_PROPERTY_SEPARATOR, "_");
    }

    /**
     * @param message
     * @return StreamingMessage must not be NULL
     */
    abstract public StreamingMessage parse(ByteBuffer message);

    abstract public boolean filter(StreamingMessage streamingMessage);

    public static StreamingParser getStreamingParser(String parserName, String parserProperties, List<TblColRef> columns) throws ReflectiveOperationException {
        if (!StringUtils.isEmpty(parserName)) {
            logger.info("Construct StreamingParse {} with properties {}", parserName, parserProperties);
            Class clazz = Class.forName(parserName);
            Map<String, String> properties = parseProperties(parserProperties);
            Constructor constructor = clazz.getConstructor(List.class, Map.class);
            return (StreamingParser) constructor.newInstance(columns, properties);
        } else {
            throw new IllegalStateException("invalid StreamingConfig, parserName " + parserName + ", parserProperties " + parserProperties + ".");
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
        if (derivedTimeColumns.contains(columnName) == false)
            return false;

        long normalized = 0;
        if (columnName.equals("minute_start")) {
            normalized = TimeUtil.getMinuteStart(t);
            result.add(DateFormat.formatToTimeWithoutMilliStr(normalized));
        } else if (columnName.equals("hour_start")) {
            normalized = TimeUtil.getHourStart(t);
            result.add(DateFormat.formatToTimeWithoutMilliStr(normalized));
        } else if (columnName.equals("day_start")) {
            //from day_start on, formatTs will output date format
            normalized = TimeUtil.getDayStart(t);
            result.add(DateFormat.formatToDateStr(normalized));
        } else if (columnName.equals("week_start")) {
            normalized = TimeUtil.getWeekStart(t);
            result.add(DateFormat.formatToDateStr(normalized));
        } else if (columnName.equals("month_start")) {
            normalized = TimeUtil.getMonthStart(t);
            result.add(DateFormat.formatToDateStr(normalized));
        } else if (columnName.equals("quarter_start")) {
            normalized = TimeUtil.getQuarterStart(t);
            result.add(DateFormat.formatToDateStr(normalized));
        } else if (columnName.equals("year_start")) {
            normalized = TimeUtil.getYearStart(t);
            result.add(DateFormat.formatToDateStr(normalized));
        }

        return true;
    }

}
