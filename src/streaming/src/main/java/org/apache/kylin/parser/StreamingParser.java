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

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.streaming.metadata.StreamingMessageRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * By convention:
 * 1. stream parsers should have constructor
 *     - with (List<TblColRef> allColumns, Map properties) as params
 *
 */
public abstract class StreamingParser {

    public static final String PROPERTY_TS_COLUMN_NAME = "tsColName";
    public static final String PROPERTY_TS_PARSER = "tsParser";
    public static final String PROPERTY_TS_PATTERN = "tsPattern";
    public static final String PROPERTY_EMBEDDED_SEPARATOR = "separator";
    public static final String PROPERTY_STRICT_CHECK = "strictCheck"; // whether need check each column strictly, default be false (fault tolerant).
    public static final String PROPERTY_TS_TIMEZONE = "tsTimezone";
    protected static final Map<String, String> defaultProperties = Maps.newHashMap();
    private static final Logger logger = LoggerFactory.getLogger(StreamingParser.class);

    static {
        defaultProperties.put(PROPERTY_TS_COLUMN_NAME, "timestamp");
        defaultProperties.put(PROPERTY_TS_PARSER, "org.apache.kylin.parser.DefaultTimeParser");
        defaultProperties.put(PROPERTY_TS_PATTERN, DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
        defaultProperties.put(PROPERTY_EMBEDDED_SEPARATOR, "_");
        defaultProperties.put(PROPERTY_STRICT_CHECK, "false");
        defaultProperties.put(PROPERTY_TS_TIMEZONE, "GMT+0");
    }

    protected List<TblColRef> allColumns = Lists.newArrayList();
    protected Map<String, String> columnMapping = Maps.newHashMap();

    /**
     * Constructor should be override
     */
    public StreamingParser(List<TblColRef> allColumns, Map<String, String> properties) {
        if (allColumns != null) {
            initColumns(allColumns);
        }
    }

    public static StreamingParser getStreamingParser(String parserName, String parserProperties,
            List<TblColRef> columns) throws ReflectiveOperationException {
        if (!StringUtils.isEmpty(parserName)) {
            logger.info("Construct StreamingParse {} with properties {}", parserName, parserProperties);
            Class clazz = Class.forName(parserName);
            Map<String, String> properties = parseProperties(parserProperties);
            Constructor constructor = clazz.getConstructor(List.class, Map.class);
            return (StreamingParser) constructor.newInstance(columns, properties);
        } else {
            throw new IllegalStateException("Invalid StreamingConfig, parserName " + parserName + ", parserProperties "
                    + parserProperties + ".");
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
                    logger.warn("Ignored invalid property expression '{}'.", prop);
                }
            }
        }

        return result;
    }

    private void initColumns(List<TblColRef> allColumns) {
        for (TblColRef col : allColumns) {
            if (col.getColumnDesc().isComputedColumn()) {
                continue;
            }
            this.allColumns.add(col);
        }
        logger.info("Streaming Parser columns: {}", this.allColumns);
    }

    /**
     * Flatten stream message: Extract all key value pairs to a map
     *    The key is source_attribute
     *    The value is the value string
     * @param message
     * @return a flat map must not be NULL
     */
    public abstract Map<String, Object> flattenMessage(ByteBuffer message);

    /**
     * @param message
     * @return List<StreamingMessageRow> must not be NULL
     */
    public abstract List<StreamingMessageRow> parse(ByteBuffer message);

    /**
     * Set column mapping
     * The column mapping: column_name --> source_attribute
     * @param columnMapping
     */
    public void setColumnMapping(Map<String, String> columnMapping) {
        this.columnMapping = columnMapping;
    }

}
