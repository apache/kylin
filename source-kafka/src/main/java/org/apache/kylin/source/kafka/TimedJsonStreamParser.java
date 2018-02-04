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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Collections;
import java.util.Arrays;

import com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.ByteBufferBackedInputStream;
import org.apache.kylin.common.util.StreamingMessageRow;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.google.common.collect.Lists;

/**
 * An utility class which parses a JSON streaming message to a list of strings (represent a row in table).
 * <p>
 * Each message should have a property whose value represents the message's timestamp, default the column name is "timestamp"
 * but can be customized by StreamingParser#PROPERTY_TS_PARSER.
 * <p>
 * By default it will parse the timestamp col value as Unix time. If the format isn't Unix time, need specify the time parser
 * with property StreamingParser#PROPERTY_TS_PARSER.
 * <p>
 * It also support embedded JSON format; Use TimedJsonStreamParser#EMBEDDED_PROPERTY_SEPARATOR) to separate them and save into
 * the column's "comment" filed. For example: "{ 'user' : { 'first_name': 'Tom'}}"; The 'first_name' field is expressed as
 * 'user_first_name' field, and its comment value is 'user|first_name'.
 */
public final class TimedJsonStreamParser extends StreamingParser {

    private static final Logger logger = LoggerFactory.getLogger(TimedJsonStreamParser.class);

    private List<TblColRef> allColumns;
    private final ObjectMapper mapper;
    private String tsColName = null;
    private String tsParser = null;
    private String separator = null;
    private boolean strictCheck = true;
    private final Map<String, Object> root = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private final Map<String, Object> tempMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private final Map<String, String[]> nameMap = new HashMap<>();
    public static final String EMBEDDED_PROPERTY_SEPARATOR = "|";

    private final JavaType mapType = MapType.construct(HashMap.class, SimpleType.construct(String.class), SimpleType.construct(Object.class));

    private AbstractTimeParser streamTimeParser;
    
    private long vcounter = 0;

    public TimedJsonStreamParser(List<TblColRef> allColumns, Map<String, String> properties) {
        this.allColumns = allColumns;
        if (properties == null) {
            properties = StreamingParser.defaultProperties;
        }

        tsColName = properties.get(PROPERTY_TS_COLUMN_NAME);
        tsParser = properties.get(PROPERTY_TS_PARSER);
        separator = properties.get(PROPERTY_EMBEDDED_SEPARATOR);
        strictCheck = Boolean.parseBoolean(properties.get(PROPERTY_STRICT_CHECK));

        if (!StringUtils.isEmpty(tsParser)) {
            try {
                Class clazz = Class.forName(tsParser);
                Constructor constructor = clazz.getConstructor(Map.class);
                streamTimeParser = (AbstractTimeParser) constructor.newInstance(properties);
            } catch (Exception e) {
                throw new IllegalStateException("Invalid StreamingConfig, tsParser " + tsParser + ", parserProperties " + properties + ".", e);
            }
        } else {
            throw new IllegalStateException("Invalid StreamingConfig, tsParser " + tsParser + ", parserProperties " + properties + ".");
        }
        mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.disable(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE);
        mapper.enable(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY);
    }

    @Override
    public List<StreamingMessageRow> parse(ByteBuffer buffer) {
        try {
            Map<String, Object> message = mapper.readValue(new ByteBufferBackedInputStream(buffer), mapType);
            root.clear();
            root.putAll(message);
            String tsStr = objToString(root.get(tsColName));
            long t = streamTimeParser.parseTime(tsStr);
            ArrayList<String> result = Lists.newArrayList();

            for (TblColRef column : allColumns) {
                final String columnName = column.getName().toLowerCase();
                if (populateDerivedTimeColumns(columnName, result, t) == false) {
                    result.add(getValueByKey(column, root));
                }
            }

            StreamingMessageRow streamingMessageRow = new StreamingMessageRow(result, 0, t, Collections.<String, Object>emptyMap());
            List<StreamingMessageRow> messageRowList = new ArrayList<StreamingMessageRow>();
            messageRowList.add(streamingMessageRow);
            return messageRowList;
        } catch (IOException e) {
            logger.error("error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean filter(StreamingMessageRow streamingMessageRow) {
        return true;
    }

    public String[] getEmbeddedPropertyNames(TblColRef column) {
        final String colName = column.getName().toLowerCase();
        String[] names = nameMap.get(colName);
        if (names == null) {
            String comment = column.getColumnDesc().getComment(); // use comment to parse the structure
            if (!StringUtils.isEmpty(comment) && comment.contains(EMBEDDED_PROPERTY_SEPARATOR)) {
                names = comment.toLowerCase().split("\\" + EMBEDDED_PROPERTY_SEPARATOR);
                nameMap.put(colName, names);
            } else if (colName.contains(separator)) { // deprecated, just be compitable for old version
                names = colName.toLowerCase().split(separator);
                nameMap.put(colName, names);
            }
        }

        return names;
    }

    protected String getValueByKey(TblColRef column, Map<String, Object> rootMap) throws IOException {
        final String key = column.getName().toLowerCase();
        if (rootMap.containsKey(key)) {
            return objToString(rootMap.get(key));
        }

        String[] names = getEmbeddedPropertyNames(column);

        if (names != null && names.length > 0) {
            tempMap.clear();
            tempMap.putAll(rootMap);
            for (int i = 0; i < names.length - 1; i++) {
                Object o = tempMap.get(names[i]);
                if (o instanceof Map) {
                    tempMap.clear();
                    tempMap.putAll((Map<String, Object>) o);
                } else if (strictCheck || vcounter++ % 100 == 0) {
                    final String msg = "Property '" + names[i] + "' value is not embedded JSON format. ";
                    logger.warn(msg);
                    if (strictCheck)
                        throw new IOException(msg);
                }
            }
            Object finalObject = tempMap.get(names[names.length - 1]);
            return objToString(finalObject);
        }

        return StringUtils.EMPTY;
    }

    public static String objToString(Object value) {
        if (value == null)
            return StringUtils.EMPTY;
        if (value.getClass().isArray())
            return String.valueOf(Arrays.asList((Object[]) value));
        return String.valueOf(value);
    }

}
