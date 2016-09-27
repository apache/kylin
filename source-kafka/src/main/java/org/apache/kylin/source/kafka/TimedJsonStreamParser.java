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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.StreamingMessage;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.google.common.collect.Lists;

/**
 * each json message with a "timestamp" field
 */
public final class TimedJsonStreamParser extends StreamingParser {

    private static final Logger logger = LoggerFactory.getLogger(TimedJsonStreamParser.class);

    private List<TblColRef> allColumns;
    private final ObjectMapper mapper;
    private String tsColName = "timestamp";
    private String tsParser = "org.apache.kylin.source.kafka.DefaultTimeParser";
    private final JavaType mapType = MapType.construct(HashMap.class, SimpleType.construct(String.class), SimpleType.construct(Object.class));

    private AbstractTimeParser streamTimeParser;

    public TimedJsonStreamParser(List<TblColRef> allColumns, String propertiesStr) {
        this.allColumns = allColumns;
        String[] properties = null;
        if (!StringUtils.isEmpty(propertiesStr)) {
            properties = propertiesStr.split(";");
            for (String prop : properties) {
                try {
                    String[] parts = prop.split("=");
                    if (parts.length == 2) {
                        switch (parts[0]) {
                        case "tsColName":
                            this.tsColName = parts[1];
                            break;
                        case "tsParser":
                            this.tsParser = parts[1];
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
        }

        logger.info("TimedJsonStreamParser with tsColName {}", tsColName);

        if (!StringUtils.isEmpty(tsParser)) {
            try {
                Class clazz = Class.forName(tsParser);
                Constructor constructor = clazz.getConstructor(String[].class);
                streamTimeParser = (AbstractTimeParser) constructor.newInstance((Object)properties);
            } catch (Exception e) {
                throw new IllegalStateException("Invalid StreamingConfig, tsParser " + tsParser + ", parserProperties " + propertiesStr + ".", e);
            }
        } else {
            throw new IllegalStateException("Invalid StreamingConfig, tsParser " + tsParser + ", parserProperties " + propertiesStr + ".");
        }
        mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.disable(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE);
        mapper.enable(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY);
    }

    @Override
    public StreamingMessage parse(ByteBuffer buffer) {
        try {
            Map<String, Object> message = mapper.readValue(new ByteBufferBackedInputStream(buffer), mapType);
            Map<String, Object> root = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            root.putAll(message);
            String tsStr = String.valueOf(root.get(tsColName));
            long t = streamTimeParser.parseTime(tsStr);
            ArrayList<String> result = Lists.newArrayList();

            for (TblColRef column : allColumns) {
                String columnName = column.getName().toLowerCase();

                if (populateDerivedTimeColumns(columnName, result, t) == false) {
                    String x = String.valueOf(root.get(columnName));
                    result.add(x);
                }
            }

            return new StreamingMessage(result, 0, t, Collections.<String, Object> emptyMap());
        } catch (IOException e) {
            logger.error("error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean filter(StreamingMessage streamingMessage) {
        return true;
    }

}
