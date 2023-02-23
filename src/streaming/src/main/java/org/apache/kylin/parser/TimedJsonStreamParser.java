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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;

import lombok.SneakyThrows;

/**
 * default stream JSON parser
 * recursive parse JSON
 */
public class TimedJsonStreamParser extends AbstractDataParser<ByteBuffer> {

    private final ObjectMapper mapper = new ObjectMapper();

    @SneakyThrows
    @Override
    protected Map<String, Object> parse(ByteBuffer input) {
        Map<String, Object> flatMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        traverseJsonNode("", mapper.readTree(input.array()), flatMap);
        return flatMap;
    }

    private void traverseJsonNode(String currentPath, JsonNode jsonNode, Map<String, Object> flatmap) {
        if (jsonNode.isObject()) {
            ObjectNode objectNode = (ObjectNode) jsonNode;
            Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();
            String pathPrefix = currentPath.isEmpty() ? "" : currentPath + "_";

            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> entry = iter.next();
                traverseJsonNode(pathPrefix + entry.getKey(), entry.getValue(), flatmap);
            }
        } else if (jsonNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            if (arrayNode.size() == 0) {
                flatmap.put(currentPath, StringUtils.EMPTY);
            }

            for (int i = 0; i < arrayNode.size(); i++) {
                traverseJsonNode(currentPath + "_" + i, arrayNode.get(i), flatmap);
            }
        } else if (jsonNode.isValueNode()) {
            ValueNode valueNode = (ValueNode) jsonNode;
            getJsonValueByType(currentPath, flatmap, valueNode);
        }
    }

    private void getJsonValueByType(String currentPath, Map<String, Object> flatmap, ValueNode valueNode) {
        if (valueNode.isShort()) {
            addValueToFlatMap(flatmap, currentPath, valueNode.shortValue());
        } else if (valueNode.isInt()) {
            addValueToFlatMap(flatmap, currentPath, valueNode.intValue());
        } else if (valueNode.isLong()) {
            addValueToFlatMap(flatmap, currentPath, valueNode.longValue());
        } else if (valueNode.isBigDecimal()) {
            addValueToFlatMap(flatmap, currentPath, valueNode.decimalValue());
        } else if (valueNode.isFloat()) {
            addValueToFlatMap(flatmap, currentPath, valueNode.floatValue());
        } else if (valueNode.isDouble()) {
            addValueToFlatMap(flatmap, currentPath, valueNode.doubleValue());
        } else if (valueNode.isBoolean()) {
            addValueToFlatMap(flatmap, currentPath, valueNode.booleanValue());
        } else {
            addValueToFlatMap(flatmap, currentPath, valueNode.asText());
        }
    }

    private void addValueToFlatMap(Map<String, Object> flatmap, String key, Object val) {
        // to avoid key duplicated
        addValueToFlatMap(flatmap, key, val, 0);
    }

    private void addValueToFlatMap(Map<String, Object> flatmap, String key, Object val, int iteTime) {
        if (flatmap.containsKey(key)) {
            key = key + "_" + (++iteTime);
            addValueToFlatMap(flatmap, key, val, iteTime);
        } else {
            flatmap.put(key, val);
        }
    }
}
