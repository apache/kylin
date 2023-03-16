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

package org.apache.kylin.common.util;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.ARGS_TYPE_CHECK;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class ArgsTypeJsonDeserializer {

    private ArgsTypeJsonDeserializer() {
        throw new IllegalStateException("ArgsTypeJsonDeserializer class");
    }

    public static class BooleanJsonDeserializer extends JsonDeserializer<Boolean> {

        private final List<String> boolList = Lists.newArrayList("true", "false", "TRUE", "FALSE", "null");

        @Override
        public Boolean getNullValue(DeserializationContext ctxt) {
            return Boolean.FALSE;
        }

        @Override
        public Boolean deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            try {
                String text = p.getText();
                if (StringUtils.isEmpty(text)) {
                    return Boolean.FALSE;
                }
                if (boolList.contains(text)) {
                    return Boolean.parseBoolean(text);
                }
                return p.getBooleanValue();
            } catch (Exception e) {
                throw new KylinException(ARGS_TYPE_CHECK, e, p.getText(), "Boolean");
            }
        }
    }

    public static class ListJsonDeserializer extends JsonDeserializer<List<Object>> {
        @Override
        public List<Object> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            try {
                return p.readValueAs(List.class);
            } catch (Exception e) {
                throw new KylinException(ARGS_TYPE_CHECK, e, p.getText(), "List");
            }
        }
    }

    public static class IntegerJsonDeserializer extends JsonDeserializer<Integer> {

        @Override
        public Integer getNullValue(DeserializationContext ctxt) {
            return 0;
        }

        @Override
        public Integer deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            try {
                String text = p.getText();
                if (StringUtils.isEmpty(text) || StringUtils.equals("null", text)) {
                    return 0;
                }
                return Integer.parseInt(text);
            } catch (Exception e) {
                throw new KylinException(ARGS_TYPE_CHECK, e, p.getText(), "Integer");
            }
        }
    }
}
