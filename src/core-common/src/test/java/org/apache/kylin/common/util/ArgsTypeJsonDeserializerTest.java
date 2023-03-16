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

import java.util.HashMap;

import org.apache.kylin.common.exception.KylinException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

import lombok.Data;

class ArgsTypeJsonDeserializerTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    void testDeserialize() throws Exception {
        {
            // "null" -> false
            // "null" -> 0
            HashMap<Object, Object> map = Maps.newHashMap();
            map.put("bol_value", "null");
            map.put("int_value", "null");
            String jsonStr = mapper.writeValueAsString(map);
            MockRequest request = mapper.readValue(jsonStr, MockRequest.class);
            Assertions.assertEquals(false, request.getBolValue());
            Assertions.assertEquals(0, request.getIntValue());
        }

        {
            // "" -> false
            // "" -> 0
            HashMap<Object, Object> map = Maps.newHashMap();
            map.put("bol_value", "");
            map.put("int_value", "");
            String jsonStr = mapper.writeValueAsString(map);
            MockRequest request = mapper.readValue(jsonStr, MockRequest.class);
            Assertions.assertEquals(false, request.getBolValue());
            Assertions.assertEquals(0, request.getIntValue());
        }

        {
            // null -> false
            // null -> 0
            HashMap<Object, Object> map = Maps.newHashMap();
            map.put("bol_value", null);
            map.put("int_value", null);
            String jsonStr = mapper.writeValueAsString(map);
            MockRequest request = mapper.readValue(jsonStr, MockRequest.class);
            Assertions.assertEquals(false, request.getBolValue());
            Assertions.assertEquals(0, request.getIntValue());
        }

        {
            // null -> true
            // null -> 1
            HashMap<Object, Object> map = Maps.newHashMap();
            String jsonStr = mapper.writeValueAsString(map);
            MockRequest request = mapper.readValue(jsonStr, MockRequest.class);
            Assertions.assertEquals(true, request.getBolValue());
            Assertions.assertEquals(1, request.getIntValue());
        }

        {
            // "true" -> true
            // "99" -> 99
            HashMap<Object, Object> map = Maps.newHashMap();
            map.put("bol_value", "true");
            map.put("int_value", "99");
            String jsonStr = mapper.writeValueAsString(map);
            MockRequest request = mapper.readValue(jsonStr, MockRequest.class);
            Assertions.assertEquals(true, request.getBolValue());
            Assertions.assertEquals(99, request.getIntValue());
        }

        {
            // "TRUE" -> true
            // "99" -> 99
            HashMap<Object, Object> map = Maps.newHashMap();
            map.put("bol_value", "true");
            map.put("int_value", "99");
            String jsonStr = mapper.writeValueAsString(map);
            MockRequest request = mapper.readValue(jsonStr, MockRequest.class);
            Assertions.assertEquals(true, request.getBolValue());
            Assertions.assertEquals(99, request.getIntValue());
        }

        {
            // "abd" -> exception
            HashMap<Object, Object> map = Maps.newHashMap();
            map.put("bol_value", "abc");
            String jsonStr = mapper.writeValueAsString(map);
            try {
                mapper.readValue(jsonStr, MockRequest.class);
            } catch (Exception e) {
                Assertions.assertTrue(e instanceof JsonMappingException);
                Assertions.assertTrue(e.getCause() instanceof KylinException);
                KylinException kylinException = (KylinException) e.getCause();
                Assertions.assertEquals(ARGS_TYPE_CHECK.getErrorCode().getCode(),
                        kylinException.getErrorCode().getCodeString());
            }
        }

        {
            // "abc" -> exception
            HashMap<Object, Object> map = Maps.newHashMap();
            map.put("int_value", "abc");
            String jsonStr = mapper.writeValueAsString(map);
            try {
                mapper.readValue(jsonStr, MockRequest.class);
            } catch (Exception e) {
                Assertions.assertTrue(e instanceof JsonMappingException);
                Assertions.assertTrue(e.getCause() instanceof KylinException);
                KylinException kylinException = (KylinException) e.getCause();
                Assertions.assertEquals(ARGS_TYPE_CHECK.getErrorCode().getCode(),
                        kylinException.getErrorCode().getCodeString());
            }
        }
    }

    @Data
    static class MockRequest {
        @JsonDeserialize(using = ArgsTypeJsonDeserializer.BooleanJsonDeserializer.class)
        @JsonProperty("bol_value")
        private Boolean bolValue = true;

        @JsonDeserialize(using = ArgsTypeJsonDeserializer.IntegerJsonDeserializer.class)
        @JsonProperty("int_value")
        private Integer intValue = 1;
    }
}
