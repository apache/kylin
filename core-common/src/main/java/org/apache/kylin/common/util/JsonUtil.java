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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JsonUtil {

    private JsonUtil() {
        throw new IllegalStateException("Class JsonUtil is an utility class !");
    }

    // reuse the object mapper to save memory footprint
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectMapper indentMapper = new ObjectMapper();

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        indentMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    public static <T> T readValue(File src, Class<T> valueType)
            throws IOException, JsonParseException, JsonMappingException {
        return mapper.readValue(src, valueType);
    }

    public static <T> T readValue(String content, Class<T> valueType)
            throws IOException, JsonParseException, JsonMappingException {
        return mapper.readValue(content, valueType);
    }

    public static <T> T readValue(Reader src, Class<T> valueType)
            throws IOException, JsonParseException, JsonMappingException {
        return mapper.readValue(src, valueType);
    }

    public static <T> T readValue(InputStream src, Class<T> valueType)
            throws IOException, JsonParseException, JsonMappingException {
        return mapper.readValue(src, valueType);
    }

    public static <T> T readValue(byte[] src, Class<T> valueType)
            throws IOException, JsonParseException, JsonMappingException {
        return mapper.readValue(src, valueType);
    }

    public static <T> T readValue(String content, TypeReference<T> valueTypeRef)
            throws IOException, JsonParseException, JsonMappingException {
        return mapper.readValue(content, valueTypeRef);
    }

    public static Map<String, String> readValueAsMap(String content) throws IOException {
        TypeReference<HashMap<String, String>> typeRef = new TypeReference<HashMap<String, String>>() {
        };
        return mapper.readValue(content, typeRef);
    }

    public static JsonNode readValueAsTree(String content) throws IOException {
        return mapper.readTree(content);
    }

    public static void writeValueIndent(OutputStream out, Object value)
            throws IOException, JsonGenerationException, JsonMappingException {
        indentMapper.writeValue(out, value);
    }

    public static void writeValue(OutputStream out, Object value)
            throws IOException, JsonGenerationException, JsonMappingException {
        mapper.writeValue(out, value);
    }

    public static String writeValueAsString(Object value) throws JsonProcessingException {
        return mapper.writeValueAsString(value);
    }

    public static byte[] writeValueAsBytes(Object value) throws JsonProcessingException {
        return mapper.writeValueAsBytes(value);
    }

    public static String writeValueAsIndentString(Object value) throws JsonProcessingException {
        return indentMapper.writeValueAsString(value);
    }

    public static <T> T deepCopy(T src, Class<T> valueType) throws IOException {
        String s = mapper.writeValueAsString(src);
        return mapper.readValue(s, valueType);
    }

    public static <T> T deepCopy(T src, TypeReference<T> valueType) throws IOException {
        String s = mapper.writeValueAsString(src);
        return mapper.readValue(s, valueType);
    }

    public static <T> T deepCopyQuietly(T src, Class<T> valueType) {
        try {
            return deepCopy(src, valueType);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot copy " + valueType.getName(), e);
        }
    }

    public static <T> T deepCopyQuietly(T src, TypeReference<T> typeReference) {
        try {
            return deepCopy(src, typeReference);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot copy " + typeReference.getType(), e);
        }
    }

    public static boolean isJson(String content) {
        try {
            mapper.readTree(content);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}