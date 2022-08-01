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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.google.common.base.Preconditions;

public class JsonUtil {

    // reuse the object mapper to save memory footprint
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final ObjectMapper indentMapper = new ObjectMapper();
    private static final SimpleFilterProvider simpleFilterProvider = new SimpleFilterProvider()
            .setFailOnUnknownId(false);

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .setConfig(mapper.getSerializationConfig().withView(PersistenceView.class));
        mapper.setFilterProvider(simpleFilterProvider);
        indentMapper.configure(SerializationFeature.INDENT_OUTPUT, true)
                .setConfig(indentMapper.getSerializationConfig().withView(PersistenceView.class));
        indentMapper.setFilterProvider(simpleFilterProvider);
    }

    public static <T> T readValue(File src, Class<T> valueType) throws IOException {
        return mapper.readValue(src, valueType);
    }

    public static <T> T readValueQuietly(File src, Class<T> valueType) {
        try {
            return readValue(src, valueType);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read " + valueType.getName(), e);
        }
    }

    public static <T> T readValueQuietly(byte[] src, Class<T> valueType) {
        try {
            return readValue(src, valueType);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read " + valueType.getName(), e);
        }
    }

    public static <T> T readValue(String content, Class<T> valueType) throws IOException {
        return mapper.readValue(content, valueType);
    }

    public static <T> T readValue(Reader src, Class<T> valueType) throws IOException {
        return mapper.readValue(src, valueType);
    }

    public static <T> T readValue(InputStream src, Class<T> valueType) throws IOException {
        return mapper.readValue(src, valueType);
    }

    public static <T> T readValue(byte[] src, Class<T> valueType) throws IOException {
        return mapper.readValue(src, valueType);
    }

    public static <T> T readValue(String content, TypeReference<T> valueTypeRef) throws IOException {
        return mapper.readValue(content, valueTypeRef);
    }

    public static <T> T readValue(File src, TypeReference<T> valueTypeRef) throws IOException {
        return mapper.readValue(src, valueTypeRef);
    }

    public static <T> T readValue(InputStream src, TypeReference<T> valueTypeRef) throws IOException {
        return mapper.readValue(src, valueTypeRef);
    }

    public static Map<String, String> readValueAsMap(String content) throws IOException {
        TypeReference<HashMap<String, String>> typeRef = new TypeReference<HashMap<String, String>>() {
        };
        return mapper.readValue(content, typeRef);
    }

    public static Set<String> readValueAsSet(String content) throws IOException {
        TypeReference<HashSet<String>> typeRef = new TypeReference<HashSet<String>>() {
        };
        return mapper.readValue(content, typeRef);
    }

    public static List<String> readValueAsList(String content) throws IOException {
        TypeReference<ArrayList<String>> typeRef = new TypeReference<ArrayList<String>>() {
        };
        return mapper.readValue(content, typeRef);
    }

    public static JsonNode readValueAsTree(String content) throws IOException {
        return mapper.readTree(content);
    }

    public static void writeValueIndent(OutputStream out, Object value) throws IOException {
        indentMapper.writeValue(out, value);
    }

    public static void writeValue(OutputStream out, Object value) throws IOException {
        mapper.writeValue(out, value);
    }

    public static void writeValue(File out, Object value) throws IOException {
        writeValue(new FileOutputStream(out), value);
    }

    public static String writeValueAsString(Object value) throws JsonProcessingException {
        return mapper.writeValueAsString(value);
    }

    public static String writeValueAsStringQuietly(Object value) {
        try {
            return writeValueAsString(value);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot write " + value.getClass(), e);
        }
    }

    public static byte[] writeValueAsBytes(Object value) throws JsonProcessingException {
        return mapper.writeValueAsBytes(value);
    }

    public static byte[] writeValueAsIndentBytes(Object value) throws JsonProcessingException {
        return indentMapper.writeValueAsBytes(value);
    }

    public static String writeValueAsIndentString(Object value) throws JsonProcessingException {
        return indentMapper.writeValueAsString(value);
    }

    public static <T> T convert(Object obj, Class<T> valueType) {
        return mapper.convertValue(obj, valueType);
    }

    public static <T> T convert(Object obj, TypeReference<T> valueType) {
        return mapper.convertValue(obj, valueType);
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

    public static <T extends RootPersistentEntity> T copyForWrite(T entity, Serializer<T> serializer,
            @Nullable BiConsumer<T, String> initEntityAfterReload) {
        if (!entity.isCachedAndShared())
            return entity;
        else
            return copyBySerialization(entity, serializer, initEntityAfterReload);
    }

    public static <T extends RootPersistentEntity> T copyBySerialization(T entity, Serializer<T> serializer,
            @Nullable BiConsumer<T, String> initEntityAfterReload) {
        Preconditions.checkNotNull(entity);
        T copy;
        try {
            byte[] bytes;
            try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
                    DataOutputStream dout = new DataOutputStream(buf)) {
                serializer.serialize(entity, dout);
                bytes = buf.toByteArray();
            }

            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
                copy = serializer.deserialize(in);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        copy.setMvcc(entity.getMvcc());
        copy.setCachedAndShared(false);
        if (initEntityAfterReload != null) {
            initEntityAfterReload.accept(copy, entity.resourceName());
        }
        return copy;
    }

    public static boolean isJson(String content) {
        try {
            mapper.readTree(content);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public interface PersistenceView {
    }

    public interface PublicView extends PersistenceView {
    }
}
