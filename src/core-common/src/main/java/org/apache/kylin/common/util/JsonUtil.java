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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.file.Files;
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
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.shaded.jackson.datatype.guava.GuavaModule;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.databind.util.LRUMap;

public class JsonUtil {

    private static final SimpleFilterProvider SIMPLE_FILTER_PROVIDER = new SimpleFilterProvider()
            .setFailOnUnknownId(false);
    public static final TypeFactory CUSTOM_TYPE_FACTORY = TypeFactory.defaultInstance()
            .withCache(new LRUMap<>(16, 2000));
    // restrict classes loadable via @JsonTypeInfo(Id.CLASS) ids (Event, SegmentRange, ...)
    // to Kylin's own types, blocking deserialization gadget attacks
    public static final PolymorphicTypeValidator KYLIN_TYPE_VALIDATOR = BasicPolymorphicTypeValidator.builder()
            .allowIfSubType("org.apache.kylin.").build();

    // reuse the object mapper to save memory footprint
    private static final ObjectMapper MAPPER = withPersistenceView(JsonMapper.builder()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
            .filterProvider(SIMPLE_FILTER_PROVIDER).typeFactory(CUSTOM_TYPE_FACTORY).addModule(new GuavaModule())
            .polymorphicTypeValidator(KYLIN_TYPE_VALIDATOR).build());

    private static final ObjectMapper INDENT_MAPPER = withPersistenceView(JsonMapper.builder()
            .configure(SerializationFeature.INDENT_OUTPUT, true).filterProvider(SIMPLE_FILTER_PROVIDER)
            .typeFactory(CUSTOM_TYPE_FACTORY).addModule(new GuavaModule())
            .polymorphicTypeValidator(KYLIN_TYPE_VALIDATOR).build());

    private static final ObjectMapper DEFAULT_MAPPER = JsonMapper.builder()
            .polymorphicTypeValidator(KYLIN_TYPE_VALIDATOR).build();

    // MapperBuilder has no active-view setter, so apply it on the built mapper
    private static ObjectMapper withPersistenceView(ObjectMapper mapper) {
        return mapper.setConfig(mapper.getSerializationConfig().withView(PersistenceView.class));
    }

    public static ArrayNode createArrayNode() {
        return MAPPER.createArrayNode();
    }

    public static <T> T readValue(File src, Class<T> valueType) throws IOException {
        return MAPPER.readValue(src, valueType);
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
        return MAPPER.readValue(content, valueType);
    }

    public static <T> T readValueDefault(String content, Class<T> valueType) throws IOException {
        return DEFAULT_MAPPER.readValue(content, valueType);
    }

    public static <T> T readValue(Reader src, Class<T> valueType) throws IOException {
        return MAPPER.readValue(src, valueType);
    }

    public static <T> T readValue(InputStream src, Class<T> valueType) throws IOException {
        return MAPPER.readValue(src, valueType);
    }

    public static <T> T readValue(byte[] src, Class<T> valueType) throws IOException {
        return MAPPER.readValue(src, valueType);
    }

    public static <T> ObjectNode valueToTree(T value) {
        return MAPPER.valueToTree(value);
    }

    public static <T> T readValue(String content, TypeReference<T> valueTypeRef) throws IOException {
        return MAPPER.readValue(content, valueTypeRef);
    }

    public static <T> T readValue(File src, TypeReference<T> valueTypeRef) throws IOException {
        return MAPPER.readValue(src, valueTypeRef);
    }

    public static <T> T readValue(InputStream src, TypeReference<T> valueTypeRef) throws IOException {
        return MAPPER.readValue(src, valueTypeRef);
    }

    public static Map<String, String> readValueAsMap(String content) throws IOException {
        TypeReference<HashMap<String, String>> typeRef = new TypeReference<HashMap<String, String>>() {
        };
        return MAPPER.readValue(content, typeRef);
    }

    public static Set<String> readValueAsSet(String content) throws IOException {
        TypeReference<HashSet<String>> typeRef = new TypeReference<HashSet<String>>() {
        };
        return MAPPER.readValue(content, typeRef);
    }

    public static List<String> readValueAsList(String content) throws IOException {
        TypeReference<ArrayList<String>> typeRef = new TypeReference<ArrayList<String>>() {
        };
        return MAPPER.readValue(content, typeRef);
    }

    public static JsonNode readValueAsTree(String content) throws IOException {
        return MAPPER.readTree(content);
    }

    public static JsonNode readValueAsTreeDefault(String content) throws IOException {
        return DEFAULT_MAPPER.readTree(content);
    }

    public static JsonNode readValueAsTreeDefault(byte[] content) throws IOException {
        return DEFAULT_MAPPER.readTree(content);
    }

    public static JsonNode readValueAsTreeDefault(InputStream iso) throws IOException {
        return DEFAULT_MAPPER.readTree(iso);
    }

    public static void writeValueIndent(OutputStream out, Object value) throws IOException {
        INDENT_MAPPER.writeValue(out, value);
    }

    public static void writeValueIndentDefault(OutputStream out, Object value) throws IOException {
        DEFAULT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(out, value);
    }

    public static void writeValue(OutputStream out, Object value) throws IOException {
        MAPPER.writeValue(out, value);
    }

    public static void writeValue(File out, Object value) throws IOException {
        writeValue(Files.newOutputStream(out.toPath()), value);
    }

    public static <T> String writeValueAsStringForCollection(Object value, TypeReference<T> ref)
            throws JsonProcessingException {
        return MAPPER.writerFor(ref).writeValueAsString(value);
    }

    public static String writeValueAsString(Object value) throws JsonProcessingException {
        return MAPPER.writeValueAsString(value);
    }

    public static String writeValueAsStringDefault(Object value) throws JsonProcessingException {
        return DEFAULT_MAPPER.writeValueAsString(value);
    }

    public static String writeValueAsStringQuietly(Object value) {
        try {
            return writeValueAsString(value);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot write " + value.getClass(), e);
        }
    }

    public static byte[] writeValueAsBytes(Object value) throws JsonProcessingException {
        return MAPPER.writeValueAsBytes(value);
    }

    public static byte[] writeValueAsIndentBytes(Object value) throws JsonProcessingException {
        return INDENT_MAPPER.writeValueAsBytes(value);
    }

    public static String writeValueAsIndentString(Object value) throws JsonProcessingException {
        return INDENT_MAPPER.writeValueAsString(value);
    }

    public static String writeValueAsStringWithPretty(Object value) throws JsonProcessingException {
        return INDENT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(value);
    }

    public static <T> T convert(Object obj, Class<T> valueType) {
        return MAPPER.convertValue(obj, valueType);
    }

    public static <T> T convertDefault(Object obj, Class<T> valueType) {
        return DEFAULT_MAPPER.convertValue(obj, valueType);
    }

    public static <T> T convert(Object obj, TypeReference<T> valueType) {
        return MAPPER.convertValue(obj, valueType);
    }

    public static <T> T deepCopy(T src, Class<T> valueType) throws IOException {
        String s = MAPPER.writeValueAsString(src);
        return MAPPER.readValue(s, valueType);
    }

    public static <T> T deepCopy(T src, TypeReference<T> valueType) throws IOException {
        String s = MAPPER.writeValueAsString(src);
        return MAPPER.readValue(s, valueType);
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
        if (!entity.isCachedAndShared()) {
            return entity;
        }
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
            MAPPER.readTree(content);
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
