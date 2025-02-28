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

package org.apache.kylin.common.persistence;

import static org.apache.kylin.common.persistence.ResourceStore.GLOBAL_PROJECT;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.flipkart.zjsonpatch.JsonDiff;
import com.flipkart.zjsonpatch.JsonPatch;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import lombok.extern.log4j.Log4j;

/**
 * overall, RawResource is immutable
 */
@Log4j
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RawResource {
    @JsonProperty("id")
    private Long id;

    @JsonProperty("uuid")
    private String uuid;

    @JsonProperty("meta_key")
    private String metaKey;

    @JsonProperty("content")
    private byte[] content;

    private byte[] contentDiff;

    @JsonProperty("ts")
    private Long ts;

    @JsonProperty("mvcc")
    private long mvcc;

    @JsonProperty("reserved_filed_1")
    private String reservedFiled1;

    @JsonProperty("reserved_filed_2")
    private byte[] reservedFiled2;

    @JsonProperty("reserved_filed_3")
    private byte[] reservedFiled3;

    private boolean broken = false;

    public RawResource(String metaKey, ByteSource byteSource, long timestamp, long mvcc) {
        this.metaKey = metaKey;
        this.ts = timestamp;
        this.mvcc = mvcc;
        if (byteSource != null) {
            try (InputStream is = byteSource.openStream(); DataInputStream din = new DataInputStream(is)) {
                content = IOUtils.toByteArray(din);
            } catch (IOException e) {
                throw new IllegalStateException("Error when construct RawResource from byteSource.", e);
            }
        }
    }

    public ByteSource getByteSource() {
        return content == null ? null : ByteSource.wrap(content);
    }

    public ByteSource getDiffByteSource() {
        return contentDiff == null ? null : ByteSource.wrap(contentDiff);
    }

    public String generateKeyWithType() {
        return MetadataType.mergeKeyWithType(getMetaKey(), getMetaType());
    }

    public MetadataType getMetaType() {
        return Arrays.stream(MetadataType.values()).filter(t -> t.getResourceClass().equals(this.getClass())).findAny()
                .orElseThrow(() -> new IllegalStateException("Can't find MetadataType for class: " + this.getClass()));
    }

    public String generateFilePath() {
        if (getMetaType() == MetadataType.ALL) {
            // For special files, eg. kylin.properties
            return getMetaKey();
        }
        return generateKeyWithType() + ".json";
    }

    public static RawResource constructResource(MetadataType type, ByteSource byteSource) {
        Class<?> resourceClass = type.getResourceClass();
        if (byteSource == null) {
            return (RawResource) ClassUtil.newInstance(resourceClass.getName());
        } else {
            byte[] bytes = null;
            try (InputStream is = byteSource.openStream(); DataInputStream din = new DataInputStream(is)) {
                bytes = IOUtils.toByteArray(din);
                val raw = (RawResource) JsonUtil.readValue(bytes, resourceClass);
                raw.setContent(bytes);
                return raw;
            } catch (IOException e) {
                if (bytes != null) {
                    val raw = (RawResource) ClassUtil.newInstance(resourceClass.getName());
                    raw.setContent(bytes);
                    raw.setBroken(true);
                    return raw;
                }
                throw new IllegalStateException("Error when construct RawResource from byteSource, type: " + type, e);
            }
        }
    }

    public static RawResource constructResource(MetadataType type, ByteSource byteSource, long ts, long mvcc,
            String metaKey) {
        val raw = constructResource(type, byteSource);
        raw.setMvcc(mvcc);
        raw.setTs(ts);
        raw.setMetaKey(metaKey);
        return raw;
    }

    public void fillContentDiffFromRaw(RawResource before) {
        fillContentDiffFromRaw(before, this);
    }

    private static void fillContentDiffFromRaw(RawResource before, RawResource after) {
        if (after.getContent() == null) {
            // Delete operation, no need to calculate diff
            return;
        }
        if (before == null && after.getMvcc() > 0) {
            throw new IllegalStateException("No pre-update data found, unable to calculate json diff! metaKey: "
                    + after.getMetaKey() + ", mvcc: " + after.getMvcc());
        }

        try {
            if (before == null) {
                // Add Operation
                return;
            }

            if (before.getMvcc() != after.getMvcc() - 1) {
                throw new KylinRuntimeException("Mvcc not match, unable to calculate json diff! metaKey: "
                        + after.getMetaKey() + ", before mvcc: " + before.getMvcc() + ", after mvcc: " + after.getMvcc());
            }

            JsonNode beforeJson = JsonUtil.readValue(before.getContent(), JsonNode.class);
            JsonNode afterJson = JsonUtil.readValue(after.getContent(), JsonNode.class);
            after.setContentDiff(JsonDiff.asJson(beforeJson, afterJson).toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new KylinRuntimeException("Failed to fill content diff from raw.", e);
        }
    }

    public static ByteSource applyContentDiffFromRaw(RawResource before, RawResource diff) {
        if (diff.getDiffByteSource() == null) {
            // Delete operation or Non-diff operation, no need to calculate diff
            return null;
        }
        if ((before == null || before.getByteSource() == null) && diff.getMvcc() > 0) {
            throw new KylinRuntimeException("No pre-update data found, unable to calculate json diff! metaKey: "
                    + diff.getMetaKey() + ", mvcc: " + diff.getMvcc());
        }

        try {
            if (before == null) {
                // Add Operation
                return diff.getDiffByteSource();
            }

            JsonNode beforeJson = JsonUtil.readValue(before.getContent(), JsonNode.class);
            JsonNode patch = JsonUtil.readValue(diff.getContentDiff(), JsonNode.class);
            JsonPatch.applyInPlace(patch, beforeJson);
            return ByteSource.wrap(JsonUtil.writeValueAsBytes(beforeJson));
        } catch (IOException e) {
            throw new KylinRuntimeException("Failed to fill content diff from raw.", e);
        }
    }

    public String getProject() {
        return GLOBAL_PROJECT;
    }

    public String getModelUuid() {
        return null;
    }

    public void setProject(String resPath) {
        // Filed project is only defined in partial subclasses.
        // Please override this method if needed.
    }

    public static class ByteSourceSerializer extends JsonSerializer<ByteSource> {
        @Override
        public void serialize(ByteSource value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            val bytes = value.read();
            gen.writeBinary(bytes);
        }
    }

    public static class ByteSourceDeserializer extends JsonDeserializer<ByteSource> {
        @Override
        public ByteSource deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException {
            val bytes = p.getBinaryValue();
            return ByteSource.wrap(bytes);
        }
    }
}
