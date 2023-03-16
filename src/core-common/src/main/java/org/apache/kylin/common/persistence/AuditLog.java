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

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AuditLog {

    private long id;

    private String resPath;

    @JsonSerialize(using = ByteSourceSerializer.class)
    @JsonDeserialize(using = ByteSourceDeserializer.class)
    private ByteSource byteSource;

    private Long timestamp;

    private Long mvcc;

    private String unitId;

    private String operator;

    private String instance;

    public static class ByteSourceSerializer extends JsonSerializer<ByteSource> {

        @Override
        public void serialize(ByteSource byteSource, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeBinary(byteSource.read());
        }
    }

    public static class ByteSourceDeserializer extends JsonDeserializer<ByteSource> {

        @Override
        public ByteSource deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException {
            return ByteSource.wrap(jsonParser.getBinaryValue());
        }
    }
}
