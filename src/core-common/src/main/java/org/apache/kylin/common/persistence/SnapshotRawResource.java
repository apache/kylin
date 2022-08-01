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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SnapshotRawResource {

    @JsonProperty("byte_source")
    @JsonSerialize(using = RawResource.ByteSourceSerializer.class)
    @JsonDeserialize(using = RawResource.BytesourceDeserializer.class)
    private ByteSource byteSource;

    @JsonProperty("timestamp")
    private long timestamp;
    @JsonProperty("mvcc")
    private long mvcc;

    public SnapshotRawResource(RawResource rawResource) {
        this.byteSource = rawResource.getByteSource();
        this.timestamp = rawResource.getTimestamp();
        this.mvcc = rawResource.getMvcc();
    }
}
