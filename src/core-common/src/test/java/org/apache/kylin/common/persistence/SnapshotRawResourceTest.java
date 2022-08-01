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

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.val;

@MetadataInfo(project = "ssb")
class SnapshotRawResourceTest {

    @BeforeEach
    void before() throws JsonProcessingException {
        val mockContent = new MockMetaContent("abc", 18);
        val mockContentJson = JsonUtil.writeValueAsBytes(mockContent);
        val resourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        resourceStore.putResourceWithoutCheck("/path/meta/abc", ByteSource.wrap(mockContentJson), 123, 101);
    }

    @Test
    void testRawResourceByteSourceSerializer() throws IOException {
        val resourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val rawSnapshotRes = new SnapshotRawResource(resourceStore.getResource("/path/meta/abc"));
        val mockContentSer = JsonUtil.readValue(rawSnapshotRes.getByteSource().read(), MockMetaContent.class);

        Assertions.assertEquals("abc", mockContentSer.getName());
        Assertions.assertEquals(18, mockContentSer.getAge());
    }

    @Test
    void testSnapShotRawResourceSerializer() throws IOException {
        val resourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val rawSnapshotRes = new SnapshotRawResource(resourceStore.getResource("/path/meta/abc"));

        val snapshotRawJson = JsonUtil.writeValueAsString(rawSnapshotRes);
        Assertions.assertEquals("{\"byte_source\":\"eyJuYW1lIjoiYWJjIiwiYWdlIjoxOH0=\",\"timestamp\":123,\"mvcc\":101}",
                snapshotRawJson);
    }

    @Test
    void testSnapShotRawResourceDeSerializer() throws IOException {
        val snapshotRawResDes = JsonUtil
                .readValue("{\"byte_source\":\"eyJuYW1lIjoiYWJjIiwiYWdlIjoxOH0=\",\"timestamp\":123,\"mvcc\":101}"
                        .getBytes(Charset.defaultCharset()), SnapshotRawResource.class);

        Assertions.assertEquals(101, snapshotRawResDes.getMvcc());
        Assertions.assertEquals(123, snapshotRawResDes.getTimestamp());

        val mockContentDeSer = JsonUtil.readValue(snapshotRawResDes.getByteSource().read(), MockMetaContent.class);

        Assertions.assertEquals(18, mockContentDeSer.getAge());
        Assertions.assertEquals("abc", mockContentDeSer.getName());
    }
}
