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

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;

@MetadataInfo(project = "ssb")
class RawResourceTest {

    @Test
    void testRawResourceByteSourceSerializer() throws IOException {
        val mockContent = new MockMetaContent("abc", 18);
        val mockContentJson = JsonUtil.writeValueAsBytes(mockContent);
        val resourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        resourceStore.putResourceWithoutCheck("/path/meta/abc", ByteSource.wrap(mockContentJson), 123, 101);
        val rawRes = resourceStore.getResource("/path/meta/abc");
        val mockContentSer = JsonUtil.readValue(rawRes.getByteSource().read(), MockMetaContent.class);

        Assertions.assertEquals("abc", mockContentSer.getName());
        Assertions.assertEquals(18, mockContentSer.getAge());
    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
class MockMetaContent {
    @JsonProperty("name")
    private String name;
    @JsonProperty("age")
    private int age;
}
