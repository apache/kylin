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

import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.resources.ProjectRawResource;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;

@MetadataInfo
class RawResourceTest {

    @Test
    void testRawResourceByteSourceSerializer() throws IOException {
        val mockContent = new MockMetaContent("abc", 18);
        val mockContentJson = JsonUtil.writeValueAsBytes(mockContent);
        val resourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        String resPath = MetadataType.mergeKeyWithType("abc", MetadataType.SYSTEM);
        resourceStore.putResourceWithoutCheck(resPath, ByteSource.wrap(mockContentJson), 123, 101);
        val rawRes = resourceStore.getResource(resPath);
        val mockContentSer = JsonUtil.readValue(rawRes.getByteSource().read(), MockMetaContent.class);

        Assertions.assertEquals("abc", mockContentSer.getName());
        Assertions.assertEquals(18, mockContentSer.getAge());
    }

    @Test
    void testFillContentDiffFromRaw() throws IOException {
        ProjectRawResource p1 = RawResourceTool.createProjectRawResource("p1", 0);
        ProjectRawResource p2 = RawResourceTool.createProjectRawResource("p1_copy", 1, p1.getUuid());
        p2.fillContentDiffFromRaw(p1);
        Assertions.assertEquals("[{\"op\":\"replace\",\"path\":\"/meta_key\",\"value\":\"p1_copy\"}," //
                + "{\"op\":\"replace\",\"path\":\"/name\",\"value\":\"p1_copy\"}]", //
                new String(p2.getDiffByteSource().read(), Charset.defaultCharset()));
    }

    @Test
    void testExceptionWhenFillContentDiffFromRaw() {
        ProjectRawResource p1 = RawResourceTool.createProjectRawResource("p1", 0);
        ProjectRawResource p2 = RawResourceTool.createProjectRawResource("p1_copy", 2, p1.getUuid());

        // before is null but the mvcc of raw not equal to 0
        Assertions.assertThrows(IllegalStateException.class, () -> p2.fillContentDiffFromRaw(null));
        // mvcc gap (2 - 0) is greater then 1
        Assertions.assertThrows(KylinRuntimeException.class, () -> p2.fillContentDiffFromRaw(p1));
        //content is broken
        p2.setMvcc(1L);
        p2.setContent("abc".getBytes());
        Assertions.assertThrows(KylinRuntimeException.class, () -> p2.fillContentDiffFromRaw(p1));
    }

    @Test
    void testApplyContentDiffFromRaw() throws IOException {
        ProjectRawResource p1 = RawResourceTool.createProjectRawResource("p1", 0);
        ProjectRawResource p2 = RawResourceTool.createProjectRawResource("p1_copy", 1, p1.getUuid());
        p2.fillContentDiffFromRaw(p1);

        RawResource diff = new RawResource();
        Assertions.assertNull(RawResource.applyContentDiffFromRaw(p1, diff));

        diff.setContentDiff(p2.getContentDiff());
        Assertions.assertArrayEquals(diff.getContentDiff(), RawResource.applyContentDiffFromRaw(null, diff).read());

        ProjectRawResource applied = (ProjectRawResource) RawResource.constructResource(MetadataType.PROJECT,
                RawResource.applyContentDiffFromRaw(p1, diff));
        Assertions.assertEquals(p2.getName(), applied.getName());
    }

    @Test
    void testExceptionOnApplyContentDiffFromRaw() {
        ProjectRawResource p1 = RawResourceTool.createProjectRawResource("p1", 0);

        RawResource diff = new RawResource();
        diff.setMvcc(1L);
        diff.setContentDiff("abc".getBytes());
        Assertions.assertThrows(KylinRuntimeException.class, () -> RawResource.applyContentDiffFromRaw(null, diff));
        Assertions.assertThrows(KylinRuntimeException.class, () -> RawResource.applyContentDiffFromRaw(p1, diff));
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
