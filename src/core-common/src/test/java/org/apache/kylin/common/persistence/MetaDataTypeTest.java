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

import org.apache.kylin.common.util.Pair;
import org.junit.Assert;
import org.junit.Test;

public class MetaDataTypeTest {

    @Test
    public void testSplitKeyWithType() {
        String normalPath = "PROJECT/default";
        String rootPath = "/";
        String singleMetaKey = "abc";
        String illegalType = "proj/abc";
        String illegalPath = "/PROJECT/default";

        Pair<MetadataType, String> typeAndKey = MetadataType.splitKeyWithType(normalPath);
        Assert.assertEquals(MetadataType.PROJECT, typeAndKey.getFirst());
        Assert.assertEquals("default", typeAndKey.getSecond());

        typeAndKey = MetadataType.splitKeyWithType(rootPath);
        Assert.assertEquals(MetadataType.ALL, typeAndKey.getFirst());
        Assert.assertEquals(rootPath, typeAndKey.getSecond());

        Assert.assertThrows(IllegalArgumentException.class, () -> MetadataType.splitKeyWithType(singleMetaKey));
        Assert.assertThrows(IllegalArgumentException.class, () -> MetadataType.splitKeyWithType(illegalType));
        Assert.assertThrows(IllegalArgumentException.class, () -> MetadataType.splitKeyWithType(illegalPath));
    }

    @Test
    public void testMergeKeyWithType() {
        String normalPath = "PROJECT/default";
        String rootPath = "/";
        String singleMetaKey = "abc";

        Assert.assertEquals("PROJECT/abc", MetadataType.mergeKeyWithType(singleMetaKey, MetadataType.PROJECT));
        Assert.assertEquals(normalPath, MetadataType.mergeKeyWithType(normalPath, MetadataType.ALL));
        Assert.assertEquals(rootPath, MetadataType.mergeKeyWithType(rootPath, MetadataType.ALL));

        Assert.assertThrows(IllegalArgumentException.class,
                () -> MetadataType.mergeKeyWithType(singleMetaKey, MetadataType.ALL));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> MetadataType.mergeKeyWithType(rootPath, MetadataType.PROJECT));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> MetadataType.mergeKeyWithType(normalPath, MetadataType.PROJECT));
    }
}
