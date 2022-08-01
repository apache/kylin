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
package org.apache.kylin.tool.obf;

import java.io.File;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class MappingRecorderTest extends NLocalFileMetadataTestCase {
    @Test
    public void testBasic() throws Exception {
        File file = File.createTempFile("test", "txt");
        MappingRecorder mappingRecorder = new MappingRecorder(file);
        mappingRecorder.addMapping(ObfCatalog.NONE, "test");
        val map = mappingRecorder.getMapping();
        Assert.assertEquals(1, map.size());
        Assert.assertEquals(1, map.get("NONE").size());
        Assert.assertEquals("NONE#3556498", map.get("NONE").get("test"));
    }

}
