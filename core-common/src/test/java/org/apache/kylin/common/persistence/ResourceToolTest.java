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

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ResourceToolTest extends LocalFileMetadataTestCase {
    private static final String dstPath = "../examples/test_metadata2/";

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        FileUtils.forceMkdir(new File(dstPath));
        FileUtils.cleanDirectory(new File(dstPath));
    }

    @After
    public void after() throws Exception {
        File directory = new File(dstPath);
        try {
            FileUtils.deleteDirectory(directory);
        } catch (IOException e) {
            if (directory.exists() && directory.list().length > 0)
                throw new IllegalStateException("Can't delete directory " + directory, e);
        }
        this.cleanupTestMetadata();
    }

    @Test
    public void testCopy() throws IOException {
        KylinConfig dstConfig = KylinConfig.createInstanceFromUri(dstPath);
        ResourceStore srcStore = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        ResourceStore dstStore = ResourceStore.getStore(dstConfig);

        //metadata under source path and destination path are not equal before copy
        Assert.assertNotEquals(srcStore.listResources("/"), dstStore.listResources("/"));

        ResourceTool.copy(KylinConfig.getInstanceFromEnv(), dstConfig, "/");

        //After copy, two paths have same metadata
        Assert.assertEquals(srcStore.listResources("/"), dstStore.listResources("/"));
    }
}
