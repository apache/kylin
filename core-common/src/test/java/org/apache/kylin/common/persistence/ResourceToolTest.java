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

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

public class ResourceToolTest extends LocalFileMetadataTestCase {
    private static final String dstPath = "../examples/test_metadata2/";
    private static final File DIR_1 = new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + ResourceStore.EXECUTE_RESOURCE_ROOT);
    private static final File DIR_2 = new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT);
    private static final String FILE_1 = ResourceStore.EXECUTE_RESOURCE_ROOT + "/1.json";
    private static final String FILE_2 = ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/2.json";
    private static final List<String> EXEC_FILES = Lists.newArrayList(FILE_1, FILE_2);

    @Before
    public void setup() throws Exception {
        FileUtils.forceMkdir(DIR_1);
        FileUtils.forceMkdir(DIR_2);
        FileUtils.write(new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + FILE_1), "");
        FileUtils.write(new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + FILE_2), "");
        FileUtils.forceMkdir(new File(dstPath));
        FileUtils.cleanDirectory(new File(dstPath));
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        FileUtils.deleteQuietly(new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + FILE_1));
        FileUtils.deleteQuietly(new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + FILE_2));
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

        new ResourceTool().copy(KylinConfig.getInstanceFromEnv(), dstConfig, "/");

        //After copy, two paths have same metadata
        NavigableSet<String> dstFiles = dstStore.listResourcesRecursively("/");
        NavigableSet<String> srcFiles = srcStore.listResourcesRecursively("/");
        Assert.assertTrue(srcFiles.containsAll(EXEC_FILES));
        Assert.assertFalse(dstFiles.containsAll(EXEC_FILES));
        srcFiles.removeAll(EXEC_FILES);
        Assert.assertEquals(srcFiles, dstFiles);
    }
}
