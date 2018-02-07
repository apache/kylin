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

import java.io.File;
import java.lang.reflect.Field;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HDFSResourceStoreTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testListResourcesImpl() throws Exception {
        String path = "../examples/test_metadata/";
        String cp = new File(path).getCanonicalFile().getPath();
        FileSystem fs = HadoopUtil.getFileSystem(cp);
        HDFSResourceStore store = new HDFSResourceStore(KylinConfig.getInstanceFromEnv(),
                StorageURL.valueOf("hdfs@hdfs"));
        Field field = store.getClass().getDeclaredField("fs");
        field.setAccessible(true);
        field.set(store, fs);

        File f1 = new File(cp + "/resource/resource/e1.json");
        File f2 = new File(cp + "/resource/resource/e2.json");
        if (!f1.getParentFile().exists()) {
            if (!f1.getParentFile().mkdirs()) {
                throw new RuntimeException("Can not create dir.");
            }
        }
        if (!(f1.createNewFile() && f2.createNewFile())) {
            throw new RuntimeException("Can not create file.");
        }

        Path p = new Path(cp);
        TreeSet<String> resources = store.getAllFilePath(new Path(p, "resource"), "/resource/");
        TreeSet<String> expected = new TreeSet<>();
        expected.add("/resource/resource/e1.json");
        expected.add("/resource/resource/e2.json");
        Assert.assertEquals(expected, resources);
    }
}
