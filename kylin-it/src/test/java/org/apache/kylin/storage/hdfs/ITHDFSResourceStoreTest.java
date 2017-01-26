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

package org.apache.kylin.storage.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStoreTest;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.common.util.HadoopUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class ITHDFSResourceStoreTest extends HBaseMetadataTestCase {

    KylinConfig kylinConfig;
    FileSystem fs;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        kylinConfig = KylinConfig.getInstanceFromEnv();
        fs = HadoopUtil.getWorkingFileSystem();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasic() throws Exception {
        String oldUrl = kylinConfig.getMetadataUrl();
        String path = "/kylin/kylin_metadata/metadata";
        kylinConfig.setProperty("kylin.metadata.url", path + "@hdfs");
        HDFSResourceStore store = new HDFSResourceStore(kylinConfig);
        ResourceStoreTest.testAStore(store);
        kylinConfig.setProperty("kylin.metadata.url", oldUrl);
        assertTrue(fs.exists(new Path(path)));
    }

    @Test
    public void testQalifiedName() throws Exception {
        String oldUrl = kylinConfig.getMetadataUrl();
        String path = "hdfs:///kylin/kylin_metadata/metadata_test1";
        kylinConfig.setProperty("kylin.metadata.url", path + "@hdfs");
        HDFSResourceStore store = new HDFSResourceStore(kylinConfig);
        ResourceStoreTest.testAStore(store);
        kylinConfig.setProperty("kylin.metadata.url", oldUrl);
        assertTrue(fs.exists(new Path(path)));
    }

    @Test
    public void testFullQalifiedName() throws Exception {
        String oldUrl = kylinConfig.getMetadataUrl();
        String path = "hdfs://sandbox.hortonworks.com:8020/kylin/kylin_metadata/metadata_test2";
        kylinConfig.setProperty("kylin.metadata.url", path + "@hdfs");
        HDFSResourceStore store = new HDFSResourceStore(kylinConfig);
        ResourceStoreTest.testAStore(store);
        kylinConfig.setProperty("kylin.metadata.url", oldUrl);
        assertTrue(fs.exists(new Path(path)));
    }


}
