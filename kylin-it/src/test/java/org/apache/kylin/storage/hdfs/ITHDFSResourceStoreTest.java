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

import static junit.framework.TestCase.assertTrue;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStoreTest;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.common.util.HadoopUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ITHDFSResourceStoreTest extends HBaseMetadataTestCase {

    KylinConfig kylinConfig;
    FileSystem fs;
    String workingDir;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        kylinConfig = KylinConfig.getInstanceFromEnv();
        fs = HadoopUtil.getWorkingFileSystem();
        workingDir = getHdfsWorkingDirWithoutScheme(kylinConfig);
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    private String getHdfsWorkingDirWithoutScheme(KylinConfig kylinConfig) {
        String hdfsWorkingDir = kylinConfig.getHdfsWorkingDirectory();
        int thirdIndex = StringUtils.ordinalIndexOf(hdfsWorkingDir, "/", 3);
        int fourthIndex = StringUtils.ordinalIndexOf(hdfsWorkingDir, "/", 5);
        return hdfsWorkingDir.substring(thirdIndex, fourthIndex);
    }

    @Test
    public void testBasic() throws Exception {
        String path = workingDir + "/metadata_test1";
        doTestWithPath(path);
    }

    @Test
    public void testQalifiedName() throws Exception {
        String path = "hdfs://" + workingDir + "/metadata_test2";
        doTestWithPath(path);
    }

    @Test
    public void testFullQalifiedName() throws Exception {
        String path = fs.getUri() + workingDir + "/metadata_test3";
        doTestWithPath(path);
    }

    private void doTestWithPath(String path) throws Exception {
        String oldUrl = kylinConfig.getMetadataUrl();
        kylinConfig.setProperty("kylin.metadata.url", path + "@hdfs");
        HDFSResourceStore store = new HDFSResourceStore(kylinConfig);
        ResourceStoreTest.testAStore(store);
        kylinConfig.setProperty("kylin.metadata.url", oldUrl);
        assertTrue(fs.exists(new Path(path)));
    }

}
