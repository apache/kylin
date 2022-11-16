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

package io.kyligence.kap.clickhouse.job;


import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class SegmentFileProviderTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testGetAllFilePaths() {
        // isBuildFilesSeparationEnabled false
        KylinConfig testConfig = getTestConfig();
        String rootPath = new File(testConfig.getHdfsWorkingDirectory()).getParent();
        Assert.assertTrue(new SegmentFileProvider(rootPath).getAllFilePaths().isEmpty());
        // isBuildFilesSeparationEnabled true
        testConfig.setProperty("kylin.engine.submit-hadoop-conf-dir", "/kylin");
        testConfig.setProperty("kylin.env.hdfs-write-working-dir", "file://abc");
        Assert.assertTrue(new SegmentFileProvider(rootPath).getAllFilePaths().isEmpty());
        // reset
        testConfig.setProperty("kylin.engine.submit-hadoop-conf-dir", "");
        testConfig.setProperty("kylin.env.hdfs-write-working-dir", "");
    }
}