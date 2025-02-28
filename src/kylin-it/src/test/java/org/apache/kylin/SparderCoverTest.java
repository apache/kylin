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

package org.apache.kylin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.engine.spark.storage.ParquetStorage;
import org.apache.kylin.storage.ParquetDataStorage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *  This class use for improve java coverage with scala used, to be removed after scala coverage ready.
 */
public class SparderCoverTest extends NLocalFileMetadataTestCase {

    @Before
    public void init() {
        createTestMetadata();

    }

    @After
    public void clean() {
        cleanupTestMetadata();
    }

    @Test
    public void testParquetDataStorageCubingStorage() {
        ParquetDataStorage parquetDataStorage = new ParquetDataStorage();
        NSparkCubingEngine.NSparkCubingStorage nSparkCubingStorage = parquetDataStorage
                .adaptToBuildEngine(NSparkCubingEngine.NSparkCubingStorage.class);
        Assert.assertTrue(nSparkCubingStorage instanceof ParquetStorage);
    }
    //
    //    @Test
    //    public void testParquetDataStorage() {
    //        ParquetDataStorage parquetDataStorage = new ParquetDataStorage();
    //        NDataflow nDataflow = new NDataflow();
    //        IStorageQuery query = parquetDataStorage.createQuery(nDataflow);
    //        Assert.assertTrue(query instanceof NDataStorageQuery);
    //    }

    @Test
    public void testKapConf() {
        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        assert kapConfig.getListenerBusBusyThreshold() == 5000;
        assert kapConfig.getBlockNumBusyThreshold() == 5000;
    }

    @Test
    public void testHadoopUtil() throws IOException {
        FileSystem readFileSystem = HadoopUtil.getWorkingFileSystem();
        String scheme = readFileSystem.getScheme();
        assert scheme.equals("file");
        readFileSystem = HadoopUtil.getWorkingFileSystem(new Configuration());
        scheme = readFileSystem.getScheme();
        assert scheme.equals("file");
        readFileSystem = HadoopUtil.getWorkingFileSystem(new Configuration());
        scheme = readFileSystem.getScheme();
        assert scheme.equals("file");
        readFileSystem = HadoopUtil.getWorkingFileSystem();
        scheme = readFileSystem.getScheme();
        assert scheme.equals("file");
    }
}
