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
package org.apache.kylin.common.util;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.kylin.common.util.SegmentMergeStorageChecker.checkClusterStorageThresholdValue;
import static org.apache.kylin.common.util.SegmentMergeStorageChecker.checkMergeSegmentThreshold;
import static org.apache.kylin.common.util.SegmentMergeStorageChecker.getHadoopConfiguration;
import static org.apache.kylin.common.util.SegmentMergeStorageChecker.getSpaceQuotaPath;
import static org.apache.kylin.common.util.SegmentMergeStorageChecker.isThresholdAlarms;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SegmentMergeStorageCheckerTest extends NLocalFileMetadataTestCase {
    private static String pathName = "kylinTest" + randomAlphanumeric(8);
    private static String quotaPathName = "kylinQuotaTest" + randomAlphanumeric(8);

    @Before
    public void setUp() throws Exception {
        NLocalFileMetadataTestCase.staticCreateTestMetadata();
        overwriteSystemProp("kylin.hadoop.conf.dir", "../examples/test_case_data/sandbox");
    }

    @After
    public void after() throws Exception {
        staticCleanupTestMetadata();
    }

    private FileSystemTestHelper.MockFileSystem createMockFileSystem() throws IOException, URISyntaxException {
        FileSystemTestHelper.MockFileSystem mockFs = new FileSystemTestHelper.MockFileSystem();
        FileSystemTestHelper.MockFileSystem rawFileSystem = mockFs.getRawFileSystem();
        rawFileSystem.create(new Path(pathName));
        rawFileSystem.create(new Path(quotaPathName));
        when(mockFs.getUri()).thenReturn(new URI("hdfs://hdfstest:0"));
        when(rawFileSystem.getUri()).thenReturn(new URI("hdfs://hdfstest:0"));
        when(rawFileSystem.getContentSummary(new Path("/"))).thenReturn(new ContentSummary(0, 0, 0, -1, 0, -1));
        when(rawFileSystem.getContentSummary(new Path(pathName))).thenReturn(new ContentSummary(0, 0, 0, -1, 0, -1));
        when(rawFileSystem.getContentSummary(new Path(quotaPathName)))
                .thenReturn(new ContentSummary(0, 0, 0, -1, 0, 10 * 1024 * 1024));
        when(rawFileSystem.getStatus())
                .thenReturn(new FsStatus(500 * 1024 * 1024, 100 * 1024 * 1024, 400 * 1024 * 1024));
        when(rawFileSystem.exists(new Path(quotaPathName))).thenReturn(true);
        when(rawFileSystem.exists(new Path(pathName))).thenReturn(true);
        return mockFs;
    }

    @Test
    public void testGetSpaceQuotaPath() throws IOException, URISyntaxException {
        String workingDir = "/";
        FileSystem fileSystem = HadoopUtil.getFileSystem(workingDir);
        Path quotaPath = getSpaceQuotaPath(fileSystem, new Path(workingDir));
        Assert.assertNull(quotaPath);

        FileSystemTestHelper.MockFileSystem mockFs = createMockFileSystem();
        Configuration conf = HadoopUtil.getCurrentConfiguration();
        conf.set("fs.defaultFS", "hdfs://hdfstest:0");
        FileSystemTestHelper.MockFileSystem rawFileSystem = mockFs.getRawFileSystem();

        quotaPath = getSpaceQuotaPath(rawFileSystem, new Path(pathName));
        Assert.assertNull(quotaPath);

        quotaPath = getSpaceQuotaPath(rawFileSystem, new Path(quotaPathName));
        Assert.assertEquals(quotaPath.toUri().getPath(), quotaPathName);
    }

    @Test
    public void testHadoopSpaceInfo() throws IOException, URISyntaxException {
        Configuration conf = HadoopUtil.getCurrentConfiguration();
        FileSystem rootFileSystem = HadoopUtil.getFileSystem("/");
        SegmentMergeStorageChecker.setRootFileSystem(rootFileSystem);
        SegmentMergeStorageChecker.HadoopSpaceInfo hadoopSpaceInfo = SegmentMergeStorageChecker.HadoopSpaceInfo
                .getHadoopSpaceInfo(conf, "/");
        Assert.assertEquals(rootFileSystem.getStatus().getCapacity(), hadoopSpaceInfo.getTotalSpace());

        String workingDir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
        hadoopSpaceInfo = SegmentMergeStorageChecker.HadoopSpaceInfo.getHadoopSpaceInfo(conf, workingDir);
        Assert.assertEquals(rootFileSystem.getStatus().getCapacity(), hadoopSpaceInfo.getTotalSpace());

        FileSystemTestHelper.MockFileSystem mockFs = createMockFileSystem();
        conf.set("fs.defaultFS", "hdfs://hdfstest:0");
        FileSystemTestHelper.MockFileSystem rawFileSystem = mockFs.getRawFileSystem();
        SegmentMergeStorageChecker.setRootFileSystem(rawFileSystem);
        hadoopSpaceInfo = SegmentMergeStorageChecker.HadoopSpaceInfo.getHadoopSpaceInfo(conf, pathName);
        Assert.assertEquals(rawFileSystem.getStatus().getCapacity(), hadoopSpaceInfo.getTotalSpace());
        Assert.assertEquals(rawFileSystem.getStatus().getRemaining(), hadoopSpaceInfo.getRemainingSpace());
        Assert.assertEquals(rawFileSystem.getStatus().getUsed(), hadoopSpaceInfo.getUsedSpace());

        hadoopSpaceInfo = SegmentMergeStorageChecker.HadoopSpaceInfo.getHadoopSpaceInfo(conf, quotaPathName);
        Assert.assertEquals(10485760, hadoopSpaceInfo.getTotalSpace());
    }

    @Test
    public void testGetDfsReplication() {
        Configuration conf = HadoopUtil.getCurrentConfiguration();
        Assert.assertEquals(3, SegmentMergeStorageChecker.getDfsReplication("/", conf));
        conf.set("dfs.replication", "");
        HadoopUtil.setCurrentConfiguration(conf);
        Assert.assertEquals(3, SegmentMergeStorageChecker.getDfsReplication("/", conf));
    }

    @Test
    public void testCheckClusterStorageThresholdValue() throws IOException, URISyntaxException {
        Configuration conf = HadoopUtil.getCurrentConfiguration();
        FileSystemTestHelper.MockFileSystem mockFs = createMockFileSystem();
        conf.set("fs.defaultFS", "hdfs://hdfstest:0");
        FileSystemTestHelper.MockFileSystem rawFileSystem = mockFs.getRawFileSystem();
        SegmentMergeStorageChecker.setRootFileSystem(rawFileSystem);
        overwriteSystemProp("kylin.cube.merge-segment-storage-threshold", "0.75");
        overwriteSystemProp("kylin.env.hdfs-working-dir", quotaPathName);
        // expected space size is 2MB
        checkClusterStorageThresholdValue(quotaPathName, conf, 1024 * 2048L, 0.75, 3);
        Assert.assertTrue(true);
        // expected space size is 3MB
        checkClusterStorageThresholdValue(quotaPathName, conf, 1024 * 3072L, 0.75, 2);
        Assert.assertTrue(true);

        try {
            // expected space size is 3MB
            checkClusterStorageThresholdValue(quotaPathName, conf, 1024 * 3072L, 0.75, 3);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof KylinException);
            Assert.assertEquals(MsgPicker.getMsg().getSegmentMergeStorageCheckError(), ex.getMessage());
        }
    }

    @Test
    public void testGetThresholdConfig() {
        KylinConfig instanceFromEnv = KylinConfig.getInstanceFromEnv();
        Assert.assertTrue(instanceFromEnv.getMergeSegmentStorageThreshold() == 0d);
    }

    @Test
    public void testIsThresholdAlarms() {
        Assert.assertTrue(isThresholdAlarms(2000, 1000, 10000, 0.8));
        Assert.assertTrue(isThresholdAlarms(3000, 2000, 10000, 0.8));
        Assert.assertTrue(isThresholdAlarms(4000, 3000, 10000, 0.8));
        Assert.assertTrue(isThresholdAlarms(5000, 4000, 10000, 0.8));
        Assert.assertTrue(isThresholdAlarms(6000, 5000, 10000, 0.8));

        Assert.assertFalse(isThresholdAlarms(1000, 6000, 10000, 0.75));
        Assert.assertFalse(isThresholdAlarms(2000, 5000, 10000, 0.75));
        Assert.assertTrue(isThresholdAlarms(3000, 4000, 10000, 0.75));
        Assert.assertTrue(isThresholdAlarms(4000, 3000, 10000, 0.75));
        Assert.assertTrue(isThresholdAlarms(5000, 2000, 10000, 0.75));
    }

    @Test
    public void testRecountExpectedSpaceByte() throws IOException {
        Configuration conf = HadoopUtil.getCurrentConfiguration();
        long spaceByte = SegmentMergeStorageChecker.recountExpectedSpaceByte(1024, 3);
        System.out.println(spaceByte);
    }

    @Test
    public void testGetHadoopConfiguration() {
        KylinConfig kylinConfig = getTestConfig();
        Configuration hadoopConfiguration = getHadoopConfiguration(kylinConfig, "hdfs://test:8080/kylin");
        Assert.assertNotEquals("hdfs://test:8080/kylin", hadoopConfiguration.get("fs.defaultFS"));

        kylinConfig.setProperty("kylin.engine.submit-hadoop-conf-dir", "/write_hadoop_conf");
        hadoopConfiguration = getHadoopConfiguration(kylinConfig, "hdfs://test:8080/kylin");
        Assert.assertEquals("hdfs://test:8080/kylin", hadoopConfiguration.get("fs.defaultFS"));
    }

    @Test
    public void testCheckMergeSegmentThreshold() throws IOException, URISyntaxException {
        KylinConfig testConfig = getTestConfig();
        testConfig.setProperty("kylin.cube.merge-segment-storage-threshold", "0");
        checkMergeSegmentThreshold(testConfig, "/", 10 * 1024);
        testConfig.setProperty("kylin.cube.merge-segment-storage-threshold", "2");
        checkMergeSegmentThreshold(testConfig, "/", 10 * 1024);

        Configuration conf = HadoopUtil.getCurrentConfiguration();
        FileSystemTestHelper.MockFileSystem mockFs = createMockFileSystem();
        conf.set("fs.defaultFS", "hdfs://hdfstest:0");
        FileSystemTestHelper.MockFileSystem rawFileSystem = mockFs.getRawFileSystem();
        SegmentMergeStorageChecker.setRootFileSystem(rawFileSystem);
        testConfig.setProperty("kylin.cube.merge-segment-storage-threshold", "0.75");

        List<Long> mergeSegmentsSizeKB = new ArrayList<>();
        //512KB
        mergeSegmentsSizeKB.add(1024 * 512L);
        //1MB
        mergeSegmentsSizeKB.add(1024 * 1024L);
        //256KB
        mergeSegmentsSizeKB.add(1024 * 256L);

        long expectedSpaceByKB = mergeSegmentsSizeKB.stream().mapToLong(Long::longValue).sum();
        checkMergeSegmentThreshold(testConfig, pathName, expectedSpaceByKB);
        checkMergeSegmentThreshold(testConfig, quotaPathName, expectedSpaceByKB);

        //1MB
        mergeSegmentsSizeKB.add(1024 * 1024L);
        expectedSpaceByKB = mergeSegmentsSizeKB.stream().mapToLong(Long::longValue).sum();
        checkMergeSegmentThreshold(testConfig, pathName, expectedSpaceByKB);
        try {
            checkMergeSegmentThreshold(testConfig, quotaPathName, expectedSpaceByKB);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof KylinException);
            Assert.assertEquals(MsgPicker.getMsg().getSegmentMergeStorageCheckError(), ex.getMessage());
        }
    }
}
