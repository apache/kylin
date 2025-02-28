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
package org.apache.kylin.cache.kylin;

import static org.apache.kylin.cache.kylin.KylinCacheFileSystem.extractAcceptCacheTime;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.cache.fs.AbstractCacheFileSystem;
import org.apache.kylin.cache.fs.AlluxioHdfsFileInputStream;
import org.apache.kylin.cache.fs.CacheFileInputStream;
import org.apache.kylin.cache.fs.CacheFileSystemConstants;
import org.apache.kylin.common.AbstractTestCase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.springframework.test.util.ReflectionTestUtils;

public class KylinCacheFileSystemTest extends AbstractTestCase {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @SuppressWarnings("deprecation")
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    public static FileStatus f = null;

    @Test
    public void testExtractAcceptCacheTimeInHintStr() {
        {
            String time = extractAcceptCacheTime("");
            Assert.assertNull(time);
        }
        {
            String time = extractAcceptCacheTime("ACCEPT_CACHE_TIME");
            Assert.assertNull(time);
        }
        {
            String time = extractAcceptCacheTime("select * from xxx /*+ ACCEPT_CACHE_TIME(158176387682000) */");
            Assert.assertEquals("158176387682000", time);
        }
        {
            String time = extractAcceptCacheTime("/*+ ACCEPT_CACHE_TIME(158176387682000) */\nselect * from xxx");
            Assert.assertEquals("158176387682000", time);
        }
        {
            String time = extractAcceptCacheTime(
                    "/*+ ACCEPT_CACHE_TIME(158176387682000, 2021-03-28T12:33:54) */\nselect * from xxx");
            Assert.assertEquals("158176387682000", time);
        }
        {
            String time = extractAcceptCacheTime(
                    "/*+ MODEL_PRIORITY(m1,m2), ABC, ACCEPT_CACHE_TIME(158176387682000, 2021-03-28T12:33:54), DEF, ACCEPT_CACHE_TIME(158176387682001) */");
            Assert.assertEquals("158176387682000", time);
        }
        // now 2023/2/27:  1677466066216
        // big enough:   158176387682000
        System.out.println("System.currentTimeMillis() -- " + System.currentTimeMillis());
    }

    @Test
    public void testInitKylinCacheFileSystemWithoutLocalCache() throws Exception {
        Configuration conf = initAndResetConf();
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_CACHE, "false");
        try (OnlyForTestCacheFileSystem fs = new OnlyForTestCacheFileSystem()) {
            fs.initialize(URI.create("file:/"), conf);
            FileStatus f = createSingleFile("a");
            fs.createNewFile(f.getPath());
            FSDataInputStream stream = fs.open(f.getPath());
            Assert.assertFalse(stream.getWrappedStream() instanceof AlluxioHdfsFileInputStream);
            Assert.assertFalse(stream.getWrappedStream() instanceof CacheFileInputStream);
        }
    }

    @Test
    public void testInitKylinCacheFileSystemWithLocalCacheAndUseBufferFile() throws Exception {
        Configuration conf = initAndResetConf();
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_CACHE, "true");
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_LEGACY_FILE_INPUTSTREAM, "false");
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_BUFFER_FILE_INPUTSTREAM, "true");
        try (OnlyForTestCacheFileSystem fs = new OnlyForTestCacheFileSystem()) {
            fs.initialize(URI.create("file:/"), conf);
            FileStatus f = createSingleFile("b");
            fs.createNewFile(f.getPath());
            FSDataInputStream stream = fs.open(f.getPath());
            Assert.assertTrue(stream.getWrappedStream() instanceof CacheFileInputStream);
        }
    }

    @Test
    public void testInitKylinCacheFileSystemWithLocalCacheWithUseLegacyFile() throws Exception {
        Configuration conf = initAndResetConf();
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_CACHE, "true");
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_LEGACY_FILE_INPUTSTREAM, "true");
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_BUFFER_FILE_INPUTSTREAM, "false");
        try (OnlyForTestCacheFileSystem fs = new OnlyForTestCacheFileSystem()) {
            fs.initialize(URI.create("file:/"), conf);
            FileStatus f = createSingleFile("c");
            fs.createNewFile(f.getPath());
            FSDataInputStream stream = fs.open(f.getPath());
            Assert.assertTrue(stream.getWrappedStream() instanceof AlluxioHdfsFileInputStream);
        }
    }

    @Test(expected = IOException.class)
    public void testInitKylinCacheFileSystemWithWrongScheme() throws Exception {
        Configuration conf = initAndResetConf();
        try (OnlyForTestCacheFileSystem fs = new OnlyForTestCacheFileSystem()) {
            fs.initialize(URI.create("noSuchScheme:/"), conf);
            Assert.fail();
        }
    }

    @SuppressWarnings("unchecked")
    @Test(expected = IOException.class)
    public void testInitKylinCacheFileSystemWithSchemaNoClassFound() throws Exception {
        Configuration conf = initAndResetConf();
        Map<String, String> schemeClassMap = (Map<String, String>) ReflectionTestUtils
                .getField(AbstractCacheFileSystem.class, "schemeClassMap");
        Assert.assertNotNull(schemeClassMap);
        schemeClassMap.put("noClassFoundScheme", "no.such.class.NoSuchClass");
        try (OnlyForTestCacheFileSystem fs = new OnlyForTestCacheFileSystem()) {
            fs.initialize(URI.create("noClassFoundScheme:/"), conf);
            Assert.fail();
        }
    }

    @Test
    public void testFileStatusWithoutCache() throws IOException {
        Configuration conf = initAndResetConf();
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_FILE_STATUS_CACHE, "true");
        try (OnlyForTestCacheFileSystem fs = new OnlyForTestCacheFileSystem()) {
            fs.initialize(URI.create("file:/"), conf);
            FileStatus f = createSingleFile("d");
            fs.createNewFile(f.getPath());
            Assert.assertEquals(0, fs.getStatusCache().size());
        }
    }

    @Test
    public void testFileStatusWithCache() throws IOException {
        Configuration conf = initAndResetConf();
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_FILE_STATUS_CACHE, "true");
        try (OnlyForTestCacheFileSystem fs = new OnlyForTestCacheFileSystem()) {
            fs.initialize(URI.create("file:/"), conf);
            FileStatus f = createSingleFile("d");
            fs.createNewFile(f.getPath());
            fs.getFileStatus(f.getPath());
            Assert.assertEquals(1, fs.getStatusCache().size());
        }
    }

    @Test
    public void testFileStatusCacheWithFileNotFoundException() throws IOException {
        exceptionRule.expect(FileNotFoundException.class);
        Configuration conf = initAndResetConf();
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_FILE_STATUS_CACHE, "true");
        try (OnlyForTestCacheFileSystem fs = new OnlyForTestCacheFileSystem();
                LocalFileSystem lfs = new LocalFileSystem()) {
            lfs.initialize(URI.create("file:/"), new Configuration());
            fs.initialize(URI.create("file:/"), conf);
            FileStatus f = createSingleFile("d");
            lfs.createFile(f.getPath());
            fs.getStatusCache().getFileStatus(f.getPath());
        }
    }

    private Configuration initAndResetConf() {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", OnlyForTestCacheFileSystem.class.getName());
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf.setBoolean("fs.file.impl.disable.cache", true);
        return conf;
    }

    private FileStatus createSingleFile(String fileName) {
        Path path = new Path(folder.getRoot().getPath() + fileName);
        return new FileStatus(1, false, 5, 3, 4, 5, null, "", "", path);
    }
}
