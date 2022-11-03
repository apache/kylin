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
package io.kyligence.kap.cache.fs;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.AbstractTestCase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import io.kyligence.kap.cache.kylin.OnlyForTestCacheFileSystem;

public class KylinCacheFileSystemTest extends AbstractTestCase {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    public static FileStatus f = null;

    @Test
    public void testInitKylinCacheFileSystemWithoutLocalCache() throws Exception {
        Configuration conf = initAndResetConf();
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_CACHE, "false");
        OnlyForTestCacheFileSystem fs = new OnlyForTestCacheFileSystem();
        fs.initialize(URI.create("file:/"), conf);
        FileStatus f = createSingleFile("a");
        fs.createNewFile(f.getPath());
        FSDataInputStream stream = fs.open(f.getPath());
        Assert.assertFalse(stream.getWrappedStream() instanceof AlluxioHdfsFileInputStream);
        Assert.assertFalse(stream.getWrappedStream() instanceof CacheFileInputStream);
    }

    @Test
    public void testInitKylinCacheFileSystemWithLocalCacheAndUseBufferFile() throws Exception {
        Configuration conf = initAndResetConf();
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_CACHE, "true");
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_LEGACY_FILE_INPUTSTREAM, "false");
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_BUFFER_FILE_INPUTSTREAM, "true");
        OnlyForTestCacheFileSystem fs = new OnlyForTestCacheFileSystem();
        fs.initialize(URI.create("file:/"), conf);
        FileStatus f = createSingleFile("b");
        fs.createNewFile(f.getPath());
        FSDataInputStream stream = fs.open(f.getPath());
        Assert.assertTrue(stream.getWrappedStream() instanceof CacheFileInputStream);
    }

    @Test
    public void testInitKylinCacheFileSystemWithLocalCacheWithUseLegacyFile() throws Exception {
        Configuration conf = initAndResetConf();
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_CACHE, "true");
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_LEGACY_FILE_INPUTSTREAM, "true");
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_BUFFER_FILE_INPUTSTREAM, "false");
        OnlyForTestCacheFileSystem fs = new OnlyForTestCacheFileSystem();
        fs.initialize(URI.create("file:/"), conf);
        FileStatus f = createSingleFile("c");
        fs.createNewFile(f.getPath());
        FSDataInputStream stream = fs.open(f.getPath());
        Assert.assertTrue(stream.getWrappedStream() instanceof AlluxioHdfsFileInputStream);
    }

    @Test
    public void testFileStatusWithoutCache() throws IOException, ExecutionException {
        Configuration conf = initAndResetConf();
        OnlyForTestCacheFileSystem fs = new OnlyForTestCacheFileSystem();
        fs.initialize(URI.create("file:/"), conf);
        FileStatus f = createSingleFile("d");
        fs.createNewFile(f.getPath());
        Assert.assertEquals(0, fs.fileStatusCache.size());
    }

    @Test
    public void testFileStatusWithCache() throws IOException, ExecutionException {
        Configuration conf = initAndResetConf();
        conf.set(CacheFileSystemConstants.PARAMS_KEY_USE_FILE_STATUS_CACHE, "true");
        OnlyForTestCacheFileSystem fs = new OnlyForTestCacheFileSystem();
        fs.initialize(URI.create("file:/"), conf);
        FileStatus f = createSingleFile("d");
        fs.createNewFile(f.getPath());
        fs.getFileStatus(f.getPath());
        Assert.assertEquals(1, fs.fileStatusCache.size());
    }

    @Test
    public void testFileStatusCacheWithFileNotFoundException() throws IOException, ExecutionException {
        exceptionRule.expect(ExecutionException.class);
        Configuration conf = initAndResetConf();
        OnlyForTestCacheFileSystem fs = new OnlyForTestCacheFileSystem();
        LocalFileSystem lfs = new LocalFileSystem();
        lfs.initialize(URI.create("file:/"), new Configuration());
        fs.initialize(URI.create("file:/"), conf);
        FileStatus f = createSingleFile("d");
        lfs.createFile(f.getPath());
        fs.fileStatusCache.get(f.getPath());
    }

    private Configuration initAndResetConf() {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", OnlyForTestCacheFileSystem.class.getName());
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf.setBoolean("fs.file.impl.disable.cache", true);
        return conf;
    }

    private FileStatus createSingleFile(String fileName) {
        return new FileStatus(1, false, 5, 3, 4, 5, null, "", "", new Path(folder.getRoot().getPath() + fileName));
    }
}
