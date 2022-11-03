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

import static io.kyligence.kap.cache.fs.FileInputStreamTestHelper.PAGE_SIZE;
import static io.kyligence.kap.cache.fs.FileInputStreamTestHelper.sConf;
import static io.kyligence.kap.cache.fs.FileInputStreamTestHelper.setupWithSingleFile;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.io.ByteStreams;

import alluxio.AlluxioURI;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.LocalCacheFileInStream;
import alluxio.grpc.OpenFilePOptions;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

public class AlluxioHdfsFileInputStreamTest {

    @Test
    public void readFullPage() throws Exception {
        int fileSize = PAGE_SIZE;
        int bufferSize = fileSize;
        int pages = 1;
        verifyReadFullFile(fileSize, bufferSize, pages);
    }

    @Test
    public void readFullPageThroughReadByteBufferMethod() throws Exception {
        int fileSize = PAGE_SIZE;
        int bufferSize = fileSize;
        int pages = 1;
        verifyReadFullFileThroughReadByteBufferMethod(fileSize, bufferSize, pages);
    }

    @Test
    public void readSmallPage() throws Exception {
        int fileSize = PAGE_SIZE / 5;
        int bufferSize = fileSize;
        int pages = 1;
        verifyReadFullFile(fileSize, bufferSize, pages);
    }

    @Test
    public void readSmallPageThroughReadByteBufferMethod() throws Exception {
        int fileSize = PAGE_SIZE / 5;
        int bufferSize = fileSize;
        int pages = 1;
        verifyReadFullFileThroughReadByteBufferMethod(fileSize, bufferSize, pages);
    }

    @Test
    public void readEmptyFileThroughReadByteBuffer() throws Exception {
        int fileSize = 0;
        byte[] fileData = BufferUtils.getIncreasingByteArray(fileSize);
        FileInputStreamTestHelper.ByteArrayCacheManager manager = new FileInputStreamTestHelper.ByteArrayCacheManager();
        AlluxioHdfsFileInputStream stream = wrapAlluxioSteam(setupWithSingleFile(fileData, manager));

        byte[] readData = new byte[fileSize];
        ByteBuffer buffer = ByteBuffer.wrap(readData);
        int totalBytesRead = stream.read(buffer, 0, fileSize + 1);
        Assert.assertEquals(-1, totalBytesRead);
    }

    @Test
    public void readPartialPage() throws Exception {
        int fileSize = PAGE_SIZE;
        byte[] testData = BufferUtils.getIncreasingByteArray(fileSize);
        FileInputStreamTestHelper.ByteArrayCacheManager manager = new FileInputStreamTestHelper.ByteArrayCacheManager();
        AlluxioHdfsFileInputStream stream = wrapAlluxioSteam(setupWithSingleFile(testData, manager));

        int partialReadSize = fileSize / 5;
        int offset = fileSize / 5;

        // cache miss
        byte[] cacheMiss = new byte[partialReadSize];
        stream.seek(offset);
        Assert.assertEquals(partialReadSize, stream.read(cacheMiss));
        Assert.assertArrayEquals(Arrays.copyOfRange(testData, offset, offset + partialReadSize), cacheMiss);
        Assert.assertEquals(0, manager.mPagesServed);
        Assert.assertEquals(1, manager.mPagesCached);

        // cache hit
        byte[] cacheHit = new byte[partialReadSize];
        stream.seek(offset);
        Assert.assertEquals(partialReadSize, stream.read(cacheHit));
        Assert.assertArrayEquals(Arrays.copyOfRange(testData, offset, offset + partialReadSize), cacheHit);
        Assert.assertEquals(1, manager.mPagesServed);
    }

    @Test
    public void readPartialPageThroughReadByteBufferMethod() throws Exception {
        int fileSize = PAGE_SIZE;
        byte[] testData = BufferUtils.getIncreasingByteArray(fileSize);
        FileInputStreamTestHelper.ByteArrayCacheManager manager = new FileInputStreamTestHelper.ByteArrayCacheManager();
        AlluxioHdfsFileInputStream stream = wrapAlluxioSteam(setupWithSingleFile(testData, manager));

        int partialReadSize = fileSize / 5;
        int offset = fileSize / 5;

        // cache miss
        ByteBuffer cacheMissBuffer = ByteBuffer.wrap(new byte[partialReadSize]);
        stream.seek(offset);
        Assert.assertEquals(partialReadSize, stream.read(cacheMissBuffer));
        Assert.assertArrayEquals(Arrays.copyOfRange(testData, offset, offset + partialReadSize),
                cacheMissBuffer.array());
        Assert.assertEquals(0, manager.mPagesServed);
        Assert.assertEquals(1, manager.mPagesCached);

        // cache hit
        ByteBuffer cacheHitBuffer = ByteBuffer.wrap(new byte[partialReadSize]);
        stream.seek(offset);
        Assert.assertEquals(partialReadSize, stream.read(cacheHitBuffer));
        Assert.assertArrayEquals(Arrays.copyOfRange(testData, offset, offset + partialReadSize),
                cacheHitBuffer.array());
        Assert.assertEquals(1, manager.mPagesServed);
    }

    @Test
    public void readMultiPage() throws Exception {
        int pages = 2;
        int fileSize = PAGE_SIZE + 10;
        int bufferSize = fileSize;
        verifyReadFullFile(fileSize, bufferSize, pages);
    }

    @Test
    public void readMultiPageThroughReadByteBufferMethod() throws Exception {
        int pages = 2;
        int fileSize = PAGE_SIZE + 10;
        int bufferSize = fileSize;
        verifyReadFullFileThroughReadByteBufferMethod(fileSize, bufferSize, pages);
    }

    @Test
    public void readMultiPageMixed() throws Exception {
        int pages = 10;
        int fileSize = PAGE_SIZE * pages;
        byte[] testData = BufferUtils.getIncreasingByteArray(fileSize);
        FileInputStreamTestHelper.ByteArrayCacheManager manager = new FileInputStreamTestHelper.ByteArrayCacheManager();
        AlluxioHdfsFileInputStream stream = wrapAlluxioSteam(setupWithSingleFile(testData, manager));

        // populate cache
        int pagesCached = 0;
        for (int i = 0; i < pages; i++) {
            stream.seek(PAGE_SIZE * i);
            if (ThreadLocalRandom.current().nextBoolean()) {
                Assert.assertEquals(testData[(i * PAGE_SIZE)], stream.read());
                pagesCached++;
            }
        }

        Assert.assertEquals(0, manager.mPagesServed);
        Assert.assertEquals(pagesCached, manager.mPagesCached);

        // sequential read
        stream.seek(0);
        byte[] fullRead = new byte[fileSize];
        Assert.assertEquals(fileSize, stream.read(fullRead));
        Assert.assertArrayEquals(testData, fullRead);
        Assert.assertEquals(pagesCached, manager.mPagesServed);
    }

    @Test
    public void readMultiPageMixedThroughReadByteBufferMethod() throws Exception {
        int pages = 10;
        int fileSize = PAGE_SIZE * pages;
        byte[] testData = BufferUtils.getIncreasingByteArray(fileSize);
        FileInputStreamTestHelper.ByteArrayCacheManager manager = new FileInputStreamTestHelper.ByteArrayCacheManager();
        AlluxioHdfsFileInputStream stream = wrapAlluxioSteam(setupWithSingleFile(testData, manager));

        // populate cache
        int pagesCached = 0;
        for (int i = 0; i < pages; i++) {
            stream.seek(PAGE_SIZE * i);
            if (ThreadLocalRandom.current().nextBoolean()) {
                Assert.assertEquals(testData[(i * PAGE_SIZE)], stream.read());
                pagesCached++;
            }
        }

        Assert.assertEquals(0, manager.mPagesServed);
        Assert.assertEquals(pagesCached, manager.mPagesCached);

        // sequential read
        stream.seek(0);
        ByteBuffer fullReadBuf = ByteBuffer.wrap(new byte[fileSize]);
        Assert.assertEquals(fileSize, stream.read(fullReadBuf));
        Assert.assertArrayEquals(testData, fullReadBuf.array());
        Assert.assertEquals(pagesCached, manager.mPagesServed);
    }

    @Test
    public void readOversizedBuffer() throws Exception {
        int pages = 1;
        int fileSize = PAGE_SIZE;
        int bufferSize = fileSize * 2;
        verifyReadFullFile(fileSize, bufferSize, pages);
    }

    @Test
    public void readOversizedBufferThroughReadByteBufferMethod() throws Exception {
        int pages = 1;
        int fileSize = PAGE_SIZE;
        int bufferSize = fileSize * 2;
        verifyReadFullFileThroughReadByteBufferMethod(fileSize, bufferSize, pages);
    }

    @Test
    public void readSmallPageOversizedBuffer() throws Exception {
        int pages = 1;
        int fileSize = PAGE_SIZE / 3;
        int bufferSize = fileSize * 2;
        verifyReadFullFile(fileSize, bufferSize, pages);
    }

    @Test
    public void readSmallPageOversizedBufferThroughReadByteBufferMethod() throws Exception {
        int pages = 1;
        int fileSize = PAGE_SIZE / 3;
        int bufferSize = fileSize * 2;
        verifyReadFullFileThroughReadByteBufferMethod(fileSize, bufferSize, pages);
    }

    @Test
    public void externalStoreMultiRead() throws Exception {
        int fileSize = PAGE_SIZE;
        byte[] testData = BufferUtils.getIncreasingByteArray(fileSize);
        FileInputStreamTestHelper.ByteArrayCacheManager manager = new FileInputStreamTestHelper.ByteArrayCacheManager();
        Map<AlluxioURI, byte[]> files = new HashMap<>();
        AlluxioURI testFilename = new AlluxioURI("/test");
        files.put(testFilename, testData);

        FileInputStreamTestHelper.ByteArrayFileSystem fs = new FileInputStreamTestHelper.MultiReadByteArrayFileSystem(
                files);

        AlluxioHdfsFileInputStream stream = wrapAlluxioSteam(new LocalCacheFileInStream(fs.getStatus(testFilename),
                (status) -> fs.openFile(status, OpenFilePOptions.getDefaultInstance()), manager, sConf));

        // cache miss
        byte[] cacheMiss = new byte[fileSize];
        Assert.assertEquals(fileSize, stream.read(cacheMiss));
        Assert.assertArrayEquals(testData, cacheMiss);
        Assert.assertEquals(0, manager.mPagesServed);
        Assert.assertEquals(1, manager.mPagesCached);

        // cache hit
        stream.seek(0);
        byte[] cacheHit = new byte[fileSize];
        Assert.assertEquals(fileSize, stream.read(cacheHit));
        Assert.assertArrayEquals(testData, cacheHit);
        Assert.assertEquals(1, manager.mPagesServed);
    }

    @Test
    public void externalStoreMultiReadThroughReadByteBufferMethod() throws Exception {
        int fileSize = PAGE_SIZE;
        byte[] testData = BufferUtils.getIncreasingByteArray(fileSize);
        FileInputStreamTestHelper.ByteArrayCacheManager manager = new FileInputStreamTestHelper.ByteArrayCacheManager();
        Map<AlluxioURI, byte[]> files = new HashMap<>();
        AlluxioURI testFilename = new AlluxioURI("/test");
        files.put(testFilename, testData);

        FileInputStreamTestHelper.ByteArrayFileSystem fs = new FileInputStreamTestHelper.MultiReadByteArrayFileSystem(
                files);

        AlluxioHdfsFileInputStream stream = wrapAlluxioSteam(new LocalCacheFileInStream(fs.getStatus(testFilename),
                (status) -> fs.openFile(status, OpenFilePOptions.getDefaultInstance()), manager, sConf));

        // cache miss
        ByteBuffer cacheMissBuf = ByteBuffer.wrap(new byte[fileSize]);
        Assert.assertEquals(fileSize, stream.read(cacheMissBuf));
        Assert.assertArrayEquals(testData, cacheMissBuf.array());
        Assert.assertEquals(0, manager.mPagesServed);
        Assert.assertEquals(1, manager.mPagesCached);

        // cache hit
        stream.seek(0);
        ByteBuffer cacheHitBuf = ByteBuffer.wrap(new byte[fileSize]);
        Assert.assertEquals(fileSize, stream.read(cacheHitBuf));
        Assert.assertArrayEquals(testData, cacheHitBuf.array());
        Assert.assertEquals(1, manager.mPagesServed);
    }

    @Test
    public void readMultipleFiles() throws Exception {
        Random random = new Random();
        FileInputStreamTestHelper.ByteArrayCacheManager manager = new FileInputStreamTestHelper.ByteArrayCacheManager();
        Map<String, byte[]> files = IntStream.range(0, 10)
                .mapToObj(i -> BufferUtils.getIncreasingByteArray(i, random.nextInt(100)))
                .collect(Collectors.toMap(d -> PathUtils.uniqPath(), Function.identity()));
        Map<AlluxioURI, AlluxioHdfsFileInputStream> streams = setupWithMultipleFiles(files, manager);
        for (AlluxioURI path : streams.keySet()) {
            try (InputStream stream = streams.get(path)) {
                Assert.assertArrayEquals(files.get(path.toString()), ByteStreams.toByteArray(stream));
            }
        }
    }

    private void verifyReadFullFile(int fileSize, int bufferSize, int pages) throws Exception {
        byte[] testData = BufferUtils.getIncreasingByteArray(fileSize);
        FileInputStreamTestHelper.ByteArrayCacheManager manager = new FileInputStreamTestHelper.ByteArrayCacheManager();
        AlluxioHdfsFileInputStream stream = wrapAlluxioSteam(setupWithSingleFile(testData, manager));

        // cache miss
        byte[] cacheMiss = new byte[bufferSize];
        Assert.assertEquals(fileSize, stream.read(cacheMiss));
        Assert.assertArrayEquals(testData, Arrays.copyOfRange(cacheMiss, 0, fileSize));
        Assert.assertEquals(0, manager.mPagesServed);
        Assert.assertEquals(pages, manager.mPagesCached);

        // cache hit
        stream.seek(0);
        byte[] cacheHit = new byte[bufferSize];
        Assert.assertEquals(fileSize, stream.read(cacheHit));
        Assert.assertArrayEquals(testData, Arrays.copyOfRange(cacheHit, 0, fileSize));
        Assert.assertEquals(pages, manager.mPagesServed);
    }

    private void verifyReadFullFileThroughReadByteBufferMethod(int fileSize, int bufferSize, int pages)
            throws Exception {
        byte[] testData = BufferUtils.getIncreasingByteArray(fileSize);
        FileInputStreamTestHelper.ByteArrayCacheManager manager = new FileInputStreamTestHelper.ByteArrayCacheManager();
        AlluxioHdfsFileInputStream stream = wrapAlluxioSteam(setupWithSingleFile(testData, manager));

        // cache miss
        byte[] cacheMiss = new byte[bufferSize];
        ByteBuffer cacheMissBuffer = ByteBuffer.wrap(cacheMiss);
        Assert.assertEquals(fileSize, stream.read(cacheMissBuffer));
        Assert.assertArrayEquals(testData, Arrays.copyOfRange(cacheMiss, 0, fileSize));
        Assert.assertEquals(0, manager.mPagesServed);
        Assert.assertEquals(pages, manager.mPagesCached);

        // cache hit
        stream.seek(0);
        byte[] cacheHit = new byte[bufferSize];
        ByteBuffer cacheHitBuffer = ByteBuffer.wrap(cacheHit);
        Assert.assertEquals(fileSize, stream.read(cacheHitBuffer));
        Assert.assertArrayEquals(testData, Arrays.copyOfRange(cacheHit, 0, fileSize));
        Assert.assertEquals(pages, manager.mPagesServed);
    }

    private AlluxioHdfsFileInputStream wrapAlluxioSteam(LocalCacheFileInStream stream) {
        return new AlluxioHdfsFileInputStream(stream, null);
    }

    private Map<AlluxioURI, AlluxioHdfsFileInputStream> setupWithMultipleFiles(Map<String, byte[]> files,
            CacheManager manager) {
        Map<AlluxioURI, byte[]> fileMap = files.entrySet().stream()
                .collect(Collectors.toMap(entry -> new AlluxioURI(entry.getKey()), Map.Entry::getValue));
        final FileInputStreamTestHelper.ByteArrayFileSystem fs = new FileInputStreamTestHelper.ByteArrayFileSystem(
                fileMap);

        Map<AlluxioURI, AlluxioHdfsFileInputStream> ret = new HashMap<>();
        fileMap.forEach((key, value) -> {
            try {
                ret.put(key, wrapAlluxioSteam(new LocalCacheFileInStream(fs.getStatus(key),
                        (status) -> fs.openFile(status, OpenFilePOptions.getDefaultInstance()), manager, sConf)));
            } catch (Exception e) {
                // skip
            }
        });
        return ret;
    }
}
