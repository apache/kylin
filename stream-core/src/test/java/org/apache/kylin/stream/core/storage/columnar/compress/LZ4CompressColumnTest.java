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

package org.apache.kylin.stream.core.storage.columnar.compress;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.io.CountingOutputStream;
import org.apache.kylin.shaded.com.google.common.io.Files;

public class LZ4CompressColumnTest {
    private File tmpColFile;

    @Before
    public void setUp() throws Exception {
        this.tmpColFile = File.createTempFile("testCol", ".lz4");
        tmpColFile.deleteOnExit();
    }

    @Test
    public void testWriteRead() throws Exception {
        System.out.println("file path:" + tmpColFile.getAbsolutePath());
        int rowCnt = 100000;
        int compressedSize = writeCompressedData(rowCnt);
        System.out.println("compressed data size:" + compressedSize);
        ByteBuffer byteBuffer = Files.map(tmpColFile, MapMode.READ_ONLY);
        try (LZ4CompressedColumnReader reader = new LZ4CompressedColumnReader(byteBuffer, 0, compressedSize, rowCnt)) {
            int k = 0;
            for (byte[] val : reader) {
                assertEquals(k, Bytes.toInt(val));
                k++;
            }
            assertEquals(k, rowCnt);
            Random random = new Random();
            for (int i = 0; i < 50; i++) {
                int rowNum = random.nextInt(rowCnt);
                byte[] val = reader.read(rowNum);
                assertEquals(rowNum, Bytes.toInt(val));
            }
        }
    }

    @Test
    public void testReadInputStream() throws Exception {
        System.out.println("file path:" + tmpColFile.getAbsolutePath());
        int rowCnt = 100000;
        int compressedSize = writeCompressedData(rowCnt);
        System.out.println("compressed data size:" + compressedSize);
        FileSystem fs = FileSystem.getLocal(new Configuration());
        try (FSInputLZ4CompressedColumnReader reader = new FSInputLZ4CompressedColumnReader(fs, new Path(tmpColFile.getAbsolutePath()), 0,
                compressedSize, rowCnt)) {
            int k = 0;
            for (byte[] val : reader) {
                assertEquals(k, Bytes.toInt(val));
                k++;
            }
            assertEquals(k, rowCnt);
        }
    }

    public int writeCompressedData(int rowCnt) throws IOException {
        int compressBolckSize = 64 * 1024;
        CountingOutputStream countingOutputStream = new CountingOutputStream(new FileOutputStream(tmpColFile));
        LZ4CompressedColumnWriter writer = new LZ4CompressedColumnWriter(4, rowCnt, compressBolckSize,
                countingOutputStream);
        int[] colValues = new int[rowCnt];
        for (int i = 0; i < rowCnt; i++) {
            colValues[i] = i;
        }
        for (int i = 0; i < rowCnt; i++) {
            writer.write(Bytes.toBytes(colValues[i]));
        }
        writer.flush();
        return (int) countingOutputStream.getCount();
    }
}
