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

public class RunLengthCompressColumnTest {
    private File tmpColFile;

    @Before
    public void setUp() throws Exception {
        this.tmpColFile = File.createTempFile("testCol", ".rle");
        tmpColFile.deleteOnExit();
    }

    @Test
    public void testWriteRead() throws Exception {
        System.out.println("file path:" + tmpColFile.getAbsolutePath());
        int rowCnt = 100000;

        int size = writeCompressData1(rowCnt);
        System.out.println("compressed data size:" + size);
        ByteBuffer byteBuffer = Files.map(tmpColFile, MapMode.READ_ONLY);
        try (RunLengthCompressedColumnReader reader = new RunLengthCompressedColumnReader(byteBuffer, 0, size, rowCnt)) {
            int k = 0;
            for (byte[] val : reader) {
                assertEquals(k, Bytes.toInt(val));
                k++;
            }
            assertEquals(k, rowCnt);

            reader.reset();
            byte[] val = reader.read(10);
            assertEquals(10, Bytes.toInt(val));

            val = reader.read(16384);
            assertEquals(16384, Bytes.toInt(val));

            val = reader.read(99999);
            assertEquals(99999, Bytes.toInt(val));

            Random random = new Random();
            for (int i = 0; i < 50; i++) {
                int rowNum = random.nextInt(rowCnt);
                val = reader.read(rowNum);
                assertEquals(rowNum, Bytes.toInt(val));
            }
        }
    }

    @Test
    public void testReadInputStream() throws Exception {
        System.out.println("file path:" + tmpColFile.getAbsolutePath());
        int rowCnt = 100000;

        int size = writeCompressData1(rowCnt);
        FileSystem fs = FileSystem.getLocal(new Configuration());
        try (FSInputRLECompressedColumnReader reader = new FSInputRLECompressedColumnReader(fs, new Path(tmpColFile.getAbsolutePath()), 0, size,
                rowCnt)) {
            int k = 0;
            for (byte[] val : reader) {
                assertEquals(k, Bytes.toInt(val));
                k++;
            }
            assertEquals(k, rowCnt);
        }
    }

    public int writeCompressData1(int rowCnt) throws IOException {
        int compressBolckSize = 64 * 1024;
        CountingOutputStream countingOutputStream = new CountingOutputStream(new FileOutputStream(tmpColFile));
        RunLengthCompressedColumnWriter writer = new RunLengthCompressedColumnWriter(4, rowCnt, compressBolckSize,
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

    @Test
    public void testWriteRead2() throws Exception {
        File tmpColFile = File.createTempFile("testCol", ".rle");
        System.out.println("file path:" + tmpColFile.getAbsolutePath());
        tmpColFile.deleteOnExit();
        int rowCnt = 100000;
        int compressBolckSize = 64 * 1024;
        CountingOutputStream countingOutputStream = new CountingOutputStream(new FileOutputStream(tmpColFile));
        RunLengthCompressedColumnWriter writer = new RunLengthCompressedColumnWriter(4, rowCnt, compressBolckSize,
                countingOutputStream);
        int batchCnt = 10;
        int batch = rowCnt / batchCnt;
        for (int i = 0; i < batchCnt; i++) {
            for (int j = 0; j < batch; j++) {
                writer.write(Bytes.toBytes(i));
            }
        }

        writer.flush();
        int size = (int) countingOutputStream.getCount();
        System.out.println("compressed data size:" + size);
        ByteBuffer byteBuffer = Files.map(tmpColFile, MapMode.READ_ONLY);
        try (RunLengthCompressedColumnReader reader = new RunLengthCompressedColumnReader(byteBuffer, 0, size, rowCnt)) {
            int k = 0;
            for (byte[] val : reader) {
                assertEquals(k / batch, Bytes.toInt(val));
                k++;
            }
            assertEquals(k, rowCnt);

            reader.reset();
            byte[] val = reader.read(10);
            assertEquals(10 / batch, Bytes.toInt(val));

            val = reader.read(16384);
            assertEquals(16384 / batch, Bytes.toInt(val));

            val = reader.read(99999);
            assertEquals(99999 / batch, Bytes.toInt(val));

            Random random = new Random();
            for (int i = 0; i < 50; i++) {
                int rowNum = random.nextInt(rowCnt);
                val = reader.read(rowNum);
                assertEquals(rowNum / batch, Bytes.toInt(val));
            }
        }
    }
}
