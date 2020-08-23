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

import org.apache.kylin.shaded.com.google.common.io.Files;

public class NoCompressColumnTest {
    private File tmpColFile;

    @Before
    public void setUp() throws Exception {
        this.tmpColFile = File.createTempFile("testCol", ".nocompress");
        tmpColFile.deleteOnExit();
    }

    @Test
    public void testWriteRead() throws Exception {
        int rowCnt = 10000;
        writeNoCompressedData(rowCnt);

        ByteBuffer byteBuffer = Files.map(tmpColFile, MapMode.READ_ONLY);
        try (NoCompressedColumnReader reader = new NoCompressedColumnReader(byteBuffer, 0, 4, rowCnt)) {
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
    public void testFSInputRead() throws Exception {
        int rowCnt = 10000;
        writeNoCompressedData(rowCnt);

        FileSystem fs = FileSystem.getLocal(new Configuration());
        try (FSInputNoCompressedColumnReader reader = new FSInputNoCompressedColumnReader(fs,
                new Path(tmpColFile.getAbsolutePath()), 0, 4, rowCnt)) {
            int k = 0;
            for (byte[] val : reader) {
                assertEquals(k, Bytes.toInt(val));
                k++;
            }
            assertEquals(k, rowCnt);
        }
    }

    public void writeNoCompressedData(int rowCnt) throws IOException {
        NoCompressedColumnWriter writer = new NoCompressedColumnWriter(new FileOutputStream(tmpColFile));
        int[] colValues = new int[rowCnt];
        for (int i = 0; i < rowCnt; i++) {
            colValues[i] = i;
        }
        for (int i = 0; i < rowCnt; i++) {
            writer.write(Bytes.toBytes(colValues[i]));
        }
        writer.flush();
    }
}
