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

package org.apache.kylin.stream.core.storage.columnar;

import static org.junit.Assert.assertEquals;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.Random;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.io.CountingOutputStream;
import org.apache.kylin.shaded.com.google.common.io.Files;

public class GeneralColumnDataTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @Test
    public void testWriteRead() throws Exception {
        File tmpColFile = File.createTempFile("testCol", ".general");
        System.out.println("file path:" + tmpColFile.getAbsolutePath());
        tmpColFile.deleteOnExit();

        DataType dataType = DataType.getType("decimal");
        DataTypeSerializer serializer = DataTypeSerializer.create(dataType);
        CountingOutputStream countingOutputStream = new CountingOutputStream(new FileOutputStream(tmpColFile));

        int rowCnt = 1000;
        int maxLength = serializer.maxLength();
        ByteBuffer writeBuffer = ByteBuffer.allocate(maxLength);

        GeneralColumnDataWriter writer = new GeneralColumnDataWriter(rowCnt, new DataOutputStream(countingOutputStream));
        for (int i = 0; i < rowCnt; i++) {
            writeBuffer.rewind();
            serializer.serialize(new BigDecimal(i), writeBuffer);
            byte[] bytes = Arrays.copyOf(writeBuffer.array(), writeBuffer.position());
            writer.write(bytes);
        }
        writer.flush();
        ByteBuffer dataBuffer = Files.map(tmpColFile, MapMode.READ_ONLY);
        try (GeneralColumnDataReader reader = new GeneralColumnDataReader(dataBuffer, 0,
                (int) countingOutputStream.getCount())) {

            int k = 0;
            for (byte[] val : reader) {
                assertEquals(new BigDecimal(k), serializer.deserialize(ByteBuffer.wrap(val)));
                k++;
            }
            assertEquals(k, rowCnt);

            Random random = new Random();
            for (int i = 0; i < 50; i++) {
                int rowNum = random.nextInt(rowCnt);
                byte[] val = reader.read(rowNum);
                assertEquals(new BigDecimal(rowNum), serializer.deserialize(ByteBuffer.wrap(val)));
            }
        }
    }
}
