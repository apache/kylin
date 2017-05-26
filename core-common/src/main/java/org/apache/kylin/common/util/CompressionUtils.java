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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.slf4j.LoggerFactory;

/**
 */
public class CompressionUtils {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CompressionUtils.class);

    public static byte[] compress(byte[] data) throws IOException {
        long startTime = System.currentTimeMillis();
        Deflater deflater = new Deflater(1);
        deflater.setInput(data);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);

        deflater.finish();
        byte[] buffer = new byte[1024];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer); // returns the generated code... index
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        byte[] output = outputStream.toByteArray();

        logger.debug("Original: " + data.length + " bytes. " + "Compressed: " + output.length + " byte. Time: " + (System.currentTimeMillis() - startTime));
        return output;
    }

    public static byte[] decompress(byte[] data) throws IOException, DataFormatException {
        long startTime = System.currentTimeMillis();
        Inflater inflater = new Inflater();
        inflater.setInput(data);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
        byte[] buffer = new byte[1024];
        while (!inflater.finished()) {
            int count = inflater.inflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        byte[] output = outputStream.toByteArray();

        logger.debug("Original: " + data.length + " bytes. " + "Decompressed: " + output.length + " bytes. Time: " + (System.currentTimeMillis() - startTime));
        return output;
    }
}
