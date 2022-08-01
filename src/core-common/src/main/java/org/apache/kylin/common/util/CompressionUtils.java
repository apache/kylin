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
import java.nio.charset.Charset;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.LoggerFactory;

import lombok.experimental.UtilityClass;

@UtilityClass
public class CompressionUtils {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CompressionUtils.class);
    private static final byte[] GZIP = "GZIP".getBytes(Charset.defaultCharset());

    public static byte[] compress(byte[] data) throws IOException {
        if (!KylinConfig.getInstanceFromEnv().isMetadataCompressEnabled() || data == null || data.length == 0
                || isCompressed(data)) {
            return data;
        }
        Deflater deflater = new Deflater(1);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length)) {
            long startTime = System.currentTimeMillis();
            deflater.setInput(data);
            deflater.finish();
            byte[] buffer = new byte[1024];
            while (!deflater.finished()) {
                int count = deflater.deflate(buffer); // returns the generated code... index
                outputStream.write(buffer, 0, count);
            }
            outputStream.flush();
            byte[] output = outputStream.toByteArray();

            logger.trace("Original: {} bytes. Compressed: {} bytes. Time: {}", data.length, output.length,
                    (System.currentTimeMillis() - startTime));
            return BytesUtil.mergeBytes(GZIP, output);
        } finally {
            deflater.end();
        }
    }

    public static byte[] decompress(byte[] data) throws IOException, DataFormatException {
        if (data == null || data.length == 0 || !isCompressed(data)) {
            return data;
        }
        data = BytesUtil.subarray(data, GZIP.length, data.length);
        Inflater inflater = new Inflater();
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length)) {
            long startTime = System.currentTimeMillis();
            inflater.setInput(data);
            byte[] buffer = new byte[1024];
            while (!inflater.finished()) {
                int count = inflater.inflate(buffer);
                outputStream.write(buffer, 0, count);
            }
            outputStream.flush();
            byte[] output = outputStream.toByteArray();

            logger.trace("Original: {} bytes. Decompressed: {} bytes. Time: {}", data.length, output.length,
                    (System.currentTimeMillis() - startTime));
            return output;
        } finally {
            inflater.end();
        }
    }

    public static boolean isCompressed(byte[] bytes) {
        boolean isWrapped = false;
        if (bytes.length > GZIP.length) {
            isWrapped = true;
            for (int i = 0; i < GZIP.length; i++) {
                if (bytes[i] != GZIP[i]) {
                    isWrapped = false;
                    break;
                }
            }
        }

        return isWrapped;
    }
}
