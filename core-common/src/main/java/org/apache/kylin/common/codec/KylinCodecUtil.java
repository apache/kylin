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
package org.apache.kylin.common.codec;

import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * KylinCodecUtil
 * created by haihuang.hhl @2020/1/9
 */
public class KylinCodecUtil {
    public static byte[] compress(byte[] uncompressed, String codecName) throws Exception {
        ByteArrayOutputStream compressBos = new ByteArrayOutputStream();
        try (OutputStream out = KylinCodecFactory.createCodec(codecName).compressedOutputStream(compressBos)) {
            out.write(uncompressed);
        }
        return compressBos.toByteArray();
    }

    public static byte[] decompress(byte[] compressed, String codecName) throws Exception {
        ByteArrayInputStream deCompressBis = new ByteArrayInputStream(compressed);
        try (InputStream in = KylinCodecFactory.createCodec(codecName).compressedInputStream(deCompressBis)) {
            return IOUtils.toByteArray(in);
        }

    }
}