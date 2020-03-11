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

import java.io.File;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;

import org.apache.kylin.shaded.com.google.common.io.Files;

public class LZ4CompressorTest {
    public static void main(String[] args) throws Exception {

        if(args.length == 0){
            System.out.println("args[0] must be data file path");
            return;
        }
        LZ4Factory factory = LZ4Factory.fastestInstance();

        byte[] data = Files.toByteArray(new File(args[0]));
        final int decompressedLength = data.length;

        // compress data
        LZ4Compressor compressor = factory.fastCompressor();
        long start = System.currentTimeMillis();
        int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
        byte[] compressed = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(data, 0, decompressedLength, compressed, 0, maxCompressedLength);
        System.out.println("compress take:" + (System.currentTimeMillis() - start));
        System.out.println(compressedLength);

        // decompress data
        // - method 1: when the decompressed length is known
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        start = System.currentTimeMillis();
        byte[] restored = new byte[decompressedLength];
        int compressedLength2 = decompressor.decompress(compressed, 0, restored, 0, decompressedLength);
        System.out.println("decompress take:" + (System.currentTimeMillis() - start));
        System.out.println(decompressedLength);
        // compressedLength == compressedLength2

        // - method 2: when the compressed length is known (a little slower)
        // the destination buffer needs to be over-sized
        LZ4SafeDecompressor decompressor2 = factory.safeDecompressor();
        int decompressedLength2 = decompressor2.decompress(compressed, 0, compressedLength, restored, 0);
    }
}
