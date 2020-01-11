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

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;

/**
 * CompressionCodecFactory
 * created by haihuang.hhl @2019/9/25
 */
public class KylinCodecFactory {

    public enum KylinCodecType {
        lz4, zstd
    }

    public static CompressionCodec createCodec(String codecName) throws IllegalArgumentException {
        KylinCodecType codecType = KylinCodecType.valueOf(codecName);
        switch (codecType) {
            case zstd:
                return new ZStdCompressionCodec();
            case lz4:
                return new LZ4CompressionCodec();
            default:
                throw new IllegalArgumentException("codec:" + codecName + " not support");
        }

    }
}


class LZ4CompressionCodec implements CompressionCodec {

    //refer to spark default 32K
    private int blockSize = 32 * 1024;
    // refer to spark:default true
    private final int defaultSeed = 0x9747b28c;// LZ4BlockOutputStream.DEFAULT_SEED

    public LZ4CompressionCodec() {
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public LZ4CompressionCodec(int blockSize, boolean disableConcatenationOfByteStream) {
        this.blockSize = blockSize;
    }

    @Override
    public OutputStream compressedOutputStream(OutputStream outputStream) {
        return new LZ4BlockOutputStream(outputStream, this.blockSize);
    }

    @Override
    public InputStream compressedInputStream(InputStream inputStream) {
        return new LZ4BlockInputStream(inputStream, LZ4Factory.fastestInstance().fastDecompressor(),
                XXHashFactory.fastestInstance().newStreamingHash32(defaultSeed).asChecksum());
    }

    @Override
    public String getCompressionCodecName() {
        return KylinCodecFactory.KylinCodecType.lz4.name();
    }
}

class ZStdCompressionCodec implements CompressionCodec {


    //refer to spark default 32K
    private int bufferSize = 32 * 1024;
    // Default compression level for zstd compression to 1 because it is
    // fastest of all with reasonably high compression ratio.
    private int level = 1;

    public void setBlockSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public ZStdCompressionCodec() {
    }

    @Override
    public OutputStream compressedOutputStream(OutputStream outputStream) throws IOException {
        // Wrap the zstd output stream in a buffered output stream, so that we can
        // avoid overhead excessive of JNI call while trying to compress small amount of data.
        return new BufferedOutputStream(new ZstdOutputStream(outputStream, 1), bufferSize);
    }

    @Override
    public InputStream compressedInputStream(InputStream inputStream) throws IOException {
        return new BufferedInputStream(new ZstdInputStream(inputStream), bufferSize);
    }


    @Override
    public String getCompressionCodecName() {
        return KylinCodecFactory.KylinCodecType.zstd.name();
    }
}



