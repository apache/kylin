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

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.kylin.stream.core.storage.columnar.ColumnDataWriter;
import org.apache.kylin.stream.core.storage.columnar.GeneralColumnDataWriter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class LZ4CompressedColumnWriter implements ColumnDataWriter {
    public static final int DEF_BLOCK_SIZE = 64 * 1024;
    private int valLen;
    private int numValInBlock;
    private int blockCnt;
    private LZ4Compressor compressor;
    private ByteBuffer writeBuffer;
    private DataOutputStream dataOutput;

    private GeneralColumnDataWriter blockDataWriter;

    public LZ4CompressedColumnWriter(int valLen, int rowCnt, int compressBlockSize, OutputStream output) {
        this.valLen = valLen;
        this.numValInBlock = compressBlockSize / valLen;
        this.blockCnt = rowCnt / numValInBlock;
        if (rowCnt % numValInBlock != 0) {
            blockCnt++;
        }
        this.compressor = LZ4Factory.fastestInstance().highCompressor();
        this.writeBuffer = ByteBuffer.allocate(numValInBlock * valLen);
        this.dataOutput = new DataOutputStream(output);
        this.blockDataWriter = new GeneralColumnDataWriter(blockCnt, dataOutput);
    }

    public void write(byte[] valBytes) throws IOException {
        if (!writeBuffer.hasRemaining()) {
            writeBuffer.rewind();
            byte[] block = compressor.compress(writeBuffer.array(), 0, writeBuffer.limit());
            blockDataWriter.write(block);
        }
        writeBuffer.put(valBytes);
    }

    public void flush() throws IOException {
        if (writeBuffer != null) {
            writeBuffer.flip();
            if (writeBuffer.hasRemaining()) {
                byte[] block = compressor.compress(writeBuffer.array(), 0, writeBuffer.limit());
                blockDataWriter.write(block);
            }
        }
        blockDataWriter.flush();
        dataOutput.writeInt(numValInBlock);
        dataOutput.writeInt(valLen);
        dataOutput.flush();
        writeBuffer = null;
    }
}
