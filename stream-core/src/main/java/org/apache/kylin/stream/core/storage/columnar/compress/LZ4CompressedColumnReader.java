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

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.kylin.stream.core.storage.columnar.ColumnDataReader;
import org.apache.kylin.stream.core.storage.columnar.GeneralColumnDataReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class LZ4CompressedColumnReader implements ColumnDataReader {
    private int rowCount;

    private int valLen;
    private int numValInBlock;
    private int maxBufferLength;

    private int currBlockNum;
    private LZ4SafeDecompressor deCompressor;
    private ByteBuffer decompressedBuffer;

    private GeneralColumnDataReader blockDataReader;

    public LZ4CompressedColumnReader(ByteBuffer dataBuffer, int columnDataStartOffset, int columnDataLength,
                                 int rowCount) {
        this.rowCount = rowCount;
        int footStartOffset = columnDataStartOffset + columnDataLength - 8;
        dataBuffer.position(footStartOffset);
        this.numValInBlock = dataBuffer.getInt();
        this.valLen = dataBuffer.getInt();

        this.blockDataReader = new GeneralColumnDataReader(dataBuffer, columnDataStartOffset, columnDataLength - 8);
        this.currBlockNum = -1;

        this.deCompressor = LZ4Factory.fastestInstance().safeDecompressor();
        this.maxBufferLength = numValInBlock * valLen;
        this.decompressedBuffer = ByteBuffer.allocate(maxBufferLength);
    }

    private void loadBuffer(int targetBlockNum) {
        ByteBuffer compressedBuffer = blockDataReader.get(targetBlockNum);
        int length = compressedBuffer.limit() - compressedBuffer.position();
        deCompressor.decompress(compressedBuffer, compressedBuffer.position(), length, decompressedBuffer, 0,
                maxBufferLength);
        decompressedBuffer.position(0);
        currBlockNum = targetBlockNum;
    }

    @Override
    public Iterator<byte[]> iterator() {
        return new LZ4CompressedColumnDataItr();
    }

    @Override
    public byte[] read(int rowNum) {
        int targetBlockNum = rowNum / numValInBlock;
        if (targetBlockNum != currBlockNum) {
            loadBuffer(targetBlockNum);
        }
        decompressedBuffer.position((rowNum % numValInBlock) * valLen);
        byte[] readBuffer = new byte[valLen];
        decompressedBuffer.get(readBuffer);
        return readBuffer;
    }

    @Override
    public void close() throws IOException {
        //do nothing
    }

    private class LZ4CompressedColumnDataItr implements Iterator<byte[]> {
        private int readRowCount = 0;

        public LZ4CompressedColumnDataItr() {
        }

        @Override
        public boolean hasNext() {
            return readRowCount < rowCount;
        }

        @Override
        public byte[] next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            if (currBlockNum == -1 || !decompressedBuffer.hasRemaining()) {
                loadNextBuffer();
            }
            byte[] readBuffer = new byte[valLen];
            decompressedBuffer.get(readBuffer);
            readRowCount ++;
            return readBuffer;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("not supported");
        }

        private void loadNextBuffer() {
            loadBuffer(currBlockNum + 1);
        }
    }
}
