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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.stream.core.storage.columnar.ColumnDataReader;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

public class FSInputLZ4CompressedColumnReader implements ColumnDataReader {
    private int rowCount;

    private int valLen;
    private int numValInBlock;
    private int maxDecompressedLength;

    private int currBlockNum;
    private LZ4SafeDecompressor deCompressor;
    private FSDataInputStream fsInputStream;

    public FSInputLZ4CompressedColumnReader(FileSystem fs, Path file, int columnDataStartOffset,
                                            int columnDataLength, int rowCount) throws IOException {
        this.rowCount = rowCount;
        this.fsInputStream = fs.open(file);
        int footStartOffset = columnDataStartOffset + columnDataLength - 8;
        fsInputStream.seek(footStartOffset);
        this.numValInBlock = fsInputStream.readInt();
        this.valLen = fsInputStream.readInt();

        fsInputStream.seek(columnDataStartOffset);
        this.currBlockNum = -1;

        this.deCompressor = LZ4Factory.fastestInstance().safeDecompressor();
        this.maxDecompressedLength = numValInBlock * valLen;
    }

    public Iterator<byte[]> iterator() {
        return new LZ4CompressedColumnDataItr();
    }

    @Override
    public byte[] read(int rowNum) {
        throw new UnsupportedOperationException("not support to read row operation");
    }

    @Override
    public void close() throws IOException {
        fsInputStream.close();
    }

    private class LZ4CompressedColumnDataItr implements Iterator<byte[]> {
        private int readRowCount = 0;
        private ByteBuffer decompressedBuffer;
        private byte[] decompressedBytes;

        public LZ4CompressedColumnDataItr() {
            this.decompressedBytes = new byte[maxDecompressedLength];
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
                try {
                    loadNextBuffer();
                } catch (IOException e) {
                    throw new NoSuchElementException("error when read data " + e.getMessage());
                }
            }
            byte[] readBuffer = new byte[valLen];
            decompressedBuffer.get(readBuffer);
            readRowCount++;
            return readBuffer;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("not supported");
        }

        private void loadNextBuffer() throws IOException {
            int len = fsInputStream.readInt();
            byte[] bytes = new byte[len];
            fsInputStream.readFully(bytes);
            int decompressedSize = deCompressor.decompress(bytes, decompressedBytes);
            decompressedBuffer = ByteBuffer.wrap(decompressedBytes, 0, decompressedSize);
            currBlockNum++;
        }
    }
}
