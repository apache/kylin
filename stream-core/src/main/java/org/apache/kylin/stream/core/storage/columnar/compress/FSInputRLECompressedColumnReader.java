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

public class FSInputRLECompressedColumnReader implements ColumnDataReader {
    private int valLen;
    private int numValInBlock;
    private ByteBuffer currBlockBuffer;
    private int currBlockNum;
    private FSDataInputStream fsInputStream;

    private int rowCount;

    public FSInputRLECompressedColumnReader(FileSystem fs, Path file, int columnDataStartOffset,
                                            int columnDataLength, int rowCount) throws IOException {
        this.rowCount = rowCount;
        this.fsInputStream = fs.open(file);
        int footStartOffset = columnDataStartOffset + columnDataLength - 8;
        fsInputStream.seek(footStartOffset);
        this.numValInBlock = fsInputStream.readInt();
        this.valLen = fsInputStream.readInt();
        this.fsInputStream.seek(columnDataStartOffset);
        this.currBlockNum = -1;
    }

    public void reset() {
        this.currBlockNum = -1;
    }

    @Override
    public Iterator<byte[]> iterator() {
        return new RunLengthCompressedColumnDataItr();
    }

    @Override
    public byte[] read(int rowNum) {
        throw new UnsupportedOperationException("not support to read row operation");
    }

    @Override
    public void close() throws IOException {
        fsInputStream.close();
    }

    private class RunLengthCompressedColumnDataItr implements Iterator<byte[]> {
        private int currRLEntryValCnt;
        private byte[] currRLEntryVal;
        private int readRLEntryValCnt;

        private int readRowCount = 0;
        private int blockReadRowCount = 0;

        public RunLengthCompressedColumnDataItr() {
            currRLEntryVal = new byte[valLen];
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
            if (readRLEntryValCnt >= currRLEntryValCnt) {
                loadNextEntry();
            }
            readRLEntryValCnt++;
            readRowCount++;
            blockReadRowCount++;
            return currRLEntryVal;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("not supported");
        }

        private void loadNextEntry() {
            if (currBlockNum == -1 || blockReadRowCount >= numValInBlock) {
                try {
                    loadNextBuffer();
                } catch (IOException e) {
                    throw new RuntimeException("error when read data", e);
                }
                currRLEntryVal = new byte[valLen];
                blockReadRowCount = 0;
            }
            currRLEntryValCnt = currBlockBuffer.getInt();
            currBlockBuffer.get(currRLEntryVal);
            readRLEntryValCnt = 0;
        }

        private void loadNextBuffer() throws IOException {
            int len = fsInputStream.readInt();
            byte[] bytes = new byte[len];
            fsInputStream.readFully(bytes);
            currBlockBuffer = ByteBuffer.wrap(bytes);
            currBlockNum++;
        }
    }
}
