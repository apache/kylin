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

import org.apache.kylin.stream.core.storage.columnar.ColumnDataReader;
import org.apache.kylin.stream.core.storage.columnar.GeneralColumnDataReader;

public class RunLengthCompressedColumnReader implements ColumnDataReader {
    private int valLen;
    private int numValInBlock;
    private ByteBuffer currBlockBuffer;
    private int currBlockNum;

    //private byte[] readBuffer;
    private int rowCount;
    private GeneralColumnDataReader blockDataReader;

    public RunLengthCompressedColumnReader(ByteBuffer dataBuffer, int columnDataStartOffset, int columnDataLength,
            int rowCount) {
        this.rowCount = rowCount;
        int footStartOffset = columnDataStartOffset + columnDataLength - 8;
        dataBuffer.position(footStartOffset);
        this.numValInBlock = dataBuffer.getInt();
        this.valLen = dataBuffer.getInt();

        this.blockDataReader = new GeneralColumnDataReader(dataBuffer, columnDataStartOffset, columnDataLength - 8);
        this.currBlockNum = -1;
        //this.readBuffer = new byte[valLen];
    }

    private void loadBuffer(int targetBlockNum) {
        currBlockBuffer = blockDataReader.get(targetBlockNum);
        currBlockNum = targetBlockNum;
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
        byte[] readBuffer = new byte[valLen];

        int targetBlockNum = rowNum / numValInBlock;
        if (targetBlockNum != currBlockNum) {
            loadBuffer(targetBlockNum);
        }
        int blockStartOffset = currBlockBuffer.position();
        int limit = currBlockBuffer.limit();
        int entryNum = currBlockBuffer.getInt(limit - 4);
        int entryIndexStartOffset = limit - 4 - (entryNum << 2);
        int blockRowNum = rowNum % numValInBlock;

        int targetEntry = binarySearchIndex(currBlockBuffer, entryIndexStartOffset, entryNum, blockRowNum);
        currBlockBuffer.position(blockStartOffset + (valLen + 4) * targetEntry + 4);
        currBlockBuffer.get(readBuffer);
        currBlockBuffer.position(blockStartOffset);
        return readBuffer;
    }

    private int binarySearchIndex(ByteBuffer currBlockBuffer, int entryIndexStartOffset, int entryNum, int rowNum) {
        int low = 0;
        int high = entryNum - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;

            int idxVal = currBlockBuffer.getInt(entryIndexStartOffset + (mid << 2));
            if (idxVal >= rowNum) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return low;
    }

    @Override
    public void close() throws IOException {
        //do nothing
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
                loadNextBuffer();
                blockReadRowCount = 0;
            }
            currRLEntryVal = new byte[valLen];
            currRLEntryValCnt = currBlockBuffer.getInt();
            currBlockBuffer.get(currRLEntryVal);
            readRLEntryValCnt = 0;
        }

        private void loadNextBuffer() {
            loadBuffer(currBlockNum + 1);
        }
    }
}
