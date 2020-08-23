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

public class NoCompressedColumnReader implements ColumnDataReader {
    private ByteBuffer dataBuffer;
    private int colDataStartOffset;
    private int colValLength;
    private int rowCount;

    public NoCompressedColumnReader(ByteBuffer dataBuffer, int colDataStartOffset, int colValLength, int rowCount) {
        this.dataBuffer = dataBuffer;
        this.colDataStartOffset = colDataStartOffset;
        this.colValLength = colValLength;
        this.rowCount = rowCount;
    }

    public Iterator<byte[]> iterator() {
        return new NoCompressedColumnDataItr();
    }

    public byte[] read(int rowNum) {
        byte[] readBuffer = new byte[colValLength];
        dataBuffer.position(colDataStartOffset + rowNum * colValLength);
        dataBuffer.get(readBuffer);
        return readBuffer;
    }

    @Override
    public void close() throws IOException {
        //do nothing
    }

    private class NoCompressedColumnDataItr implements Iterator<byte[]> {
        private int readRowCount = 0;

        public NoCompressedColumnDataItr() {
            dataBuffer.position(colDataStartOffset);
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
            byte[] readBuffer = new byte[colValLength];
            dataBuffer.get(readBuffer);
            readRowCount++;
            return readBuffer;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("not supported");
        }
    }

}
