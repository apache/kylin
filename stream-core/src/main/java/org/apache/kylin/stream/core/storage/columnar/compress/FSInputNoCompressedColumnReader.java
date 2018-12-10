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
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.kylin.stream.core.storage.columnar.ColumnDataReader;

public class FSInputNoCompressedColumnReader implements ColumnDataReader {
    private FSDataInputStream fsInputStream;
    private byte[] readBuffer;
    private int colDataStartOffset;
    private int colValLength;
    private int rowCount;

    public FSInputNoCompressedColumnReader(FSDataInputStream fsInputStream, int colDataStartOffset, int colValLength,
            int rowCount) throws IOException {
        this.fsInputStream = fsInputStream;
        this.colDataStartOffset = colDataStartOffset;
        fsInputStream.seek(colDataStartOffset);
        this.colValLength = colValLength;
        this.rowCount = rowCount;
        this.readBuffer = new byte[colValLength];
    }

    public Iterator<byte[]> iterator() {
        return new NoCompressedColumnDataItr();
    }

    @Override
    public byte[] read(int rowNum) {
        throw new UnsupportedOperationException("not support to read row operation");
    }

    @Override
    public void close() throws IOException {
        fsInputStream.close();
    }

    private class NoCompressedColumnDataItr implements Iterator<byte[]> {
        private int readRowCount = 0;

        @Override
        public boolean hasNext() {
            return readRowCount < rowCount;
        }

        @Override
        public byte[] next() {
            try {
                fsInputStream.readFully(readBuffer);
            } catch (IOException e) {
                throw new RuntimeException("error when read data", e);
            }
            readRowCount++;
            return readBuffer;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("not supported");
        }
    }

}
