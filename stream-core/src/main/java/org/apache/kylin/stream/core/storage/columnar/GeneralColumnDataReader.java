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

package org.apache.kylin.stream.core.storage.columnar;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class GeneralColumnDataReader implements ColumnDataReader{
    private ByteBuffer dataBuffer;
    private int numOfVals;
    private int dataStartOffset;
    private int indexStartOffset;

    public GeneralColumnDataReader(ByteBuffer dataBuffer, int dataStartOffset, int dataLength) {
        this.dataBuffer = dataBuffer;
        this.dataStartOffset = dataStartOffset;
        dataBuffer.position(dataStartOffset + dataLength - 4);
        this.numOfVals = dataBuffer.getInt();
        this.indexStartOffset = dataStartOffset + dataLength - 4 - 4 * numOfVals;
    }

    @Override
    public byte[] read(int index) {
        int offset;
        if (index == 0) {
            offset = 0;
        } else {
            dataBuffer.position(indexStartOffset + ((index - 1) << 2));
            offset = dataBuffer.getInt();
        }
        dataBuffer.position(dataStartOffset + offset);
        int length = dataBuffer.getInt();
        byte[] result = new byte[length];
        dataBuffer.get(result);
        return result;
    }

    @Override
    public Iterator<byte[]> iterator() {
        dataBuffer.position(dataStartOffset);
        return new Iterator<byte[]>() {
            int readRowCount = 0;
            @Override
            public boolean hasNext() {
                return readRowCount < numOfVals;
            }

            @Override
            public byte[] next() {
                int size = dataBuffer.getInt();
                byte[] result = new byte[size];
                dataBuffer.get(result);
                readRowCount ++;
                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("unSupport operation");
            }
        };
    }

    public ByteBuffer get(int index) {
        int offset;
        if (index == 0) {
            offset = 0;
        } else {
            dataBuffer.position(indexStartOffset + ((index - 1) << 2));
            offset = dataBuffer.getInt();
        }

        ByteBuffer resultBuffer = dataBuffer.asReadOnlyBuffer();
        int startOffset = dataStartOffset + offset;
        resultBuffer.position(startOffset);
        int length = resultBuffer.getInt();
        resultBuffer.limit(startOffset + 4 + length);
        return resultBuffer;
    }

    public int getNumOfVals() {
        return numOfVals;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
