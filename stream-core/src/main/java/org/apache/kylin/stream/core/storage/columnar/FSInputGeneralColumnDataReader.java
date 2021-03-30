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
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FSInputGeneralColumnDataReader implements ColumnDataReader {
    private FSDataInputStream fsInputStream;
    private int numOfVals;

    public FSInputGeneralColumnDataReader(FileSystem fs, Path file, int dataStartOffset, int dataLength)
            throws IOException {
        this.fsInputStream = fs.open(file);
        fsInputStream.seek(dataStartOffset + dataLength - 4L);
        this.numOfVals = fsInputStream.readInt();
        fsInputStream.seek(dataStartOffset);
    }

    @Override
    public byte[] read(int index) {
        throw new UnsupportedOperationException("not support to read row operation");
    }

    @Override
    public Iterator<byte[]> iterator() {
        return new Iterator<byte[]>() {
            int readRowCount = 0;

            @Override
            public boolean hasNext() {
                return readRowCount < numOfVals;
            }

            @Override
            public byte[] next() {
                try {
                    int size = fsInputStream.readInt();
                    byte[] result = new byte[size];
                    fsInputStream.readFully(result);
                    readRowCount++;
                    return result;
                } catch (IOException e) {
                    throw new NoSuchElementException("error when read data");
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("unSupport operation");
            }
        };
    }

    @Override
    public void close() throws IOException {
        fsInputStream.close();
    }
}
