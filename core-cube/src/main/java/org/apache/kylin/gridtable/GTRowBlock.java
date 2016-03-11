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

package org.apache.kylin.gridtable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ImmutableBitSet;

public class GTRowBlock {

    /** create a row block, allocate memory, get ready for writing */
    public static GTRowBlock allocate(GTInfo info) {
        GTRowBlock b = new GTRowBlock(info);

        byte[] array = new byte[info.getMaxColumnLength(info.primaryKey)];
        b.primaryKey.set(array);

        int maxRows = info.isRowBlockEnabled() ? info.rowBlockSize : 1;
        for (int i = 0; i < b.cellBlocks.length; i++) {
            array = new byte[info.getMaxColumnLength(info.colBlocks[i]) * maxRows];
            b.cellBlocks[i].set(array);
        }
        return b;
    }

    final GTInfo info;

    int seqId; // 0, 1, 2...
    int nRows;
    ByteArray primaryKey; // the primary key of the first (smallest) row
    ByteArray[] cellBlocks; // cells for each column block

    /** create a row block that has no underlying space */
    public GTRowBlock(GTInfo info) {
        this.info = info;
        this.primaryKey = new ByteArray();
        this.cellBlocks = new ByteArray[info.colBlocks.length];
        for (int i = 0; i < this.cellBlocks.length; i++) {
            this.cellBlocks[i] = new ByteArray();
        }
    }

    public int getSequenceId() {
        return seqId;
    }

    public ByteArray getPrimaryKey() {
        return primaryKey;
    }

    public ByteArray getCellBlock(int i) {
        return cellBlocks[i];
    }

    public Writer getWriter() {
        return new Writer();
    }

    public class Writer {
        ByteBuffer[] cellBlockBuffers;

        Writer() {
            cellBlockBuffers = new ByteBuffer[info.colBlocks.length];
            for (int i = 0; i < cellBlockBuffers.length; i++) {
                cellBlockBuffers[i] = cellBlocks[i].asBuffer();
            }
        }

        public void copyFrom(GTRowBlock other) {
            assert info == other.info;

            seqId = other.seqId;
            nRows = other.nRows;
            primaryKey.copyFrom(other.primaryKey);
            for (int i = 0; i < info.colBlocks.length; i++) {
                cellBlockBuffers[i].clear();
                cellBlockBuffers[i].put(other.cellBlocks[i].array(), other.cellBlocks[i].offset(), other.cellBlocks[i].length());
            }
        }

        public void append(GTRecord r) {
            // add record to block
            if (isEmpty()) {
                r.exportColumns(info.primaryKey, primaryKey);
            }
            for (int i = 0; i < info.colBlocks.length; i++) {
                r.exportColumnBlock(i, cellBlockBuffers[i]);
            }
            nRows++;
        }

        public void readyForFlush() {
            for (int i = 0; i < cellBlocks.length; i++) {
                cellBlocks[i].setLength(cellBlockBuffers[i].position());
            }
        }

        public void clearForNext() {
            seqId++;
            nRows = 0;
            for (int i = 0; i < cellBlockBuffers.length; i++) {
                cellBlockBuffers[i].clear();
            }
        }
    }

    public Reader getReader() {
        return new Reader(info.colBlocksAll);
    }

    public Reader getReader(ImmutableBitSet selectedColBlocks) {
        return new Reader(selectedColBlocks);
    }

    public class Reader {
        int cur;
        ByteBuffer primaryKeyBuffer;
        ByteBuffer[] cellBlockBuffers;
        ImmutableBitSet selectedColBlocks;

        Reader(ImmutableBitSet selectedColBlocks) {
            primaryKeyBuffer = primaryKey.asBuffer();
            cellBlockBuffers = new ByteBuffer[info.colBlocks.length];
            for (int i = 0; i < cellBlockBuffers.length; i++) {
                cellBlockBuffers[i] = cellBlocks[i].asBuffer();
            }
            this.selectedColBlocks = selectedColBlocks;
        }

        public boolean hasNext() {
            return cur < nRows;
        }

        public void fetchNext(GTRecord result) {
            if (hasNext() == false)
                throw new IllegalArgumentException();

            for (int i = 0; i < selectedColBlocks.trueBitCount(); i++) {
                int c = selectedColBlocks.trueBitAt(i);
                result.loadCellBlock(c, cellBlockBuffers[c]);
            }
            cur++;
        }
    }

    public GTRowBlock copy() {
        GTRowBlock copy = new GTRowBlock(info);

        ByteBuffer buf = ByteBuffer.allocate(this.exportLength());
        this.export(buf);
        buf.clear();
        copy.load(buf);

        return copy;
    }

    public boolean isEmpty() {
        return nRows == 0;
    }

    public boolean isFull() {
        if (info.isRowBlockEnabled())
            return nRows >= info.rowBlockSize;
        else
            return nRows > 0;
    }

    public int getNumberOfRows() {
        return nRows;
    }

    public void setNumberOfRows(int nRows) {
        this.nRows = nRows;
    }

    // ============================================================================

    public int exportLength() {
        int len = 4; // seq Id
        if (info.isRowBlockEnabled())
            len += 4; // nRows
        len += 4 + primaryKey.length(); // PK byte array
        for (ByteArray array : cellBlocks) {
            len += 4 + array.length(); // cell block byte array
        }
        return len;
    }

    /** write data to given output stream, like serialize */
    public void export(DataOutputStream out) throws IOException {
        out.writeInt(seqId);
        if (info.isRowBlockEnabled())
            out.writeInt(nRows);
        export(out, primaryKey);
        for (ByteArray cb : cellBlocks) {
            export(out, cb);
        }
    }

    private void export(DataOutputStream out, ByteArray array) throws IOException {
        out.writeInt(array.length());
        out.write(array.array(), array.offset(), array.length());
    }

    /** write data to given buffer, like serialize */
    public void export(ByteBuffer buf) {
        buf.putInt(seqId);
        if (info.isRowBlockEnabled())
            buf.putInt(nRows);
        export(primaryKey, buf);
        for (ByteArray cb : cellBlocks) {
            export(cb, buf);
        }
    }

    private void export(ByteArray array, ByteBuffer buf) {
        buf.putInt(array.length());
        buf.put(array.array(), array.offset(), array.length());
    }

    /** read data from given input stream, like deserialize */
    public void importFrom(DataInputStream in) throws IOException {
        seqId = in.readInt();
        nRows = info.isRowBlockEnabled() ? in.readInt() : 1;
        importFrom(in, primaryKey);
        for (int i = 0; i < info.colBlocks.length; i++) {
            ByteArray cb = cellBlocks[i];
            importFrom(in, cb);
        }
    }

    private void importFrom(DataInputStream in, ByteArray result) throws IOException {
        byte[] data = result.array();
        int len = in.readInt();
        in.read(data, 0, len);
        result.set(data, 0, len);
    }

    /** change pointers to point to data in given buffer, UNLIKE deserialize */
    public void load(ByteBuffer buf) {
        seqId = buf.getInt();
        nRows = info.isRowBlockEnabled() ? buf.getInt() : 1;
        load(primaryKey, buf);
        for (int i = 0; i < info.colBlocks.length; i++) {
            ByteArray cb = cellBlocks[i];
            load(cb, buf);
        }
    }

    private void load(ByteArray array, ByteBuffer buf) {
        int len = buf.getInt();
        int pos = buf.position();
        array.set(buf.array(), buf.arrayOffset() + pos, len);
        buf.position(pos + len);
    }

}
