/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.cube.invertedindex;

import com.google.common.collect.Lists;
import com.kylinolap.common.util.BytesUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author yangli9
 */
public class IIKeyValueCodec {

    private static final int TIMEPART_LEN = 8;
    private static final int SLICENO_LEN = 3;
    private static final int COLNO_LEN = 2;

    private TableRecordInfo info;

    public IIKeyValueCodec(TableRecordInfo info) {
        this.info = info;
    }

    public Collection<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> encodeKeyValue(TimeSlice slice) {
        ArrayList<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> result = Lists.newArrayList();
        ColumnValueContainer[] containers = slice.containers;
        for (int col = 0; col < containers.length; col++) {
            if (containers[col] instanceof BitMapContainer) {
                collectKeyValues(slice, col, (BitMapContainer) containers[col], result);
            } else if (containers[col] instanceof CompressedValueContainer) {
                collectKeyValues(slice, col, (CompressedValueContainer) containers[col], result);
            } else {
                throw new IllegalArgumentException("Unkown container class " + containers[col].getClass());
            }
        }
        return result;
    }

    private void collectKeyValues(TimeSlice slice, int col, CompressedValueContainer container,
                                  ArrayList<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> result) {
        ImmutableBytesWritable key = encodeKey(slice.getTimeParititon(), slice.getSliceNo(), col, -1);
        ImmutableBytesWritable value = container.toBytes();
        result.add(new Pair<ImmutableBytesWritable, ImmutableBytesWritable>(key, value));
    }

    private void collectKeyValues(TimeSlice slice, int col, BitMapContainer container,
                                  ArrayList<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> result) {
        List<ImmutableBytesWritable> values = container.toBytes();
        for (int v = 0; v < values.size(); v++) {
            ImmutableBytesWritable key = encodeKey(slice.getTimeParititon(), slice.getSliceNo(), col, v);
            result.add(new Pair<ImmutableBytesWritable, ImmutableBytesWritable>(key, values.get(v)));
        }
    }

    ImmutableBytesWritable encodeKey(long timePartition, int sliceNo, int col, int colValue) {
        byte[] bytes = new byte[20];
        int len = encodeKey(timePartition, sliceNo, col, colValue, bytes, 0);
        return new ImmutableBytesWritable(bytes, 0, len);
    }

    int encodeKey(long timePartition, int sliceNo, int col, int colValue, byte[] buf, int offset) {
        int i = offset;

        BytesUtil.writeUnsignedLong(timePartition, buf, i, TIMEPART_LEN);
        i += TIMEPART_LEN;

        BytesUtil.writeUnsigned(sliceNo, buf, i, SLICENO_LEN);
        i += SLICENO_LEN;

        BytesUtil.writeUnsigned(col, buf, i, COLNO_LEN);
        i += COLNO_LEN;

        if (colValue >= 0) {
            int colLen = info.length(col);
            BytesUtil.writeUnsigned(colValue, buf, i, colLen);
            i += colLen;
        }

        return i - offset;
    }

    public Iterable<TimeSlice> decodeKeyValue(
            Iterable<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> kvs) {
        return new Decoder(info, kvs);
    }

    private static class Decoder implements Iterable<TimeSlice> {

        TableRecordInfo info;
        Iterator<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> iterator;

        TimeSlice next = null;
        long curPartition = Long.MIN_VALUE;
        int curSliceNo = -1;
        int curCol = -1;
        int curColValue = -1;
        long lastPartition = Long.MIN_VALUE;
        int lastSliceNo = -1;
        int lastCol = -1;
        ColumnValueContainer[] containers = null;
        List<ImmutableBytesWritable> bitMapValues = Lists.newArrayList();

        Decoder(TableRecordInfo info, Iterable<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> kvs) {
            this.info = info;
            this.iterator = kvs.iterator();
        }

        private void goToNext() {
            if (next != null) { // was not fetched
                return;
            }

            // NOTE the input keys are ordered
            while (next == null && iterator.hasNext()) {
                Pair<ImmutableBytesWritable, ImmutableBytesWritable> kv = iterator.next();
                ImmutableBytesWritable k = kv.getFirst();
                ImmutableBytesWritable v = kv.getSecond();
                decodeKey(k);

                if (curPartition != lastPartition || curSliceNo != lastSliceNo) {
                    makeNext();
                }
                consumeCurrent(v);
            }
            if (next == null) {
                makeNext();
            }
        }

        private void decodeKey(ImmutableBytesWritable k) {
            byte[] buf = k.get();
            int i = k.getOffset();

            curPartition = BytesUtil.readUnsignedLong(buf, i, TIMEPART_LEN);
            i += TIMEPART_LEN;

            curSliceNo = BytesUtil.readUnsigned(buf, i, SLICENO_LEN);
            i += SLICENO_LEN;

            curCol = BytesUtil.readUnsigned(buf, i, COLNO_LEN);
            i += COLNO_LEN;

            if (i - k.getOffset() < k.getLength()) {
                int colLen = info.length(curCol);
                curColValue = BytesUtil.readUnsigned(buf, i, colLen);
                i += colLen;
            } else {
                curColValue = -1;
            }
        }

        private void consumeCurrent(ImmutableBytesWritable v) {
            if (curCol != lastCol && bitMapValues.isEmpty() == false) {
                addBitMapContainer(lastCol);
            }
            if (curColValue < 0) {
                CompressedValueContainer c = new CompressedValueContainer(info, curCol, 0);
                c.fromBytes(v);
                addContainer(curCol, c);
            } else {
                assert curColValue == bitMapValues.size();
                bitMapValues.add(v);
            }

            lastPartition = curPartition;
            lastSliceNo = curSliceNo;
            lastCol = curCol;
        }

        private void makeNext() {
            if (bitMapValues.isEmpty() == false) {
                addBitMapContainer(lastCol);
            }
            if (containers != null) {
                next = new TimeSlice(info, lastPartition, lastSliceNo, containers);
            }
            lastPartition = Long.MIN_VALUE;
            lastSliceNo = -1;
            lastCol = -1;
            containers = null;
            bitMapValues.clear();
        }

        private void addBitMapContainer(int col) {
            BitMapContainer c = new BitMapContainer(info, col);
            c.fromBytes(bitMapValues);
            addContainer(col, c);
            bitMapValues.clear();
        }

        private void addContainer(int col, ColumnValueContainer c) {
            if (containers == null) {
                containers = new ColumnValueContainer[info.getColumnCount()];
            }
            containers[col] = c;
        }

        @Override
        public Iterator<TimeSlice> iterator() {
            return new Iterator<TimeSlice>() {
                @Override
                public boolean hasNext() {
                    goToNext();
                    return next != null;
                }

                @Override
                public TimeSlice next() {
                    TimeSlice result = next;
                    next = null;
                    return result;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

    }

}
