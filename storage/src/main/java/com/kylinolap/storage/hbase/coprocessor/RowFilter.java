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

package com.kylinolap.storage.hbase.coprocessor;

import com.google.common.collect.Lists;
import com.kylinolap.common.util.BytesSerializer;
import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.cube.kv.AbstractRowKeyEncoder;
import com.kylinolap.cube.kv.RowConstants;
import com.kylinolap.cube.kv.RowKeyEncoder;
import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.storage.hbase.ColumnValueRange;
import com.kylinolap.storage.hbase.HBaseKeyRange;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author yangli9
 */
public class RowFilter {

    private static class DefaultRowFilter extends RowFilter {
        public DefaultRowFilter() {
            super(null);
        }

        @Override
        public boolean evaluate(ImmutableBytesWritable rowkey) {
            return true;
        }
    }

    public static byte[] serialize(RowFilter o) {
        ByteBuffer buf = ByteBuffer.allocate(CoprocessorEnabler.SERIALIZE_BUFFER_SIZE);
        serializer.serialize(o, buf);
        byte[] result = new byte[buf.position()];
        System.arraycopy(buf.array(), 0, result, 0, buf.position());
        return result;
    }

    public static RowFilter deserialize(byte[] filterBytes) {
        if (filterBytes == null || filterBytes.length == 0) {
            return new DefaultRowFilter();
        }
        return serializer.deserialize(ByteBuffer.wrap(filterBytes));
    }

    private static final Serializer serializer = new Serializer();

    private static class Serializer implements BytesSerializer<RowFilter> {

        @Override
        public void serialize(RowFilter value, ByteBuffer out) {
            ColumnFilter[][] filter = value.flatOrAndFilter;
            if (filter == null) {
                BytesUtil.writeVInt(-1, out);
                return;
            }
            BytesUtil.writeVInt(filter.length, out);
            for (int i = 0; i < filter.length; i++) {
                serialize(filter[i], out);
            }
        }

        private void serialize(ColumnFilter[] filter, ByteBuffer out) {
            if (filter == null) {
                BytesUtil.writeVInt(-1, out);
                return;
            }
            BytesUtil.writeVInt(filter.length, out);
            for (int i = 0; i < filter.length; i++) {
                serialize(filter[i], out);
            }
        }

        private void serialize(ColumnFilter filter, ByteBuffer out) {
            BytesUtil.writeVInt(filter.columnOffset, out);
            BytesUtil.writeVInt(filter.columnLength, out);
            BytesUtil.writeByteArray(filter.valueStart, out);
            BytesUtil.writeByteArray(filter.valueEnd, out);
        }

        @Override
        public RowFilter deserialize(ByteBuffer in) {
            ColumnFilter[][] filter = null;
            int len = BytesUtil.readVInt(in);
            if (len >= 0) {
                filter = new ColumnFilter[len][];
                for (int i = 0; i < len; i++) {
                    filter[i] = deserializeAndFilter(in);
                }
            }
            return new RowFilter(filter);
        }

        private ColumnFilter[] deserializeAndFilter(ByteBuffer in) {
            ColumnFilter[] filter = null;
            int len = BytesUtil.readVInt(in);
            if (len >= 0) {
                filter = new ColumnFilter[len];
                for (int i = 0; i < len; i++) {
                    filter[i] = deserializeColumnValueFilter(in);
                }
            }
            return filter;
        }

        private ColumnFilter deserializeColumnValueFilter(ByteBuffer in) {
            int offset = BytesUtil.readVInt(in);
            int length = BytesUtil.readVInt(in);
            byte[] valueStart = BytesUtil.readByteArray(in);
            byte[] valueEnd = BytesUtil.readByteArray(in);
            return new ColumnFilter(offset, length, valueStart, valueEnd);
        }
    }

    // ============================================================================

    protected final ColumnFilter[][] flatOrAndFilter; // like (A AND B AND ..) OR (C AND D AND ..) OR ..

    protected RowFilter(ColumnFilter[][] flatOrAndFilter) {
        this.flatOrAndFilter = flatOrAndFilter;
    }

    public boolean evaluate(ImmutableBytesWritable rowkey) {
        if (flatOrAndFilter == null || flatOrAndFilter.length == 0)
            return true;

        boolean r = false;
        for (int i = 0, n = flatOrAndFilter.length; i < n && r == false; i++) {
            r = evaluateAndFilter(rowkey, flatOrAndFilter[i]);
        }
        return r;
    }

    private boolean evaluateAndFilter(ImmutableBytesWritable rowkey, ColumnFilter[] andFilter) {
        if (andFilter == null || andFilter.length == 0)
            return false;

        boolean r = true;
        for (int i = 0, n = andFilter.length; i < n && r == true; i++) {
            r = andFilter[i].evaluate(rowkey);
        }
        return r;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(flatOrAndFilter);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RowFilter other = (RowFilter) obj;
        if (!Arrays.deepEquals(flatOrAndFilter, other.flatOrAndFilter))
            return false;
        return true;
    }

    // ============================================================================

    public static class ColumnFilter {
        final int columnOffset;
        final int columnLength;
        final byte[] valueStart;
        final byte[] valueEnd;

        public ColumnFilter(int columnOffset, int columnLength, byte[] valueStart, byte[] valueEnd) {
            this.columnOffset = columnOffset;
            this.columnLength = columnLength;
            this.valueStart = valueStart;
            this.valueEnd = valueEnd;

            if (valueStart != null)
                assert columnLength == valueStart.length;
            if (valueEnd != null)
                assert columnLength == valueEnd.length;
        }

        public boolean evaluate(ImmutableBytesWritable rowkey) {
            if (valueStart != null) {
                if (compareToValue(rowkey, valueStart) < 0)
                    return false;
            }
            if (valueEnd != null) {
                if (compareToValue(rowkey, valueEnd) > 0)
                    return false;
            }
            return true;
        }

        private int compareToValue(ImmutableBytesWritable rowkey, byte[] v) {
            return Bytes.compareTo(rowkey.get(), rowkey.getOffset() + columnOffset, columnLength, v, 0,
                    v.length);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + columnLength;
            result = prime * result + columnOffset;
            result = prime * result + Arrays.hashCode(valueEnd);
            result = prime * result + Arrays.hashCode(valueStart);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ColumnFilter other = (ColumnFilter) obj;
            if (columnLength != other.columnLength)
                return false;
            if (columnOffset != other.columnOffset)
                return false;
            if (!Arrays.equals(valueEnd, other.valueEnd))
                return false;
            if (!Arrays.equals(valueStart, other.valueStart))
                return false;
            return true;
        }
    }

    public static RowFilter fromKeyRange(HBaseKeyRange keyRange) {
        List<Collection<ColumnValueRange>> flatOrAndFilter = keyRange.getFlatOrAndFilter();
        RowKeyEncoder encoder =
                (RowKeyEncoder) AbstractRowKeyEncoder.createInstance(keyRange.getCubeSegment(),
                        keyRange.getCuboid());

        // translate flatOrAndFilter to coprocessor RowFilter
        List<ColumnFilter[]> result = Lists.newArrayListWithCapacity(flatOrAndFilter.size());
        for (int i = 0; i < flatOrAndFilter.size(); i++) {
            ColumnFilter[] filter = buildAndFilter(flatOrAndFilter.get(i), encoder);
            if (filter != null && filter.length > 0)
                result.add(filter);
        }

        return new RowFilter((ColumnFilter[][]) result.toArray(new ColumnFilter[result.size()][]));
    }

    private static ColumnFilter[] buildAndFilter(Collection<ColumnValueRange> andFilter, RowKeyEncoder encoder) {
        ColumnFilter[] result = new ColumnFilter[andFilter.size()];
        Iterator<ColumnValueRange> iterator = andFilter.iterator();
        for (int i = 0; i < result.length; i++) {
            result[i] = buildColumnValueFilter(iterator.next(), encoder);
        }
        return result;
    }

    private static ColumnFilter buildColumnValueFilter(ColumnValueRange range, RowKeyEncoder encoder) {
        TblColRef col = range.getColumn();
        int colOffset = encoder.getColumnOffset(col);
        int colLength = encoder.getColumnLength(col);

        byte[] valueStart =
                encodeColumnValue(range.getBeginValue(), col, colOffset, colLength,
                        RowConstants.ROWKEY_LOWER_BYTE, encoder);
        byte[] valueEnd =
                encodeColumnValue(range.getEndValue(), col, colOffset, colLength,
                        RowConstants.ROWKEY_UPPER_BYTE, encoder);
        return new ColumnFilter(colOffset, colLength, valueStart, valueEnd);
    }

    private static byte[] encodeColumnValue(String value, TblColRef col, int colOffset, int colLength,
                                            byte dft, RowKeyEncoder encoder) {
        if (value == null)
            return null;

        byte[] valueBytes = encoder.valueStringToBytes(value);
        byte[] valueOnRowKey = new byte[colLength];
        encoder.getColumnIO().writeColumn(col, valueBytes, valueBytes.length, dft, valueOnRowKey, 0);
        return valueOnRowKey;
    }

}
