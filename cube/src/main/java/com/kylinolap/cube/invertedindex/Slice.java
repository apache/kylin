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

import java.util.Iterator;

import it.uniroma3.mat.extendedset.intset.ConciseSet;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Within a partition (per timestampGranularity), records are further sliced
 * (per sliceLength) to fit into HBASE cell.
 *
 * @author yangli9
 */
public class Slice implements Iterable<TableRecordBytes>, Comparable<Slice> {

    TableRecordInfoDigest info;
    int nColumns;

    short shard;
    long timestamp;
    int nRecords;
    ColumnValueContainer[] containers;

    Slice(TableRecordInfoDigest info, short shard, long timestamp, ColumnValueContainer[] containers) {
        this.info = info;
        this.nColumns = info.getColumnCount();

        this.shard = shard;
        this.timestamp = timestamp;
        this.nRecords = containers[0].getSize();
        this.containers = containers;

        assert nColumns == containers.length;
        for (int i = 0; i < nColumns; i++) {
            assert nRecords == containers[i].getSize();
        }
    }

    public int getRecordCount() {
        return this.nRecords;
    }

    public short getShard() {
        return shard;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public ColumnValueContainer getColumnValueContainer(int col) {
        return containers[col];
    }

    public Iterator<TableRecordBytes> iterateWithBitmap(final ConciseSet resultBitMap) {
        if (resultBitMap == null) {
            return this.iterator();
        } else {
            return new Iterator<TableRecordBytes>() {
                int i = 0;
                int iteratedCount = 0;
                int resultSize = resultBitMap.size();

                TableRecordBytes rec = info.createTableRecord();
                ImmutableBytesWritable temp = new ImmutableBytesWritable();

                @Override
                public boolean hasNext() {
                    return iteratedCount < resultSize;
                }

                @Override
                public TableRecordBytes next() {
                    while (!resultBitMap.contains(i) && i < nRecords - 1) {
                        i++;
                    }
                    for (int col = 0; col < nColumns; col++) {
                        containers[col].getValueAt(i, temp);
                        rec.setValueBytes(col, temp);
                    }
                    iteratedCount++;

                    return rec;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }

            };
        }
    }

    @Override
    public Iterator<TableRecordBytes> iterator() {
        return new Iterator<TableRecordBytes>() {
            int i = 0;
            TableRecordBytes rec = info.createTableRecord();
            ImmutableBytesWritable temp = new ImmutableBytesWritable();

            @Override
            public boolean hasNext() {
                return i < nRecords;
            }

            @Override
            public TableRecordBytes next() {
                for (int col = 0; col < nColumns; col++) {
                    containers[col].getValueAt(i, temp);
                    rec.setValueBytes(col, temp);
                }
                i++;
                return rec;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((info == null) ? 0 : info.hashCode());
        result = prime * result + shard;
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Slice other = (Slice) obj;
        if (info == null) {
            if (other.info != null)
                return false;
        } else if (!info.equals(other.info))
            return false;
        if (shard != other.shard)
            return false;
        if (timestamp != other.timestamp)
            return false;
        return true;
    }

    @Override
    public int compareTo(Slice o) {
        int comp = this.shard - o.shard;
        if (comp != 0)
            return comp;

        comp = (int) (this.timestamp - o.timestamp);
        return comp;
    }

}
