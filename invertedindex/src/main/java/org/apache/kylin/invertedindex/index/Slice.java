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

package org.apache.kylin.invertedindex.index;

import com.google.common.base.Objects;
import it.uniroma3.mat.extendedset.intset.ConciseSet;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.dict.Dictionary;

import java.util.Iterator;
import java.util.Map;

/**
 * Within a partition (per timestampGranularity), records are further sliced
 * (per sliceLength) to fit into HBASE cell.
 * 
 * @author yangli9
 */
public class Slice implements Iterable<RawTableRecord>, Comparable<Slice> {

    final TableRecordInfoDigest info;
    final int nColumns;

    final short shard;
    final long timestamp;
    final int nRecords;
    final ColumnValueContainer[] containers;
    private Dictionary<?>[] localDictionaries;

    public Slice(TableRecordInfoDigest digest, short shard, long timestamp, ColumnValueContainer[] containers) {
        this.info = digest;
        this.nColumns = digest.getColumnCount();

        this.shard = shard;
        this.timestamp = timestamp;
        this.nRecords = containers[0].getSize();
        this.containers = containers;

        assert nColumns == containers.length;
        for (int i = 0; i < nColumns; i++) {
            assert nRecords == containers[i].getSize();
        }
    }

    public Dictionary<?>[] getLocalDictionaries() {
        return localDictionaries;
    }

    public void setLocalDictionaries(Dictionary<?>[] localDictionaries) {
        this.localDictionaries = localDictionaries;
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

    public ColumnValueContainer[] getColumnValueContainers() {
        return containers;
    }

    public ColumnValueContainer getColumnValueContainer(int col) {
        return containers[col];
    }

    public Iterator<RawTableRecord> iterateWithBitmap(final ConciseSet resultBitMap) {
        if (resultBitMap == null) {
            return this.iterator();
        } else {
            final RawTableRecord rec = info.createTableRecordBytes();
            final ImmutableBytesWritable temp = new ImmutableBytesWritable();

            return new Iterator<RawTableRecord>() {
                int i = 0;
                int iteratedCount = 0;
                int resultSize = resultBitMap.size();

                @Override
                public boolean hasNext() {
                    return iteratedCount < resultSize;
                }

                @Override
                public RawTableRecord next() {
                    while (!resultBitMap.contains(i)) {
                        i++;
                    }
                    for (int col = 0; col < nColumns; col++) {
                        containers[col].getValueAt(i, temp);
                        rec.setValueBytes(col, temp);
                    }
                    iteratedCount++;
                    i++;

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
    public Iterator<RawTableRecord> iterator() {
        return new Iterator<RawTableRecord>() {
            int i = 0;
            RawTableRecord rec = info.createTableRecordBytes();
            ImmutableBytesWritable temp = new ImmutableBytesWritable();

            @Override
            public boolean hasNext() {
                return i < nRecords;
            }

            @Override
            public RawTableRecord next() {
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

    /*
     * (non-Javadoc)
     * 
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

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Slice other = (Slice) obj;
        if (shard != other.shard) {
            return false;
        }
        if (timestamp != other.timestamp) {
            return false;
        }
        return Objects.equal(info, other.info);
    }

    @Override
    public int compareTo(Slice o) {
        int comp = this.shard - o.shard;
        if (comp != 0)
            return comp;

        comp = (int) (this.timestamp - o.timestamp);
        return comp;
    }

    public TableRecordInfoDigest getInfo() {
        return info;
    }
}
