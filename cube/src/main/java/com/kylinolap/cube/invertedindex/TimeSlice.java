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

import java.util.Arrays;
import java.util.Iterator;

/**
 * Within a partition (per timestampGranularity), records are further sliced (per sliceLength) to fit into HBASE cell.
 * 
 * @author yangli9
 */
public class TimeSlice implements Iterable<TableRecord> {

    TableRecordInfo info;
    int nColumns;
    long timePartition;
    int sliceNo;
    int nRecords;
    ColumnValueContainer[] containers;

    TimeSlice(TableRecordInfo info, long timePartition, int sliceNo, ColumnValueContainer[] containers) {
        this.info = info;
        this.nColumns = info.getColumnCount();
        this.timePartition = timePartition;
        this.sliceNo = sliceNo;
        this.nRecords = containers[0].getSize();
        this.containers = containers;

        assert nColumns == containers.length;
        for (int i = 0; i < nColumns; i++) {
            assert nRecords == containers[i].getSize();
        }
    }

    public long getTimeParititon() {
        return timePartition;
    }

    public int getSliceNo() {
        return sliceNo;
    }

    @Override
    public Iterator<TableRecord> iterator() {
        return new Iterator<TableRecord>() {
            int i = 0;
            TableRecord rec = new TableRecord(info);

            @Override
            public boolean hasNext() {
                return i < nRecords;
            }

            @Override
            public TableRecord next() {
                for (int col = 0; col < nColumns; col++) {
                    rec.setValueID(col, containers[col].getValueAt(i));
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(containers);
        result = prime * result + ((info == null) ? 0 : info.hashCode());
        result = prime * result + nColumns;
        result = prime * result + nRecords;
        result = prime * result + sliceNo;
        result = prime * result + (int) (timePartition ^ (timePartition >>> 32));
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
        TimeSlice other = (TimeSlice) obj;
        if (!Arrays.equals(containers, other.containers))
            return false;
        if (info == null) {
            if (other.info != null)
                return false;
        } else if (!info.equals(other.info))
            return false;
        if (nColumns != other.nColumns)
            return false;
        if (nRecords != other.nRecords)
            return false;
        if (sliceNo != other.sliceNo)
            return false;
        if (timePartition != other.timePartition)
            return false;
        return true;
    }

}
