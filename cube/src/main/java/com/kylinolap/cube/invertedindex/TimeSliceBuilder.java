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

import java.io.IOException;

/**
 * @author yangli9
 *
 */
public class TimeSliceBuilder {

    TableRecordInfo info;
    private int nColumns;
    int nRecordsCap;

    long curTimePartition;
    int curSliceNo;

    int nRecords;
    private ColumnValueContainer[] containers;

    public TimeSliceBuilder(TableRecordInfo info) throws IOException {
        this.info = info;
        this.nColumns = info.getColumnCount();
        this.nRecordsCap = Math.max(1, info.getDescriptor().getSliceLength() / maxDictionaryIdSize());

        this.containers = null;
        this.curTimePartition = Long.MIN_VALUE;
        this.curSliceNo = -1;
        this.nRecords = 0;
    }

    private int maxDictionaryIdSize() throws IOException {
        int max = 0;
        for (int i = 0; i < nColumns; i++) {
            max = Math.max(max, info.length(i));
        }
        return max;
    }

    private TimeSlice doneSlice() {
        TimeSlice r = null;
        if (nRecords > 0) {
            for (int i = 0; i < nColumns; i++) {
                containers[i].closeForChange();
            }
            r = new TimeSlice(info, curTimePartition, curSliceNo, containers);
        }

        // reset for next slice
        curSliceNo++;
        nRecords = 0;
        containers = new ColumnValueContainer[nColumns];
        for (int i : info.getDescriptor().getBitmapColumns()) {
            containers[i] = new BitMapContainer(info, i);
        }
        for (int i : info.getDescriptor().getValueColumns()) {
            containers[i] = new CompressedValueContainer(info, i, nRecordsCap);
        }

        return r;

    }

    // rec must be appended in time order
    public TimeSlice append(TableRecord rec) {
        TimeSlice doneSlice = null;

        if (curTimePartition != rec.getTimePartition()) {
            doneSlice = doneSlice();
            curTimePartition = rec.getTimePartition();
            curSliceNo = 0;
        } else if (isFull()) {
            doneSlice = doneSlice();
        }

        nRecords++;

        for (int i = 0; i < nColumns; i++) {
            containers[i].append(rec.getValueID(i));
        }

        return doneSlice;
    }

    public TimeSlice close() {
        TimeSlice doneSlice = doneSlice();
        this.curTimePartition = Long.MIN_VALUE;
        this.curSliceNo = -1;
        this.nRecords = 0;
        return doneSlice;
    }

    private boolean isFull() {
        return nRecords >= nRecordsCap;
    }
}
