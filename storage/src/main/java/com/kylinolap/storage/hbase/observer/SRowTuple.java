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

package com.kylinolap.storage.hbase.observer;

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.kylinolap.dict.Dictionary;
import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.storage.tuple.ITuple;

/**
 * A special kind of tuple that exposes column value (dictionary ID) directly on
 * top of row key.
 * 
 * @author yangli9
 */
public class SRowTuple implements ITuple {

    final SRowType type;

    ImmutableBytesWritable rowkey;
    String[] values;

    public SRowTuple(SRowType type) {
        this.type = type;
        this.rowkey = new ImmutableBytesWritable();
        this.values = new String[type.getColumnCount()];
    }

    public void setUnderlying(byte[] array, int offset, int length) {
        rowkey.set(array, offset, length);
        for (int i = 0; i < values.length; i++) {
            values[i] = null;
        }
    }

    @Override
    public List<TblColRef> getAllColumns() {
        return type.columnsAsList;
    }

    @Override
    public Object[] getAllValues() {
        int n = type.getColumnCount();
        for (int i = 0; i < n; i++) {
            getValueAt(i);
        }
        return values;
    }

    private String getValueAt(int i) {
        int n = type.getColumnCount();
        if (i < 0 || i >= n)
            return null;

        if (values[i] == null) {
            values[i] = Dictionary.dictIdToString(rowkey.get(), rowkey.getOffset() + type.columnOffsets[i], type.columnSizes[i]);
        }

        return values[i];
    }

    @Override
    public Object getValue(TblColRef col) {
        int i = type.columnIdxMap.get(col);
        return getValueAt(i);
    }

    @Override
    public List<String> getAllFields() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getValue(String field) {
        throw new UnsupportedOperationException();
    }

}
