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

package org.apache.kylin.storage.hbase.cube.v1.coprocessor.observer;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorRowType;

/**
 * A special kind of tuple that exposes column value (dictionary ID) directly on
 * top of row key.
 *
 * @author yangli9
 */
public class ObserverTuple implements IEvaluatableTuple {

    final CoprocessorRowType type;

    ImmutableBytesWritable rowkey;
    String[] values;

    public ObserverTuple(CoprocessorRowType type) {
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
        int i = type.getColIndexByTblColRef(col);
        return getValueAt(i);
    }

}
