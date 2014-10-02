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
package com.kylinolap.storage.filter;

import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.metadata.model.schema.ColumnDesc;
import com.kylinolap.metadata.model.schema.TableDesc;
import com.kylinolap.storage.tuple.Tuple;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author xjiang
 */
public class ColumnTupleFilter extends TupleFilter {

    private TblColRef columnRef;
    private Object tupleValue;
    private List<String> values;

    public ColumnTupleFilter(TblColRef column) {
        super(Collections.<TupleFilter>emptyList(), FilterOperatorEnum.COLUMN);
        this.columnRef = column;
        this.values = new ArrayList<String>(1);
        this.values.add(null);
    }

    public TblColRef getColumn() {
        return columnRef;
    }

    @Override
    public void addChild(TupleFilter child) {
        throw new UnsupportedOperationException("This is " + this + " and child is " + child);
    }

    @Override
    public String toString() {
        return "ColumnFilter [column=" + columnRef + "]";
    }

    @Override
    public boolean evaluate(Tuple tuple) {
        String fieldName = tuple.getFieldName(columnRef);
        this.tupleValue = tuple.getFieldValue(fieldName);
        return true;
    }

    @Override
    public boolean isEvaluable() {
        return true;
    }

    @Override
    public Collection<String> getValues() {
        this.values.set(0, (String) this.tupleValue);
        return this.values;
    }

    @Override
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        String table = columnRef.getTable();
        if (table == null) {
            BytesUtil.writeByteArray(new byte[0], buffer);
        } else {
            BytesUtil.writeByteArray(table.getBytes(Charset.forName("utf-8")), buffer);
        }

        String columnName = columnRef.getName();
        if (columnName == null) {
            BytesUtil.writeByteArray(new byte[0], buffer);
        } else {
            BytesUtil.writeByteArray(columnName.getBytes(Charset.forName("utf-8")), buffer);
        }

        String dataType = columnRef.getDatatype();
        if (dataType == null) {
            BytesUtil.writeByteArray(new byte[0], buffer);
        } else {
            BytesUtil.writeByteArray(dataType.getBytes(Charset.forName("utf-8")), buffer);
        }

        byte[] result = new byte[buffer.position()];
        System.arraycopy(buffer.array(), 0, result, 0, buffer.position());
        return result;
    }

    @Override
    public void deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        TableDesc table = new TableDesc();

        byte[] tableBytes = BytesUtil.readByteArray(buffer);
        if (tableBytes.length == 0) {
            table.setName(null);
        } else {
            String tableName = new String(tableBytes, Charset.forName("utf-8"));
            table.setName(tableName);
        }

        ColumnDesc column = new ColumnDesc();
        column.setTable(table);

        byte[] columnBytes = BytesUtil.readByteArray(buffer);
        if (columnBytes.length == 0) {
            column.setName(null);
        } else {
            String columnName = new String(columnBytes, Charset.forName("utf-8"));
            column.setName(columnName);
        }

        byte[] typeBytes = BytesUtil.readByteArray(buffer);
        if (typeBytes.length == 0) {
            column.setDatatype(null);
        } else {
            String dataType = new String(typeBytes, Charset.forName("utf-8"));
            column.setDatatype(dataType);
        }
        this.columnRef = new TblColRef(column);
    }
}
