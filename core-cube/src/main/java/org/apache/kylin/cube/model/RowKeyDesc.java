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

package org.apache.kylin.cube.model;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class RowKeyDesc {

    @JsonProperty("rowkey_columns")
    private RowKeyColDesc[] rowkeyColumns;

    // computed content
    private long fullMask;
    private CubeDesc cubeDesc;
    private Map<TblColRef, RowKeyColDesc> columnMap;

    public RowKeyColDesc[] getRowKeyColumns() {
        return rowkeyColumns;
    }

    public void setCubeDesc(CubeDesc cubeRef) {
        this.cubeDesc = cubeRef;
    }

    public int getColumnBitIndex(TblColRef col) {
        return getColDesc(col).getBitIndex();
    }

    public RowKeyColDesc getColDesc(TblColRef col) {
        RowKeyColDesc desc = columnMap.get(col);
        if (desc == null)
            throw new NullPointerException("Column " + col + " does not exist in row key desc");
        return desc;
    }

    public boolean isUseDictionary(TblColRef col) {
        return getColDesc(col).isUsingDictionary();
    }

    public void init(CubeDesc cubeDesc) {

        setCubeDesc(cubeDesc);
        Map<String, TblColRef> colNameAbbr = cubeDesc.buildColumnNameAbbreviation();

        buildRowKey(colNameAbbr);
    }

    public void setRowkeyColumns(RowKeyColDesc[] rowkeyColumns) {
        this.rowkeyColumns = rowkeyColumns;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("RowKeyColumns", Arrays.toString(rowkeyColumns)).toString();
    }

    private void buildRowKey(Map<String, TblColRef> colNameAbbr) {
        columnMap = new HashMap<TblColRef, RowKeyColDesc>();

        for (int i = 0; i < rowkeyColumns.length; i++) {
            RowKeyColDesc rowKeyColDesc = rowkeyColumns[i];
            rowKeyColDesc.init();
            String column = rowKeyColDesc.getColumn();
            rowKeyColDesc.setColumn(column.toUpperCase());
            rowKeyColDesc.setBitIndex(rowkeyColumns.length - i - 1);
            rowKeyColDesc.setColRef(colNameAbbr.get(column));
            if (rowKeyColDesc.getColRef() == null) {
                throw new IllegalArgumentException("Cannot find rowkey column " + column + " in cube " + cubeDesc);
            }

            columnMap.put(rowKeyColDesc.getColRef(), rowKeyColDesc);
        }

        this.fullMask = 0L;
        for (int i = 0; i < this.rowkeyColumns.length; i++) {
            int index = rowkeyColumns[i].getBitIndex();
            this.fullMask |= 1L << index;
        }
    }

    public long getFullMask() {
        return this.fullMask;
    }

}
