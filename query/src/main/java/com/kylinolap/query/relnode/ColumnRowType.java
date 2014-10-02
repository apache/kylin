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
package com.kylinolap.query.relnode;

import com.kylinolap.metadata.model.cube.TblColRef;

import java.util.List;

/**
 * @author xjiang
 */
public class ColumnRowType {

    private List<TblColRef> columns;

    public ColumnRowType(List<TblColRef> columns) {
        this.columns = columns;
    }

    public TblColRef getColumnByIndex(int index) {
        return columns.get(index);
    }

    public int getIndexByName(String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(columnName)) {
                return i;
            }
        }
        return -1;
    }

    public List<TblColRef> getAllColumns() {
        return columns;
    }

    public int size() {
        return columns.size();
    }

    public void appendColumn(TblColRef column) {
        this.columns.add(column);
    }

    @Override
    public String toString() {
        return "ColumnRowType [" + columns + "]";
    }
}
