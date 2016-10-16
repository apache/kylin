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

package org.apache.kylin.query.relnode;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kylin.metadata.model.TblColRef;

/**
 * 
 * @author xjiang
 * 
 */
public class ColumnRowType {

    private List<TblColRef> columns;
    // for calculated column, like (CASE LSTG_FORMAT_NAME WHEN 'Auction' THEN '111' ELSE '222' END)
    // source columns are the contributing physical columns, here the LSTG_FORMAT_NAME
    private List<Set<TblColRef>> sourceColumns;

    public ColumnRowType(List<TblColRef> columns) {
        this(columns, null);
    }

    public ColumnRowType(List<TblColRef> columns, List<Set<TblColRef>> sourceColumns) {
        this.columns = columns;
        this.sourceColumns = sourceColumns;
    }

    public TblColRef getColumnByIndex(int index) {
        return columns.get(index);
    }

    public TblColRef getColumnByIndexNullable(int index) {
        if (index < 0 || index >= columns.size())
            return null;
        else
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

    public Set<TblColRef> getSourceColumnsByIndex(int i) {
        Set<TblColRef> result = null;
        if (sourceColumns != null) {
            result = sourceColumns.get(i);
        }
        if (result == null || result.isEmpty()) {
            result = Collections.singleton(getColumnByIndex(i));
        }
        return result;
    }

    public List<TblColRef> getAllColumns() {
        return columns;
    }

    public int size() {
        return columns.size();
    }

    @Override
    public String toString() {
        return "ColumnRowType [" + columns + "]";
    }

}
