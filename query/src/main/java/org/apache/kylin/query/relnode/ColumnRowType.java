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

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.expression.ColumnTupleExpression;
import org.apache.kylin.metadata.expression.NoneTupleExpression;
import org.apache.kylin.metadata.expression.TupleExpression;
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
    private List<TupleExpression> sourceColumns;

    public ColumnRowType(List<TblColRef> columns) {
        this(columns, null);
    }

    public ColumnRowType(List<TblColRef> columns, List<TupleExpression> sourceColumns) {
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

    public TblColRef getColumnByName(String columnName) {
        return getColumnByIndexNullable(getIndexByName(columnName));
    }

    public int getIndexByName(String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(columnName)) {
                return i;
            }
        }
        return -1;
    }

    public Pair<TblColRef, TupleExpression> replaceColumnByIndex(int index, TblColRef newColumn,
            TupleExpression newTupleExpr) {
        if (index < 0 || index >= columns.size()) {
            return null;
        }
        TblColRef oldCol = columns.set(index, newColumn);
        TupleExpression oldExpr = sourceColumns.set(index, newTupleExpr);
        return new Pair<>(oldCol, oldExpr);
    }

    public TupleExpression getTupleExpressionByIndex(int i) {
        TupleExpression result = null;
        if (sourceColumns != null) {
            result = sourceColumns.get(i);
        }
        if (result == null || result instanceof NoneTupleExpression) {
            result = new ColumnTupleExpression(getColumnByIndex(i));
        }
        return result;
    }

    public List<TupleExpression> getSourceColumns() {
        if (sourceColumns == null) {
            List<TupleExpression> sources = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                sources.add(getTupleExpressionByIndex(i));
            }
            sourceColumns = sources;
        }
        return sourceColumns;
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
