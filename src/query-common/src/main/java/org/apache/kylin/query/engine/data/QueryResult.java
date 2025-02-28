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

package org.apache.kylin.query.engine.data;

import java.util.LinkedList;
import java.util.List;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.metadata.query.StructField;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.query.engine.exec.ExecuteResult;

public class QueryResult {

    private Iterable<List<String>> rows;
    private List<List<String>> rowsList; // save the rows iteratored
    private int size;

    private List<StructField> columns;
    private List<SelectedColumnMeta> columnMetas;

    public QueryResult() {
        this(new LinkedList<>(), 0, new LinkedList<>());
    }

    public QueryResult(ExecuteResult result, List<StructField> columns) {
        this.rows = result.getRows();
        this.size = result.getSize();
        this.columns = columns;
    }

    public QueryResult(Iterable<List<String>> rows, int size, List<StructField> columns) {
        this.rows = rows;
        this.columns = columns;
        this.size = size;
    }

    public QueryResult(Iterable<List<String>> rows, int size, List<StructField> columns,
            List<SelectedColumnMeta> columnMetas) {
        this.rows = rows;
        this.size = size;
        this.columns = columns;
        this.columnMetas = columnMetas;
    }

    /**
     * This method is generally supposed to be used for testing only
     * @return
     * @deprecated
     */
    @Deprecated
    public List<List<String>> getRows() {
        if (rowsList == null) {
            rowsList = ImmutableList.copyOf(rows);
        }
        return rowsList;
    }

    public Iterable<List<String>> getRowsIterable() {
        return rows;
    }

    public int getSize() {
        return size;
    }

    public List<StructField> getColumns() {
        return columns;
    }

    public List<SelectedColumnMeta> getColumnMetas() {
        if (columnMetas != null) {
            return columnMetas;
        }
        columnMetas = new LinkedList<>();
        int columnCount = this.columns.size();
        List<StructField> fieldList = this.columns;

        // fill in selected column meta
        for (int i = 0; i < columnCount; ++i) {
            int nullable = fieldList.get(i).isNullable() ? 1 : 0;
            columnMetas.add(new SelectedColumnMeta(false, false, false, false, nullable, true,
                    fieldList.get(i).getPrecision(), fieldList.get(i).getName(), fieldList.get(i).getName(), null, null,
                    null, fieldList.get(i).getPrecision(), fieldList.get(i).getScale(), fieldList.get(i).getDataType(),
                    fieldList.get(i).getDataTypeName(), false, false, false));
        }
        return columnMetas;
    }
}
