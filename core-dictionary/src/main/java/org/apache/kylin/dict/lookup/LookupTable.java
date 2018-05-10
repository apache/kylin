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

package org.apache.kylin.dict.lookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.IReadableTable.TableReader;

import com.google.common.collect.Sets;

/**
 * An in-memory lookup table, in which each cell is an object of type T. The
 * table is indexed by specified PK for fast lookup.
 *
 * @author yangli9
 */
abstract public class LookupTable<T> {

    protected TableDesc tableDesc;
    protected String[] keyColumns;
    protected IReadableTable table;
    protected Map<Array<T>, T[]> data;

    public LookupTable(TableDesc tableDesc, String[] keyColumns, IReadableTable table) throws IOException {
        this.tableDesc = tableDesc;
        this.keyColumns = keyColumns;
        this.table = table;
        this.data = new HashMap<Array<T>, T[]>();
        init();
    }

    protected void init() throws IOException {
        int[] keyIndex = new int[keyColumns.length];
        for (int i = 0; i < keyColumns.length; i++) {
            keyIndex[i] = tableDesc.findColumnByName(keyColumns[i]).getZeroBasedIndex();
        }

        TableReader reader = table.getReader();
        try {
            while (reader.next()) {
                initRow(reader.getRow(), keyIndex);
            }
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }

    @SuppressWarnings("unchecked")
    private void initRow(String[] cols, int[] keyIndex) {
        T[] value = convertRow(cols);
        T[] keyCols = (T[]) java.lang.reflect.Array.newInstance(getType(), keyIndex.length);
        for (int i = 0; i < keyCols.length; i++)
            keyCols[i] = value[keyIndex[i]];

        Array<T> key = new Array<T>(keyCols);

        if (data.containsKey(key))
            throw new IllegalStateException("The table: " + tableDesc.getName() + " Dup key found, key=" + toString(keyCols) + ", value1=" + toString(data.get(key)) + ", value2=" + toString(value));

        data.put(key, value);
    }

    abstract protected T[] convertRow(String[] cols);

    public T[] getRow(Array<T> key) {
        return data.get(key);
    }

    public Collection<T[]> getAllRows() {
        return data.values();
    }

    public List<T> scan(String col, List<T> values, String returnCol) {
        ArrayList<T> result = new ArrayList<T>();
        int colIdx = tableDesc.findColumnByName(col).getZeroBasedIndex();
        int returnIdx = tableDesc.findColumnByName(returnCol).getZeroBasedIndex();
        for (T[] row : data.values()) {
            if (values.contains(row[colIdx]))
                result.add(row[returnIdx]);
        }
        return result;
    }

    public Pair<T, T> mapRange(String col, T beginValue, T endValue, String returnCol) {
        int colIdx = tableDesc.findColumnByName(col).getZeroBasedIndex();
        int returnIdx = tableDesc.findColumnByName(returnCol).getZeroBasedIndex();
        Comparator<T> colComp = getComparator(colIdx);
        Comparator<T> returnComp = getComparator(returnIdx);

        T returnBegin = null;
        T returnEnd = null;
        for (T[] row : data.values()) {
            if (between(beginValue, row[colIdx], endValue, colComp)) {
                T returnValue = row[returnIdx];
                if (returnBegin == null || returnComp.compare(returnValue, returnBegin) < 0) {
                    returnBegin = returnValue;
                }
                if (returnEnd == null || returnComp.compare(returnValue, returnEnd) > 0) {
                    returnEnd = returnValue;
                }
            }
        }
        if (returnBegin == null && returnEnd == null)
            return null;
        else
            return Pair.newPair(returnBegin, returnEnd);
    }

    public Set<T> mapValues(String col, Set<T> values, String returnCol) {
        int colIdx = tableDesc.findColumnByName(col).getZeroBasedIndex();
        int returnIdx = tableDesc.findColumnByName(returnCol).getZeroBasedIndex();
        Set<T> result = Sets.newHashSetWithExpectedSize(values.size());
        for (T[] row : data.values()) {
            if (values.contains(row[colIdx])) {
                result.add(row[returnIdx]);
            }
        }
        return result;
    }

    private boolean between(T beginValue, T v, T endValue, Comparator<T> comp) {
        return (beginValue == null || comp.compare(beginValue, v) <= 0) && (endValue == null || comp.compare(v, endValue) <= 0);
    }

    abstract protected Comparator<T> getComparator(int colIdx);

    public String toString() {
        return "LookupTable [path=" + table + "]";
    }

    protected String toString(T[] cols) {
        StringBuilder b = new StringBuilder();
        b.append("[");
        for (int i = 0; i < cols.length; i++) {
            if (i > 0)
                b.append(",");
            b.append(toString(cols[i]));
        }
        b.append("]");
        return b.toString();
    }

    abstract protected String toString(T cell);

    abstract public Class<?> getType();

    public void dump() {
        for (Array<T> key : data.keySet()) {
            System.out.println(toString(key.data) + " => " + toString(data.get(key)));
        }
    }

}
