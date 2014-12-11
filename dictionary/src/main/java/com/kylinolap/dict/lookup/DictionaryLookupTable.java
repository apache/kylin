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

package com.kylinolap.dict.lookup;

import java.io.IOException;

import com.kylinolap.dict.Dictionary;
import com.kylinolap.metadata.model.TableDesc;

/**
 * An in-memory lookup table indexed by a dictionary column. The column value
 * must be unique within the table.
 * 
 * @author yangli9
 */
public class DictionaryLookupTable {

    private static final int MAX_CARDINALITY = 1000000;

    private TableDesc tableDesc;
    private String keyCol; // whose value must be unique across table
    private Dictionary<String> dict;
    private String tablePath;

    private int keyColIndex;
    private String[][] table;

    public DictionaryLookupTable(TableDesc tableDesc, String keyCol, Dictionary<String> dict, String tablePath) throws IOException {
        this.tableDesc = tableDesc;
        this.keyCol = keyCol;
        this.dict = dict;
        this.tablePath = tablePath;
        init();
    }

    private void init() throws IOException {
        keyColIndex = tableDesc.findColumnByName(keyCol).getZeroBasedIndex();
        table = new String[dict.getMaxId() - dict.getMinId() + 1][];

        if (table.length > MAX_CARDINALITY) // 1 million
            throw new IllegalStateException("Too high cardinality of table " + tableDesc + " as an in-mem lookup: " + table.length);

        TableReader reader = new FileTable(tablePath, tableDesc.getColumnCount()).getReader();
        try {
            while (reader.next()) {
                String[] cols = reader.getRow();
                String key = cols[keyColIndex];
                int rowNo = getRowNoByValue(key);

                if (table[rowNo] != null) // dup key
                    throw new IllegalStateException("Dup key found, key=" + key + ", value1=" + toString(table[rowNo]) + ", value2=" + toString(cols));

                table[rowNo] = cols;
            }
        } finally {
            reader.close();
        }
    }

    public String[] getRow(int id) {
        return table[getRowNoByID(id)];
    }

    public String[] getRow(String key) {
        return table[getRowNoByValue(key)];
    }

    private int getRowNoByValue(String key) {
        return getRowNoByID(dict.getIdFromValue(key));
    }

    private int getRowNoByID(int id) {
        int rowNo = id - dict.getMinId();
        return rowNo;
    }

    public void dump() {
        for (int i = 0; i < table.length; i++) {
            String key = dict.getValueFromId(i + dict.getMinId());
            System.out.println(key + " => " + toString(table[i]));
        }
    }

    private String toString(String[] cols) {
        StringBuilder b = new StringBuilder();
        b.append("[");
        for (int i = 0; i < cols.length; i++) {
            if (i > 0)
                b.append(",");
            b.append(cols[i]);
        }
        b.append("]");
        return b.toString();
    }
}
