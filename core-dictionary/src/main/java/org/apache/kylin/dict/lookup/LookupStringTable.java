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

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.ReadableTable;

/**
 * @author yangli9
 * 
 */
public class LookupStringTable extends LookupTable<String> {

    int[] keyIndexOfDates;
    
    public LookupStringTable(TableDesc tableDesc, String[] keyColumns, ReadableTable table) throws IOException {
        super(tableDesc, keyColumns, table);
    }

    @Override
    protected String[] convertRow(String[] cols) {
        if (keyIndexOfDates == null) {
            keyIndexOfDates = new int[keyColumns.length];
            for (int i = 0; i < keyColumns.length; i++) {
                ColumnDesc col = tableDesc.findColumnByName(keyColumns[i]);
                keyIndexOfDates[i] = col.getType().isDateTimeFamily() ? col.getZeroBasedIndex() : -1;
            }
        }
        
        for (int i = 0; i < keyIndexOfDates.length; i++) {
            int c = keyIndexOfDates[i];
            if (c >= 0)
                cols[c] = String.valueOf(DateFormat.stringToMillis(cols[c]));
        }
        return cols;
    }

    @Override
    protected String toString(String cell) {
        return cell;
    }

    public Class<?> getType() {
        return String.class;
    }
}
