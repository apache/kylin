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
import java.util.Comparator;
import java.util.Iterator;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;

/**
 * @author yangli9
 * 
 */
public class LookupStringTable extends LookupTable<String> implements ILookupTable{

    private static final Comparator<String> dateStrComparator = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            long l1 = Long.parseLong(o1);
            long l2 = Long.parseLong(o2);
            return Long.compare(l1, l2);
        }
    };

    private static final Comparator<String> numStrComparator = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            double d1 = Double.parseDouble(o1);
            double d2 = Double.parseDouble(o2);
            return Double.compare(d1, d2);
        }
    };

    private static final Comparator<String> defaultStrComparator = new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    };

    boolean[] colIsDateTime;
    boolean[] colIsNumber;

    public LookupStringTable(TableDesc tableDesc, String[] keyColumns, IReadableTable table) throws IOException {
        super(tableDesc, keyColumns, table);
    }

    @Override
    protected void init() throws IOException {
        ColumnDesc[] cols = tableDesc.getColumns();
        colIsDateTime = new boolean[cols.length];
        colIsNumber = new boolean[cols.length];
        for (int i = 0; i < cols.length; i++) {
            DataType t = cols[i].getType();
            colIsDateTime[i] = t.isDateTimeFamily();
            colIsNumber[i] = t.isNumberFamily();
        }

        super.init();
    }

    @Override
    protected String[] convertRow(String[] cols) {
        for (int i = 0; i < cols.length; i++) {
            if (colIsDateTime[i]) {
                if (cols[i] != null) {
                    if (cols[i].isEmpty())
                        cols[i] = null;
                    else
                        cols[i] = String.valueOf(DateFormat.stringToMillis(cols[i]));
                }
            }
        }
        return cols;
    }

    @Override
    protected Comparator<String> getComparator(int idx) {
        if (colIsDateTime[idx])
            return dateStrComparator;
        else if (colIsNumber[idx])
            return numStrComparator;
        else
            return defaultStrComparator;
    }

    @Override
    protected String toString(String cell) {
        return cell;
    }

    public Class<?> getType() {
        return String.class;
    }

    @Override
    public Iterator<String[]> iterator() {
        return data.values().iterator();
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
