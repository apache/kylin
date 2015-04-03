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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.model.TableDesc;

/**
 * @author yangli9
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class SnapshotTable extends RootPersistentEntity implements ReadableTable {

    @JsonProperty("signature")
    private TableSignature signature;
    @JsonProperty("column_delimeter")
    private String columnDelimeter;

    private ArrayList<String[]> rows;

    // default constructor for JSON serialization
    public SnapshotTable() {
    }

    SnapshotTable(ReadableTable table) throws IOException {
        this.signature = table.getSignature();
        this.columnDelimeter = table.getColumnDelimeter();
    }

    public void takeSnapshot(ReadableTable table, TableDesc tableDesc) throws IOException {
        this.signature = table.getSignature();
        this.columnDelimeter = table.getColumnDelimeter();

        int maxIndex = tableDesc.getMaxColumnIndex();

        TableReader reader = table.getReader();
        ArrayList<String[]> allRows = new ArrayList<String[]>();
        while (reader.next()) {
            String[] row = reader.getRow();
            if (row.length <= maxIndex) {
                throw new IllegalStateException("Bad hive table row, " + tableDesc + " expect " + (maxIndex + 1) + " columns, but got " + Arrays.toString(row));
            }
            allRows.add(row);
        }
        this.rows = allRows;
    }

    public String getResourcePath() {
        return ResourceStore.SNAPSHOT_RESOURCE_ROOT + "/" + new Path(signature.getPath()).getName() + "/" + uuid + ".snapshot";
    }

    public String getResourceDir() {
        return ResourceStore.SNAPSHOT_RESOURCE_ROOT + "/" + new Path(signature.getPath()).getName();
    }

    @Override
    public TableReader getReader() throws IOException {
        return new TableReader() {

            int i = -1;

            @Override
            public boolean next() throws IOException {
                i++;
                return i < rows.size();
            }

            @Override
            public String[] getRow() {
                return rows.get(i);
            }

            @Override
            public void close() throws IOException {
            }

            @Override
            public void setExpectedColumnNumber(int expectedColumnNumber) {
                // noop
            }
        };
    }

    @Override
    public TableSignature getSignature() throws IOException {
        return signature;
    }

    @Override
    public String getColumnDelimeter() throws IOException {
        return columnDelimeter;
    }

    /**
     * a naive implementation
     *
     * @return
     */
    @Override
    public int hashCode() {
        int[] parts = new int[this.rows.size()];
        for (int i = 0; i < parts.length; ++i)
            parts[i] = Arrays.hashCode(this.rows.get(i));
        return Arrays.hashCode(parts);
    }

    @Override
    public boolean equals(Object o) {
        if ((o instanceof SnapshotTable) == false)
            return false;
        SnapshotTable that = (SnapshotTable) o;

        //compare row by row
        if (this.rows.size() != that.rows.size())
            return false;
        for (int i = 0; i < this.rows.size(); ++i) {
            if (!ArrayUtils.isEquals(this.rows.get(i), that.rows.get(i)))
                return false;
        }
        return true;
    }

    private static String NULL_STR;
    {
        try {
            // a special placeholder to indicate a NULL; 0, 1, 9, 127 are a few invisible ASCII characters
            NULL_STR = new String(new byte[] { 0, 1, 9, 127 }, "ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            // does not happen
        }
    }

    void writeData(DataOutput out) throws IOException {
        out.writeInt(rows.size());
        if (rows.size() > 0) {
            int n = rows.get(0).length;
            out.writeInt(n);
            for (int i = 0; i < rows.size(); i++) {
                String[] row = rows.get(i);
                for (int j = 0; j < n; j++) {
                    // NULL_STR is tricky, but we don't want to break the current snapshots
                    out.writeUTF(row[j] == null ? NULL_STR : row[j]);
                }
            }
        }
    }

    void readData(DataInput in) throws IOException {
        int rowNum = in.readInt();
        rows = new ArrayList<String[]>(rowNum);
        if (rowNum > 0) {
            int n = in.readInt();
            for (int i = 0; i < rowNum; i++) {
                String[] row = new String[n];
                rows.add(row);
                for (int j = 0; j < n; j++) {
                    row[j] = in.readUTF();
                    // NULL_STR is tricky, but we don't want to break the current snapshots
                    if (row[j].equals(NULL_STR))
                        row[j] = null;
                }
            }
        }
    }

}
