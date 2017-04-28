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

package org.apache.kylin.metadata.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Arrays;

/**
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class JoinDesc implements Serializable {

    // inner, left, right, outer...
    @JsonProperty("type")
    private String type;
    @JsonProperty("primary_key")
    private String[] primaryKey;
    @JsonProperty("foreign_key")
    private String[] foreignKey;

    private TblColRef[] primaryKeyColumns;
    private TblColRef[] foreignKeyColumns;

    public void swapPKFK() {
        String[] t = primaryKey;
        primaryKey = foreignKey;
        foreignKey = t;

        TblColRef[] tt = primaryKeyColumns;
        primaryKeyColumns = foreignKeyColumns;
        foreignKeyColumns = tt;
    }

    public boolean isInnerJoin() {
        return "INNER".equalsIgnoreCase(type);
    }
    
    public boolean isLeftJoin() {
        return "LEFT".equalsIgnoreCase(type);
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }

    public String[] getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String[] primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String[] getForeignKey() {
        return foreignKey;
    }

    public void setForeignKey(String[] foreignKey) {
        this.foreignKey = foreignKey;
    }

    public TblColRef[] getPrimaryKeyColumns() {
        return primaryKeyColumns;
    }

    public void setPrimaryKeyColumns(TblColRef[] primaryKeyColumns) {
        checkSameTable(primaryKeyColumns);
        this.primaryKeyColumns = primaryKeyColumns;
    }

    public TblColRef[] getForeignKeyColumns() {
        return foreignKeyColumns;
    }

    public void setForeignKeyColumns(TblColRef[] foreignKeyColumns) {
        checkSameTable(primaryKeyColumns);
        this.foreignKeyColumns = foreignKeyColumns;
    }

    private void checkSameTable(TblColRef[] cols) {
        if (cols == null || cols.length == 0)
            return;
        
        TableRef tableRef = cols[0].getTableRef();
        for (int i = 1; i < cols.length; i++)
            Preconditions.checkState(tableRef == cols[i].getTableRef());
    }

    public TableRef getPKSide() {
        return primaryKeyColumns[0].getTableRef();
    }
    
    public TableRef getFKSide() {
        return foreignKeyColumns[0].getTableRef();
    }

    public void sortByFK() {
        Preconditions.checkState(primaryKey.length == foreignKey.length && primaryKey.length == primaryKeyColumns.length && foreignKey.length == foreignKeyColumns.length);
        boolean cont = true;
        int n = foreignKey.length;
        for (int i = 0; i < n - 1 && cont; i++) {
            cont = false;
            for (int j = i; j < n - 1; j++) {
                int jj = j + 1;
                if (foreignKey[j].compareTo(foreignKey[jj]) > 0) {
                    swap(foreignKey, j, jj);
                    swap(primaryKey, j, jj);
                    swap(foreignKeyColumns, j, jj);
                    swap(primaryKeyColumns, j, jj);
                    cont = true;
                }
            }
        }
    }

    private void swap(String[] arr, int j, int jj) {
        String tmp = arr[j];
        arr[j] = arr[jj];
        arr[jj] = tmp;
    }
    
    private void swap(TblColRef[] arr, int j, int jj) {
        TblColRef tmp = arr[j];
        arr[j] = arr[jj];
        arr[jj] = tmp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(primaryKeyColumns);
        result = prime * result + Arrays.hashCode(foreignKeyColumns);
        result = prime * result + this.type.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        JoinDesc other = (JoinDesc) obj;

        // note pk/fk are sorted, sortByFK()
        if (!Arrays.equals(foreignKeyColumns, other.foreignKeyColumns))
            return false;
        if (!Arrays.equals(primaryKeyColumns, other.primaryKeyColumns))
            return false;

        if (!this.type.equalsIgnoreCase(other.getType()))
            return false;
        return true;
    }

    // equals() without alias
    public boolean matches(JoinDesc other) {
        if (other == null)
            return false;
        
        if (!this.type.equalsIgnoreCase(other.getType()))
            return false;
        
        // note pk/fk are sorted, sortByFK()
        if (!this.columnDescEquals(foreignKeyColumns, other.foreignKeyColumns))
            return false;
        if (!this.columnDescEquals(primaryKeyColumns, other.primaryKeyColumns))
            return false;
        
        return true;
    }

    private boolean columnDescEquals(TblColRef[] a, TblColRef[] b) {
        if (a.length != b.length)
            return false;
        
        for (int i = 0; i < a.length; i++) {
            if (a[i].getColumnDesc().equals(b[i].getColumnDesc()) == false)
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "JoinDesc [type=" + type + ", primary_key=" + Arrays.toString(primaryKey) + ", foreign_key=" + Arrays.toString(foreignKey) + "]";
    }

}
