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

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class JoinDesc {

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
        this.primaryKeyColumns = primaryKeyColumns;
    }

    public TblColRef[] getForeignKeyColumns() {
        return foreignKeyColumns;
    }

    public void setForeignKeyColumns(TblColRef[] foreignKeyColumns) {
        this.foreignKeyColumns = foreignKeyColumns;
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

        if (!this.columnsEqualIgnoringOrder(foreignKeyColumns, other.foreignKeyColumns))
            return false;
        if (!this.columnsEqualIgnoringOrder(primaryKeyColumns, other.primaryKeyColumns))
            return false;

        if (!this.type.equalsIgnoreCase(other.getType()))
            return false;
        return true;
    }

    private boolean columnsEqualIgnoringOrder(TblColRef[] a, TblColRef[] b) {
        if (a.length != b.length)
            return false;

        return Arrays.asList(a).containsAll(Arrays.asList(b));
    }

    @Override
    public String toString() {
        return "JoinDesc [type=" + type + ", primary_key=" + Arrays.toString(primaryKey) + ", foreign_key=" + Arrays.toString(foreignKey) + "]";
    }

}
