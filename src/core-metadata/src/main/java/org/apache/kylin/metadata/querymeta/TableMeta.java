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

package org.apache.kylin.metadata.querymeta;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 */
public class TableMeta implements Serializable {

    private static final long serialVersionUID = 1L;
    protected String TABLE_CAT;
    protected String TABLE_SCHEM;
    protected String TABLE_NAME;
    protected String TABLE_TYPE;
    protected String REMARKS;
    protected String TYPE_CAT;
    protected String TYPE_SCHEM;
    protected String TYPE_NAME;
    protected String SELF_REFERENCING_COL_NAME;
    protected String REF_GENERATION;
    private List<ColumnMeta> columns = new ArrayList<ColumnMeta>();

    public TableMeta() {
    }

    public TableMeta(String tABLE_CAT, String tABLE_SCHEM, String tABLE_NAME, String tABLE_TYPE, String rEMARKS,
            String tYPE_CAT, String tYPE_SCHEM, String tYPE_NAME, String sELF_REFERENCING_COL_NAME,
            String rEF_GENERATION) {
        super();
        TABLE_CAT = tABLE_CAT;
        TABLE_SCHEM = tABLE_SCHEM;
        TABLE_NAME = tABLE_NAME;
        TABLE_TYPE = tABLE_TYPE;
        REMARKS = rEMARKS;
        TYPE_CAT = tYPE_CAT;
        TYPE_SCHEM = tYPE_SCHEM;
        TYPE_NAME = tYPE_NAME;
        SELF_REFERENCING_COL_NAME = sELF_REFERENCING_COL_NAME;
        REF_GENERATION = rEF_GENERATION;
    }

    public String getTABLE_CAT() {
        return TABLE_CAT;
    }

    public void setTABLE_CAT(String tABLE_CAT) {
        TABLE_CAT = tABLE_CAT;
    }

    public String getTABLE_SCHEM() {
        return TABLE_SCHEM;
    }

    public void setTABLE_SCHEM(String tABLE_SCHEM) {
        TABLE_SCHEM = tABLE_SCHEM;
    }

    public String getTABLE_NAME() {
        return TABLE_NAME;
    }

    public void setTABLE_NAME(String tABLE_NAME) {
        TABLE_NAME = tABLE_NAME;
    }

    public String getTABLE_TYPE() {
        return TABLE_TYPE;
    }

    public void setTABLE_TYPE(String tABLE_TYPE) {
        TABLE_TYPE = tABLE_TYPE;
    }

    public String getREMARKS() {
        return REMARKS;
    }

    public void setREMARKS(String rEMARKS) {
        REMARKS = rEMARKS;
    }

    public String getTYPE_CAT() {
        return TYPE_CAT;
    }

    public void setTYPE_CAT(String tYPE_CAT) {
        TYPE_CAT = tYPE_CAT;
    }

    public String getTYPE_SCHEM() {
        return TYPE_SCHEM;
    }

    public void setTYPE_SCHEM(String tYPE_SCHEM) {
        TYPE_SCHEM = tYPE_SCHEM;
    }

    public String getTYPE_NAME() {
        return TYPE_NAME;
    }

    public void setTYPE_NAME(String tYPE_NAME) {
        TYPE_NAME = tYPE_NAME;
    }

    public String getSELF_REFERENCING_COL_NAME() {
        return SELF_REFERENCING_COL_NAME;
    }

    public void setSELF_REFERENCING_COL_NAME(String sELF_REFERENCING_COL_NAME) {
        SELF_REFERENCING_COL_NAME = sELF_REFERENCING_COL_NAME;
    }

    public String getREF_GENERATION() {
        return REF_GENERATION;
    }

    public void setREF_GENERATION(String rEF_GENERATION) {
        REF_GENERATION = rEF_GENERATION;
    }

    public List<ColumnMeta> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnMeta> columns) {
        this.columns = columns;
    }

    public void addColumn(ColumnMeta column) {
        this.columns.add(column);
    }

    @Override
    public String toString() {
        List<ColumnMeta> columnMetas = new ArrayList<>(columns);
        columnMetas.sort(Comparator.comparingInt(ColumnMeta::getORDINAL_POSITION));
        return "TableMeta{" + "TABLE_CAT='" + TABLE_CAT + '\n' + ", TABLE_SCHEM='" + TABLE_SCHEM + '\n'
                + ", TABLE_NAME='" + TABLE_NAME + '\n' + ", TABLE_TYPE='" + TABLE_TYPE + '\n' + ", REMARKS='" + REMARKS
                + '\n' + ", TYPE_CAT='" + TYPE_CAT + '\n' + ", TYPE_SCHEM='" + TYPE_SCHEM + '\n' + ", TYPE_NAME='"
                + TYPE_NAME + '\n' + ", SELF_REFERENCING_COL_NAME='" + SELF_REFERENCING_COL_NAME + '\n'
                + ", REF_GENERATION='" + REF_GENERATION + '\n' + ", columns="
                + columnMetas.stream().map(ColumnMeta::toString).reduce("", (r, s) -> r + s + '\n') + '}';
    }
}
