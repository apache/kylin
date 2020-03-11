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

import static org.apache.kylin.shaded.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.expression.TupleExpression;

/**
 */
@SuppressWarnings({ "serial" })
public class TblColRef implements Serializable {

    private static final String INNER_TABLE_NAME = "_kylin_table";
    private static final DataModelDesc UNKNOWN_MODEL = new DataModelDesc();

    static {
        UNKNOWN_MODEL.setName("UNKNOWN_MODEL");
    }

    private TableRef table;
    private TableRef backupTable;// only used in fixTableRef()
    private ColumnDesc column;
    private String identity;
    private String parserDescription;
    //used in window function
    private List<TupleExpression> subTupleExps;
    /**
     * Function used to get quoted identitier
     */
    private transient Function<TblColRef, String> quotedFunc;

    TblColRef(ColumnDesc column) {
        this.column = column;
    }

    TblColRef(TableRef table, ColumnDesc column) {
        checkArgument(table.getTableDesc().getIdentity().equals(column.getTable().getIdentity()));
        this.table = table;
        this.column = column;
    }

    // ============================================================================

    // used by projection rewrite, see OLAPProjectRel
    public static TblColRef newInnerColumn(String columnName, InnerDataTypeEnum dataType) {
        return newInnerColumn(columnName, dataType, null);
    }

    // used by projection rewrite, see OLAPProjectRel
    public static TblColRef newInnerColumn(String columnName, InnerDataTypeEnum dataType, String parserDescription) {
        ColumnDesc column = new ColumnDesc();
        column.setName(columnName);
        TableDesc table = new TableDesc();
        column.setTable(table);
        TblColRef colRef = new TblColRef(column);
        colRef.markInnerColumn(dataType);
        colRef.parserDescription = parserDescription;
        return colRef;
    }

    public static TableRef tableForUnknownModel(String tempTableAlias, TableDesc table) {
        return new TableRef(UNKNOWN_MODEL, tempTableAlias, table, false);
    }

    public static TblColRef columnForUnknownModel(TableRef table, ColumnDesc colDesc) {
        checkArgument(table.getModel() == UNKNOWN_MODEL);
        return new TblColRef(table, colDesc);
    }

    public static void fixUnknownModel(DataModelDesc model, String alias, TblColRef col) {
        checkArgument(col.table.getModel() == UNKNOWN_MODEL || col.table.getModel() == model);
        TableRef tableRef = model.findTable(alias);
        checkArgument(tableRef.getTableDesc().getIdentity().equals(col.column.getTable().getIdentity()));
        col.fixTableRef(tableRef);
    }

    public static void unfixUnknownModel(TblColRef col) {
        col.unfixTableRef();
    }

    // for test mainly
    public static TblColRef mockup(TableDesc table, int oneBasedColumnIndex, String name, String datatype) {
        return mockup(table, oneBasedColumnIndex, name, datatype, null);
    }

    // for test mainly
    public static TblColRef mockup(TableDesc table, int oneBasedColumnIndex, String name, String datatype,
            String comment) {
        ColumnDesc desc = new ColumnDesc();
        String id = "" + oneBasedColumnIndex;
        desc.setId(id);
        desc.setName(name);
        desc.setDatatype(datatype);
        desc.init(table);
        desc.setComment(comment);
        return new TblColRef(desc);
    }

    public void setQuotedFunc(Function<TblColRef, String> quotedFunc) {
        this.quotedFunc = quotedFunc;
    }

    public void fixTableRef(TableRef tableRef) {
        this.backupTable = this.table;
        this.table = tableRef;
        this.identity = null;
    }

    public ColumnDesc getColumnDesc() {
        return column;
    }

    public void unfixTableRef() {
        this.table = backupTable;
        this.identity = null;
    }

    public String getName() {
        return column.getName();
    }

    public TableRef getTableRef() {
        return table;
    }

    public boolean isQualified() {
        return table != null;
    }

    public String getTableAlias() {
        return table != null ? table.getAlias() : "UNKNOWN_ALIAS";
    }

    public String getExpressionInSourceDB() {
        if (!column.isComputedColumn()) {
            return getIdentity();
        } else {
            return column.getComputedColumnExpr();
        }
    }

    public String getTable() {
        if (column.getTable() == null) {
            return null;
        }
        return column.getTable().getIdentity();
    }

    public String getCanonicalName() {
        return getTable() + "." + getName();
    }

    public String getDatatype() {
        return column.getDatatype();
    }

    public DataType getType() {
        return column.getType();
    }

    public List<TupleExpression> getSubTupleExps() {
        return subTupleExps;
    }

    public void setSubTupleExps(List<TupleExpression> subTubleExps) {
        this.subTupleExps = subTubleExps;
    }

    public String getBackupTableAlias() {
        return backupTable.getAlias();
    }

    private void markInnerColumn(InnerDataTypeEnum dataType) {
        this.column.setDatatype(dataType.getDataType());
        this.column.getTable().setName(INNER_TABLE_NAME);
        this.column.getTable().setDatabase("DEFAULT");
    }

    public boolean isInnerColumn() {
        return InnerDataTypeEnum.contains(getDatatype());
    }

    public int hashCode() {
        // NOTE: tableRef MUST NOT participate in hashCode().
        // Because fixUnknownModel() can change tableRef while TblColRef is held as set/map keys.
        final int prime = 31;
        int result = 1;

        result = prime * result + column.getTable().getIdentity().hashCode();
        result = prime * result + column.getName().hashCode();
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
        TblColRef other = (TblColRef) obj;
        if (!StringUtils.equals(column.getTable().getIdentity(), other.column.getTable().getIdentity()))
            return false;
        if (!StringUtils.equals(column.getName(), other.column.getName()))
            return false;
        if ((table == null ? other.table == null : table.equals(other.table)) == false)
            return false;
        if (this.isInnerColumn() != other.isInnerColumn())
            return false;
        return true;
    }

    public String getIdentity() {
        if (identity == null)
            identity = getTableAlias() + "." + getName();
        return identity;
    }

    public String getQuotedIdentity() {
        if (quotedFunc == null)
            return getIdentity();
        else
            return quotedFunc.apply(this);
    }

    @Override
    public String toString() {
        if (isInnerColumn() && parserDescription != null)
            return parserDescription;

        String alias = table == null ? "UNKNOWN_MODEL" : table.getAlias();
        String tableName = column.getTable() == null ? "NULL" : column.getTable().getName();
        String tableIdentity = column.getTable() == null ? "NULL" : column.getTable().getIdentity();
        if (alias.equals(tableName)) {
            return tableIdentity + "." + column.getName();
        } else {
            return alias + ":" + tableIdentity + "." + column.getName();
        }
    }

    // return DB.TABLE
    public String getTableWithSchema() {
        if (isInnerColumn() && parserDescription != null)
            return parserDescription;
        if (column.getTable() == null) {
            return "NULL";
        } else {
            return column.getTable().getIdentity().toUpperCase(Locale.ROOT);
        }
    }

    // return DB.TABLE.COLUMN
    public String getColumWithTableAndSchema() {
        return (getTableWithSchema() + "." + column.getName()).toUpperCase(Locale.ROOT);
    }


    public String getColumnWithTable() {
        return (getTable() + "." + column.getName()).toUpperCase(Locale.ROOT);
    }

    // used by projection rewrite, see OLAPProjectRel
    public enum InnerDataTypeEnum {

        LITERAL("_literal_type"), DERIVED("_derived_type");

        private final String dateType;

        private InnerDataTypeEnum(String name) {
            this.dateType = name;
        }

        public static boolean contains(String name) {
            return LITERAL.getDataType().equals(name) || DERIVED.getDataType().equals(name);
        }

        public String getDataType() {
            return dateType;
        }
    }
}
