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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.datatype.DataType;

import lombok.Getter;
import lombok.Setter;

/**
 */
@SuppressWarnings({ "serial" })
public class TblColRef implements Serializable {

    private static final String INNER_TABLE_NAME = "_kylin_table";
    private static final String BACK_TICK = Quoting.BACK_TICK.string;
    private static final String DOUBLE_QUOTE = Quoting.DOUBLE_QUOTE.string;
    public static final String DYNAMIC_DATA_TYPE = "_dynamic_type";
    public static final String UNKNOWN_ALIAS = "UNKNOWN_ALIAS";

    // used by projection rewrite, see OLAPProjectRel
    public enum InnerDataTypeEnum {

        LITERAL("_literal_type"), DERIVED("_derived_type"), AGGREGATION_TYPE("_aggregation_type");

        private final String dateType;

        private InnerDataTypeEnum(String name) {
            this.dateType = name;
        }

        public String getDataType() {
            return dateType;
        }

        public static boolean contains(String name) {
            return LITERAL.getDataType().equals(name) || DERIVED.getDataType().equals(name)
                    || AGGREGATION_TYPE.getDataType().equals(name);
        }

        public static boolean isAggregationType(String name) {
            return AGGREGATION_TYPE.getDataType().equals(name);
        }
    }

    public enum FilterColEnum {
        EQUAL_FILTER("equal_filter", 5),

        INFERIOR_EQUAL_FILTER("inferior_equal_filter", 4),

        RANGE_FILTER("range_filter", 3),

        LIKE_FILTER("like_filter", 2),

        OTHER_FILTER("other_filter", 1),

        NONE("none", 0);

        private final String filterLevel;
        private final int priority;

        FilterColEnum(String filterLevel, int priority) {
            this.filterLevel = filterLevel;
            this.priority = priority;
        }

        public int getPriority() {
            return this.priority;
        }

    }

    // used by projection rewrite, see OLAPProjectRel
    public static TblColRef newInnerColumn(String columnName, InnerDataTypeEnum dataType) {
        return newInnerColumn(columnName, dataType, null);
    }

    // used by projection rewrite, see OLAPProjectRel
    public static TblColRef newInnerColumn(String columnName, InnerDataTypeEnum dataType, String parserDescription) {
        return newInnerColumn(columnName, dataType, parserDescription, null, null);
    }

    public static TblColRef newInnerColumn(String columnName, InnerDataTypeEnum dataType, String parserDescription,
            SqlOperator operator, List<TblColRef> opreands) {
        ColumnDesc column = new ColumnDesc();
        column.setName(columnName);
        TableDesc table = new TableDesc();
        column.setTable(table);
        TblColRef colRef = new TblColRef(column);
        colRef.markInnerColumn(dataType);
        colRef.parserDescription = parserDescription;
        colRef.setOperator(operator);
        colRef.setOperands(opreands);
        return colRef;
    }

    public static TblColRef newDynamicColumn(String columnName) {
        ColumnDesc column = new ColumnDesc();
        column.setName(columnName);
        column.setDatatype(DYNAMIC_DATA_TYPE);
        TableDesc table = new TableDesc();
        column.setTable(table);
        TblColRef colRef = new TblColRef(column);
        return colRef;
    }

    private static final NDataModel UNKNOWN_MODEL = new NDataModel();
    static {
        UNKNOWN_MODEL.setAlias("UNKNOWN_MODEL");
    }

    public static TableRef tableForUnknownModel(String tempTableAlias, TableDesc table) {
        return new TableRef(UNKNOWN_MODEL, tempTableAlias, table, false);
    }

    public static TblColRef columnForUnknownModel(TableRef table, ColumnDesc colDesc) {
        checkArgument(table.getModel() == UNKNOWN_MODEL);
        return new TblColRef(table, colDesc);
    }

    public static void fixUnknownModel(NDataModel model, String alias, TblColRef col) {
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
            String computedColumnExpr) {
        return mockup(table, oneBasedColumnIndex, name, datatype, null, computedColumnExpr);
    }

    // for test mainly
    public static TblColRef mockup(TableDesc table, int oneBasedColumnIndex, String name, String datatype,
            String comment, String computedColumnExpr) {
        String id = "" + oneBasedColumnIndex;
        ColumnDesc desc = new ColumnDesc(id, name, datatype, comment, null, null, computedColumnExpr);
        desc.init(table);
        return new TblColRef(desc);
    }

    // ============================================================================

    private TableRef table;
    private TableRef backupTable;// only used in fixTableRef()
    private ColumnDesc column;
    private String identity;
    private String parserDescription;
    @Getter
    @Setter
    private FilterColEnum filterLevel = FilterColEnum.NONE;
    @Setter
    @Getter
    private transient SqlOperator operator;//only used for InnerCol, other case it should be null
    @Setter
    @Getter
    private List<TblColRef> operands;//only used for InnerCol

    TblColRef(ColumnDesc column) {
        this.column = column;
    }

    public TblColRef(TableRef table, ColumnDesc column) {
        checkArgument(table.getTableDesc().getIdentity().equals(column.getTable().getIdentity()));
        this.table = table;
        this.column = column;
    }
    
    private String wrapIdentity(String wrap) {
        return wrap + getTableAlias() + wrap + "." + wrap + getName() + wrap;
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

    public String getOriginalName() {
        return column.getOriginalName();
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
        return table != null ? table.getAlias() : UNKNOWN_ALIAS;
    }

    public String getExpressionInSourceDB() {
        if (!column.isComputedColumn()) {
            return getIdentity();
        } else {
            return column.getComputedColumnExpr();
        }
    }

    public String getDoubleQuoteExpressionInSourceDB() {
        if (column.isComputedColumn())
            return column.getComputedColumnExpr();

        return wrapIdentity(DOUBLE_QUOTE);
    }

    public String getBackTickExpressionInSourceDB() {
        if (column.isComputedColumn())
            return column.getComputedColumnExpr();

        return wrapIdentity(BACK_TICK);
    }

    public String getTable() {
        if (column.getTable() == null) {
            return null;
        }
        return column.getTable().getIdentity();
    }

    public String getAliasDotName() {
        return getTableAlias() + "." + getName();
    }

    public String getTableDotName() {
        return column.getTable().getName() + "." + getName();
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

    public String getParserDescription() {
        return parserDescription;
    }

    private void markInnerColumn(InnerDataTypeEnum dataType) {
        this.column.setDatatype(dataType.getDataType());
        this.column.getTable().setName(INNER_TABLE_NAME);
        this.column.getTable().setDatabase("DEFAULT");
    }

    public boolean isInnerColumn() {
        return InnerDataTypeEnum.contains(getDatatype());
    }

    public boolean isAggregationColumn() {
        return InnerDataTypeEnum.isAggregationType(getDatatype());
    }

    public int hashCode() {
        // NOTE: tableDesc MUST NOT participate in hashCode().
        // Because fixUnknownModel() can change tableDesc while TblColRef is held as set/map keys.
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
        if (!(table == null ? other.table == null : table.equals(other.table)))
            return false;
        //#9121 self-join's agg pushdown sql's left and right OlapContext have identical table,
        //backupTable, containing olapContext's info, should be compared when both is not null
        if (backupTable != null && other.backupTable != null && !backupTable.equals(other.backupTable))
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

    public String getBackTickIdentity() {
        return wrapIdentity(BACK_TICK);
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
    public String getColumnWithTableAndSchema() {
        return (getTableWithSchema() + "." + column.getName()).toUpperCase(Locale.ROOT);
    }

    public boolean isCastInnerColumn() {
        return isInnerColumn() && getOperator() != null && getOperator().kind == SqlKind.CAST;
    }

    public static void collectSourceColumns(TblColRef colRef, Set<TblColRef> collector) {
        boolean innerColumn = colRef.isInnerColumn();
        if (!innerColumn) {
            collector.add(colRef);
            return;
        }
        if (colRef.getOperands() == null) {
            return;
        }
        for (TblColRef child : colRef.getOperands()) {
            collectSourceColumns(child, collector);
        }
    }

    public Set<TblColRef> getSourceColumns() {
        Set<TblColRef> resultSet = new HashSet<>();
        collectSourceColumns(this, resultSet);
        return resultSet;
    }
}
