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

package org.apache.kylin.query.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.query.enumerator.OLAPQuery;
import org.apache.kylin.query.enumerator.OLAPQuery.EnumeratorTypeEnum;
import org.apache.kylin.query.relnode.OLAPTableScan;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 */
public class OLAPTable extends AbstractQueryableTable implements TranslatableTable {

    private static Map<String, SqlTypeName> SQLTYPE_MAPPING = new HashMap<String, SqlTypeName>();

    static {
        SQLTYPE_MAPPING.put("char", SqlTypeName.CHAR);
        SQLTYPE_MAPPING.put("varchar", SqlTypeName.VARCHAR);
        SQLTYPE_MAPPING.put("boolean", SqlTypeName.BOOLEAN);
        SQLTYPE_MAPPING.put("integer", SqlTypeName.INTEGER);
        SQLTYPE_MAPPING.put("tinyint", SqlTypeName.TINYINT);
        SQLTYPE_MAPPING.put("smallint", SqlTypeName.SMALLINT);
        SQLTYPE_MAPPING.put("bigint", SqlTypeName.BIGINT);
        SQLTYPE_MAPPING.put("decimal", SqlTypeName.DECIMAL);
        SQLTYPE_MAPPING.put("numeric", SqlTypeName.DECIMAL);
        SQLTYPE_MAPPING.put("float", SqlTypeName.FLOAT);
        SQLTYPE_MAPPING.put("real", SqlTypeName.REAL);
        SQLTYPE_MAPPING.put("double", SqlTypeName.DOUBLE);
        SQLTYPE_MAPPING.put("date", SqlTypeName.DATE);
        SQLTYPE_MAPPING.put("time", SqlTypeName.TIME);
        SQLTYPE_MAPPING.put("timestamp", SqlTypeName.TIMESTAMP);
        SQLTYPE_MAPPING.put("any", SqlTypeName.ANY);

        // try {
        // Class.forName("org.apache.hive.jdbc.HiveDriver");
        // } catch (ClassNotFoundException e) {
        // e.printStackTrace();
        // }
    }

    private final OLAPSchema olapSchema;
    private final TableDesc sourceTable;
    private RelDataType rowType;
    private List<ColumnDesc> exposedColumns;

    public OLAPTable(OLAPSchema schema, TableDesc tableDesc) {
        super(Object[].class);
        this.olapSchema = schema;
        this.sourceTable = tableDesc;
        this.rowType = null;
    }

    public OLAPSchema getSchema() {
        return this.olapSchema;
    }

    public TableDesc getSourceTable() {
        return this.sourceTable;
    }

    public String getTableName() {
        return this.sourceTable.getIdentity();
    }

    public List<ColumnDesc> getExposedColumns() {
        return exposedColumns;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (this.rowType == null) {
            // always build exposedColumns and rowType together
            this.exposedColumns = listSourceColumns();
            this.rowType = deriveRowType(typeFactory);
        }
        return this.rowType;
    }

    private RelDataType deriveRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
        for (ColumnDesc column : exposedColumns) {
            RelDataType sqlType = createSqlType(typeFactory, column.getType(), column.isNullable());
            sqlType = SqlTypeUtil.addCharsetAndCollation(sqlType, typeFactory);
            fieldInfo.add(column.getName(), sqlType);
        }
        return typeFactory.createStructType(fieldInfo);
    }

    public static RelDataType createSqlType(RelDataTypeFactory typeFactory, DataType dataType, boolean isNullable) {
        SqlTypeName sqlTypeName = SQLTYPE_MAPPING.get(dataType.getName());
        if (sqlTypeName == null)
            throw new IllegalArgumentException("Unrecognized data type " + dataType);

        int precision = dataType.getPrecision();
        int scale = dataType.getScale();

        RelDataType result;
        if (precision >= 0 && scale >= 0)
            result = typeFactory.createSqlType(sqlTypeName, precision, scale);
        else if (precision >= 0)
            result = typeFactory.createSqlType(sqlTypeName, precision);
        else
            result = typeFactory.createSqlType(sqlTypeName);

        // due to left join and uncertain data quality, dimension value can be null
        if (isNullable) {
            result = typeFactory.createTypeWithNullability(result, true);
        } else {
            result = typeFactory.createTypeWithNullability(result, false);
        }

        return result;
    }
    
    private List<ColumnDesc> listSourceColumns() {
        ProjectManager mgr = ProjectManager.getInstance(olapSchema.getConfig());
        List<ColumnDesc> tableColumns = Lists.newArrayList(mgr.listExposedColumns(olapSchema.getProjectName(), sourceTable.getIdentity()));

        List<MeasureDesc> countMeasures = mgr.listEffectiveMeasures(olapSchema.getProjectName(), sourceTable.getIdentity(), true);
        HashSet<String> metFields = new HashSet<String>();
        List<ColumnDesc> metricColumns = Lists.newArrayList();
        for (MeasureDesc m : countMeasures) {
            FunctionDesc func = m.getFunction();
            String fieldName = func.getRewriteFieldName();
            if (!metFields.contains(fieldName)) {
                metFields.add(fieldName);
                ColumnDesc fakeCountCol = new ColumnDesc();
                fakeCountCol.setName(fieldName);
                fakeCountCol.setDatatype(func.getRewriteFieldType().toString());
                if (func.isCount())
                    fakeCountCol.setNullable(false);
                fakeCountCol.init(sourceTable);
                metricColumns.add(fakeCountCol);
            }
        }

        //if exist sum(x), where x is integer/short/byte
        //to avoid overflow we upgrade x's type to long
        HashSet<ColumnDesc> updateColumns = Sets.newHashSet();
        for (MeasureDesc m : mgr.listEffectiveMeasures(olapSchema.getProjectName(), sourceTable.getIdentity(), false)) {
            if (m.getFunction().isSum()) {
                FunctionDesc func = m.getFunction();
                if (//func.getReturnDataType() != func.getRewriteFieldType() && //
                func.getReturnDataType().isBigInt() && //
                        func.getRewriteFieldType().isIntegerFamily()) {
                    updateColumns.add(func.getParameter().getColRefs().get(0).getColumnDesc());
                }
            }
        }
        for (ColumnDesc upgrade : updateColumns) {
            int index = tableColumns.indexOf(upgrade);
            tableColumns.get(index).setDatatype("bigint");
        }

        return Lists.newArrayList(Iterables.concat(tableColumns, metricColumns));
    }

    @Override
    public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
        int fieldCount = relOptTable.getRowType().getFieldCount();
        int[] fields = identityList(fieldCount);
        return new OLAPTableScan(context.getCluster(), relOptTable, this, fields);
    }

    private int[] identityList(int n) {
        int[] integers = new int[n];
        for (int i = 0; i < n; i++) {
            integers[i] = i;
        }
        return integers;
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
            @SuppressWarnings("unchecked")
            public Enumerator<T> enumerator() {
                final OLAPQuery query = new OLAPQuery(EnumeratorTypeEnum.INDEX, 0);
                return (Enumerator<T>) query.enumerator();
            }
        };
    }

    @Override
    public Statistic getStatistic() {
        List<ImmutableBitSet> keys = new ArrayList<ImmutableBitSet>();
        return Statistics.of(100, keys);
    }

    @Override
    public String toString() {
        return "OLAPTable {" + getTableName() + "}";
    }

    public Enumerable<Object[]> executeIndexQuery(DataContext optiqContext, int ctxSeq) {
        return new OLAPQuery(optiqContext, EnumeratorTypeEnum.INDEX, ctxSeq);
    }

    public Enumerable<Object[]> executeLookupTableQuery(DataContext optiqContext, int ctxSeq) {
        return new OLAPQuery(optiqContext, EnumeratorTypeEnum.LOOKUP_TABLE, ctxSeq);
    }

    public Enumerable<Object[]> executeHiveQuery(DataContext optiqContext, int ctxSeq) {
        return new OLAPQuery(optiqContext, EnumeratorTypeEnum.HIVE, ctxSeq);
    }

}
