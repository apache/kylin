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
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.Queryable;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;
import net.hydromatic.optiq.TranslatableTable;
import net.hydromatic.optiq.impl.AbstractTableQueryable;
import net.hydromatic.optiq.impl.java.AbstractQueryableTable;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptTable.ToRelContext;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.type.SqlTypeUtil;

import com.google.common.collect.Lists;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.query.enumerator.OLAPQuery;
import org.apache.kylin.query.enumerator.OLAPQuery.EnumeratorTypeEnum;
import org.apache.kylin.query.relnode.OLAPTableScan;

/**
 * 
 * @author xjiang
 * 
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
            this.rowType = deduceRowType(typeFactory);
        }
        return this.rowType;
    }

    private RelDataType deduceRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
        for (ColumnDesc column : exposedColumns) {
            RelDataType sqlType = createSqlType(typeFactory, column);
            sqlType = SqlTypeUtil.addCharsetAndCollation(sqlType, typeFactory);
            fieldInfo.add(column.getName(), sqlType);
        }
        return typeFactory.createStructType(fieldInfo);
    }

    private RelDataType createSqlType(RelDataTypeFactory typeFactory, ColumnDesc column) {
        SqlTypeName sqlTypeName = SQLTYPE_MAPPING.get(column.getTypeName());
        if (sqlTypeName == null)
            throw new IllegalArgumentException("Unrecognized column type " + column.getTypeName() + " from " + column);

        int precision = column.getTypePrecision();
        int scale = column.getTypeScale();

        RelDataType result;
        if (precision >= 0 && scale >= 0)
            result = typeFactory.createSqlType(sqlTypeName, precision, scale);
        else if (precision >= 0)
            result = typeFactory.createSqlType(sqlTypeName, precision);
        else
            result = typeFactory.createSqlType(sqlTypeName);

        // due to left join and uncertain data quality, dimension value can be
        // null
        if (column.isNullable()) {
            result = typeFactory.createTypeWithNullability(result, true);
        } else {
            result = typeFactory.createTypeWithNullability(result, false);
        }

        return result;
    }

    private List<ColumnDesc> listSourceColumns() {
        ProjectManager mgr = ProjectManager.getInstance(olapSchema.getConfig());
        List<ColumnDesc> exposedColumns = Lists.newArrayList(mgr.listExposedColumns(olapSchema.getProjectName(), sourceTable.getIdentity()));

        List<MeasureDesc> countMeasures = mgr.listEffectiveRewriteMeasures(olapSchema.getProjectName(), sourceTable.getIdentity());
        HashSet<String> metFields = new HashSet<String>();
        for (MeasureDesc m : countMeasures) {
            FunctionDesc func = m.getFunction();
            String fieldName = func.getRewriteFieldName();
            if (!metFields.contains(fieldName)) {
                metFields.add(fieldName);
                ColumnDesc fakeCountCol = new ColumnDesc();
                fakeCountCol.setName(fieldName);
                fakeCountCol.setDatatype(func.getSQLType());
                fakeCountCol.setNullable(false);
                fakeCountCol.init(sourceTable);
                exposedColumns.add(fakeCountCol);
            }
        }

        return exposedColumns;
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
                final OLAPQuery query = new OLAPQuery(EnumeratorTypeEnum.OLAP, 0);
                return (Enumerator<T>) query.enumerator();
            }
        };
    }

    @Override
    public Statistic getStatistic() {
        List<BitSet> keys = new ArrayList<BitSet>();
        return Statistics.of(100, keys);
    }

    @Override
    public String toString() {
        return "OLAPTable {" + getTableName() + "}";
    }

    public Enumerable<Object[]> executeIndexQuery(DataContext optiqContext, int ctxSeq) {
        return new OLAPQuery(optiqContext, EnumeratorTypeEnum.OLAP, ctxSeq);
    }

    public Enumerable<Object[]> executeLookupTableQuery(DataContext optiqContext, int ctxSeq) {
        return new OLAPQuery(optiqContext, EnumeratorTypeEnum.LOOKUP_TABLE, ctxSeq);
    }

    public Enumerable<Object[]> executeHiveQuery(DataContext optiqContext, int ctxSeq) {
        return new OLAPQuery(optiqContext, EnumeratorTypeEnum.HIVE, ctxSeq);
    }

}
