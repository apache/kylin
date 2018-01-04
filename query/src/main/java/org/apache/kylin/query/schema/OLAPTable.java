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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 */
public class OLAPTable extends AbstractQueryableTable implements TranslatableTable {

    protected static final Logger logger = LoggerFactory.getLogger(OLAPTable.class);

    private static Map<String, SqlTypeName> SQLTYPE_MAPPING = new HashMap<String, SqlTypeName>();
    private static Map<String, SqlTypeName> REGEX_SQLTYPE_MAPPING = new HashMap<String, SqlTypeName>();

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

        REGEX_SQLTYPE_MAPPING.put("array\\<.+\\>", SqlTypeName.ARRAY);
    }

    private final boolean exposeMore;
    private final OLAPSchema olapSchema;
    private final TableDesc sourceTable;
    protected RelDataType rowType;
    private List<ColumnDesc> sourceColumns;

    public OLAPTable(OLAPSchema schema, TableDesc tableDesc, boolean exposeMore) {
        super(Object[].class);
        this.exposeMore = exposeMore;
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

    public List<ColumnDesc> getSourceColumns() {
        if (sourceColumns == null) {
            sourceColumns = listSourceColumns();
        }
        return sourceColumns;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (this.rowType == null) {
            // always build exposedColumns and rowType together
            this.sourceColumns = getSourceColumns();
            this.rowType = deriveRowType(typeFactory);
        }
        return this.rowType;
    }

    @SuppressWarnings("deprecation")
    private RelDataType deriveRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
        for (ColumnDesc column : sourceColumns) {
            RelDataType sqlType = createSqlType(typeFactory, column.getUpgradedType(), column.isNullable());
            sqlType = SqlTypeUtil.addCharsetAndCollation(sqlType, typeFactory);
            fieldInfo.add(column.getName(), sqlType);
        }
        return typeFactory.createStructType(fieldInfo);
    }

    public static RelDataType createSqlType(RelDataTypeFactory typeFactory, DataType dataType, boolean isNullable) {
        SqlTypeName sqlTypeName = SQLTYPE_MAPPING.get(dataType.getName());
        if (sqlTypeName == null) {
            for (String reg : REGEX_SQLTYPE_MAPPING.keySet()) {
                Pattern pattern = Pattern.compile(reg);
                if (pattern.matcher(dataType.getName()).matches()) {
                    sqlTypeName = REGEX_SQLTYPE_MAPPING.get(reg);
                    break;
                }
            }
        }

        if (sqlTypeName == null)
            throw new IllegalArgumentException("Unrecognized data type " + dataType);

        int precision = dataType.getPrecision();
        int scale = dataType.getScale();

        RelDataType result;
        if (sqlTypeName == SqlTypeName.ARRAY) {
            String innerTypeName = dataType.getName().split("<|>")[1];
            result = typeFactory.createArrayType(createSqlType(typeFactory, DataType.getType(innerTypeName), false),
                    -1);
        } else if (precision >= 0 && scale >= 0)
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

        List<ColumnDesc> tableColumns = mgr.listExposedColumns(olapSchema.getProjectName(), sourceTable, exposeMore);

        List<ColumnDesc> metricColumns = Lists.newArrayList();
        List<MeasureDesc> countMeasures = mgr.listEffectiveRewriteMeasures(olapSchema.getProjectName(),
                sourceTable.getIdentity());
        HashSet<String> metFields = new HashSet<String>();
        for (MeasureDesc m : countMeasures) {

            FunctionDesc func = m.getFunction();
            String fieldName = func.getRewriteFieldName();
            if (!metFields.contains(fieldName)) {
                metFields.add(fieldName);
                ColumnDesc fakeCountCol = func.newFakeRewriteColumn(sourceTable);
                metricColumns.add(fakeCountCol);
            }
        }

        Collections.sort(tableColumns, new Comparator<ColumnDesc>() {
            @Override
            public int compare(ColumnDesc o1, ColumnDesc o2) {
                return o1.getZeroBasedIndex() - o2.getZeroBasedIndex();
            }
        });
        return Lists.newArrayList(Iterables.concat(tableColumns, metricColumns));
    }

    @Override
    public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
        int fieldCount = relOptTable.getRowType().getFieldCount();
        int[] fields = identityList(fieldCount);
        return new OLAPTableScan(context.getCluster(), relOptTable, this, fields);
    }

    protected int[] identityList(int n) {
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
        List<ImmutableBitSet> keys = new ArrayList<ImmutableBitSet>();
        return Statistics.of(100, keys);
    }

    @Override
    public String toString() {
        return "OLAPTable {" + getTableName() + "}";
    }

    public Enumerable<Object[]> executeOLAPQuery(DataContext optiqContext, int ctxSeq) {
        return new OLAPQuery(optiqContext, EnumeratorTypeEnum.OLAP, ctxSeq);
    }

    public Enumerable<Object[]> executeLookupTableQuery(DataContext optiqContext, int ctxSeq) {
        return new OLAPQuery(optiqContext, EnumeratorTypeEnum.LOOKUP_TABLE, ctxSeq);
    }

    public Enumerable<Object[]> executeHiveQuery(DataContext optiqContext, int ctxSeq) {
        return new OLAPQuery(optiqContext, EnumeratorTypeEnum.HIVE, ctxSeq);
    }

}
