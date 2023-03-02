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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.CollectionUtil;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.QueryExtension;
import org.apache.kylin.query.enumerator.OLAPQuery;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.apache.kylin.rest.constant.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import lombok.val;

/**
 */
public class OLAPTable extends AbstractQueryableTable implements TranslatableTable {

    protected static final Logger logger = LoggerFactory.getLogger(OLAPTable.class);

    private static Map<String, SqlTypeName> SQLTYPE_MAPPING = new HashMap<>();
    private static Map<String, SqlTypeName> REGEX_SQLTYPE_MAPPING = new HashMap<>();

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

        REGEX_SQLTYPE_MAPPING.put("array\\<.*\\>", SqlTypeName.ARRAY);
    }

    private final OLAPSchema olapSchema;
    private final TableDesc sourceTable;
    protected RelDataType rowType;
    private List<ColumnDesc> sourceColumns;
    private Map<String, List<NDataModel>> modelsMap;

    public OLAPTable(OLAPSchema schema, TableDesc tableDesc, Map<String, List<NDataModel>> modelsMap) {
        super(Object[].class);
        this.olapSchema = schema;
        this.sourceTable = tableDesc;
        this.rowType = null;
        this.modelsMap = modelsMap;
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
        //TODO Add Fluid API to build a list of fields, TypeFactory.Builder
        KylinRelDataTypeFactoryImpl kylinRelDataTypeFactory = new KylinRelDataTypeFactoryImpl(typeFactory);
        List<String> fieldNameList = Lists.newArrayList();
        List<RelDataType> typeList = Lists.newArrayList();
        List<KylinRelDataTypeFieldImpl.ColumnType> colTypes = Lists.newArrayList();
        for (ColumnDesc column : sourceColumns) {
            RelDataType sqlType = createSqlType(kylinRelDataTypeFactory, column.getUpgradedType(), column.isNullable());
            sqlType = SqlTypeUtil.addCharsetAndCollation(sqlType, kylinRelDataTypeFactory);
            typeList.add(sqlType);
            String project = this.sourceTable != null ? this.sourceTable.getProject() : null;
            KylinConfig projectKylinConfig = StringUtils.isNotEmpty(project)
                    ? NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project).getConfig()
                    : KylinConfig.getInstanceFromEnv();
            String columnName = projectKylinConfig.getSourceNameCaseSensitiveEnabled()
                    ? StringUtils.isNotEmpty(column.getCaseSensitiveName()) ? column.getCaseSensitiveName()
                            : column.getName()
                    : column.getName();
            if (column.isComputedColumn()) {
                fieldNameList.add(columnName);
                colTypes.add(KylinRelDataTypeFieldImpl.ColumnType.CC_FIELD);
            } else {
                fieldNameList.add(columnName);
                colTypes.add(KylinRelDataTypeFieldImpl.ColumnType.ORIGIN_FILED);
            }
        }
        return kylinRelDataTypeFactory.createStructType(StructKind.FULLY_QUALIFIED, typeList, fieldNameList, colTypes);
    }

    private List<ColumnDesc> listSourceColumns() {
        NProjectManager mgr = NProjectManager.getInstance(olapSchema.getConfig());

        List<ColumnDesc> tableColumns = listTableColumnsIncludingCC();

        List<ColumnDesc> metricColumns = Lists.newArrayList();
        List<MeasureDesc> countMeasures = mgr.listEffectiveRewriteMeasures(olapSchema.getProjectName(),
                sourceTable.getIdentity());
        HashSet<String> metFields = new HashSet<>();
        for (MeasureDesc m : countMeasures) {

            FunctionDesc func = m.getFunction();
            String fieldName;
            if (FunctionDesc.FUNC_TOP_N.equalsIgnoreCase(func.getExpression())) {
                fieldName = TopNMeasureType.getRewriteName(func);
            } else {
                fieldName = func.getRewriteFieldName();
            }
            if (!metFields.contains(fieldName)) {
                metFields.add(fieldName);
                ColumnDesc fakeCountCol = func.newFakeRewriteColumn(fieldName, sourceTable);
                metricColumns.add(fakeCountCol);
            }
        }

        tableColumns.sort(Comparator.comparingInt(ColumnDesc::getZeroBasedIndex));
        return Lists.newArrayList(Iterables.concat(tableColumns, metricColumns));
    }

    private List<ColumnDesc> listTableColumnsIncludingCC() {
        List<ColumnDesc> allColumns = Lists.newArrayList(sourceTable.getColumns());

        if (!modelsMap.containsKey(sourceTable.getIdentity())) {
            return allColumns;
        }

        ProjectInstance projectInstance = NProjectManager.getInstance(olapSchema.getConfig())
                .getProject(sourceTable.getProject());
        NDataflowManager dataflowManager = NDataflowManager.getInstance(olapSchema.getConfig(),
                sourceTable.getProject());
        if (projectInstance.getConfig().useTableIndexAnswerSelectStarEnabled()) {
            Set<ColumnDesc> exposeColumnDescSet = new HashSet<>();
            String tableName = sourceTable.getIdentity();
            List<NDataModel> modelList = modelsMap.get(tableName);
            for (NDataModel dataModel : modelList) {
                NDataflow dataflow = dataflowManager.getDataflow(dataModel.getId());
                if (dataflow.getStatus() == RealizationStatusEnum.ONLINE) {
                    dataflow.getAllColumns().forEach(tblColRef -> {
                        if (tblColRef.getTable().equalsIgnoreCase(tableName)) {
                            exposeColumnDescSet.add(tblColRef.getColumnDesc());
                        }
                    });
                }
            }
            if (!exposeColumnDescSet.isEmpty()) {
                allColumns = Lists.newArrayList(exposeColumnDescSet);
            }
        }

        val authorizedCC = getAuthorizedCC();
        if (CollectionUtils.isNotEmpty(authorizedCC)) {
            val ccAsColumnDesc = ComputedColumnUtil.createComputedColumns(authorizedCC, sourceTable);
            allColumns.addAll(Lists.newArrayList(ccAsColumnDesc));
        }

        return allColumns.stream().distinct().collect(Collectors.toList());
    }

    // since computed columns are either of different expr and different names,
    // or of same expr and same names
    // so we need only the filter out duplicated named cols to avoid duplication of names and exprs
    private List<ComputedColumnDesc> removeDuplicatedNamedComputedCols(List<ComputedColumnDesc> computedColumnDescs) {
        // remove duplicated named computed cols
        CollectionUtil.distinct(computedColumnDescs, cc -> cc.getIdentName().toUpperCase(Locale.ROOT));
        return computedColumnDescs;
    }

    private List<ComputedColumnDesc> getAuthorizedCC() {
        if (isACLDisabledOrAdmin()) {
            return removeDuplicatedNamedComputedCols(modelsMap.get(sourceTable.getIdentity()).stream()
                    .map(NDataModel::getComputedColumnDescs).flatMap(List::stream).collect(Collectors.toList()));
        }

        val authorizedCC = ComputedColumnUtil.getAuthorizedCC(modelsMap.get(sourceTable.getIdentity()),
                this::isColumnAuthorized);

        return removeDuplicatedNamedComputedCols(authorizedCC);
    }

    private boolean isACLDisabledOrAdmin() {
        QueryContext.AclInfo aclInfo = QueryContext.current().getAclInfo();
        if (!olapSchema.getConfig().isAclTCREnabled()
                || Objects.nonNull(aclInfo) && (CollectionUtils.isNotEmpty(aclInfo.getGroups())
                        && aclInfo.getGroups().stream().anyMatch(Constant.ROLE_ADMIN::equals))
                || Objects.nonNull(aclInfo) && aclInfo.isHasAdminPermission()) {
            return true;
        }

        return false;
    }

    private boolean isColumnAuthorized(Set<String> ccSourceCols) {
        QueryContext.AclInfo aclInfo = QueryContext.current().getAclInfo();
        String userName = Objects.nonNull(aclInfo) ? aclInfo.getUsername() : null;
        Set<String> groups = Objects.nonNull(aclInfo) ? aclInfo.getGroups() : null;
        return QueryExtension.getFactory().getTableColumnAuthExtension().isColumnsAuthorized(olapSchema.getConfig(),
                olapSchema.getProjectName(), userName, groups, ccSourceCols);
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
                final OLAPQuery query = new OLAPQuery(OLAPQuery.EnumeratorTypeEnum.OLAP, 0);
                return (Enumerator<T>) query.enumerator();
            }
        };
    }

    @Override
    public Statistic getStatistic() {
        List<ImmutableBitSet> keys = new ArrayList<>();
        return Statistics.of(100, keys);
    }

    @Override
    public String toString() {
        return "OLAPTable {" + getTableName() + "}";
    }

    public Enumerable<Object[]> executeOLAPQuery(DataContext optiqContext, int ctxSeq) {
        return new OLAPQuery(optiqContext, OLAPQuery.EnumeratorTypeEnum.OLAP, ctxSeq);
    }

    public Enumerable<Object[]> executeHiveQuery(DataContext optiqContext, int ctxSeq) {
        return new OLAPQuery(optiqContext, OLAPQuery.EnumeratorTypeEnum.HIVE, ctxSeq);
    }

    public Enumerable<Object[]> executeSimpleAggregationQuery(DataContext optiqContext, int ctxSeq) {
        return new OLAPQuery(optiqContext, OLAPQuery.EnumeratorTypeEnum.SIMPLE_AGGREGATION, ctxSeq);
    }
}
