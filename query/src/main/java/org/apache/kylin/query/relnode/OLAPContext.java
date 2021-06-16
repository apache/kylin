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

package org.apache.kylin.query.relnode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.calcite.DataContext;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.threadlocal.InternalThreadLocal;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.expression.ExpressionColCollector;
import org.apache.kylin.metadata.expression.TupleExpression;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DynamicFunctionDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinsTree;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.realization.SQLDigest.SQLCall;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.query.routing.RealizationCheck;
import org.apache.kylin.query.schema.OLAPSchema;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.hybrid.HybridInstance;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 */
public class OLAPContext {

    public static final String PRM_ACCEPT_PARTIAL_RESULT = "AcceptPartialResult";
    public static final String PRM_USER_AUTHEN_INFO = "UserAuthenInfo";

    static final InternalThreadLocal<Map<String, String>> _localPrarameters = new InternalThreadLocal<>();

    static final InternalThreadLocal<Map<Integer, OLAPContext>> _localContexts = new InternalThreadLocal<>();

    public static void setParameters(Map<String, String> parameters) {
        _localPrarameters.set(parameters);
    }

    public static void clearParameter() {
        _localPrarameters.remove();
    }

    public static void registerContext(OLAPContext ctx) {
        if (_localContexts.get() == null) {
            Map<Integer, OLAPContext> contextMap = new HashMap<>();
            _localContexts.set(contextMap);
        }
        _localContexts.get().put(ctx.id, ctx);
    }

    public static Collection<OLAPContext> getThreadLocalContexts() {
        Map<Integer, OLAPContext> map = _localContexts.get();
        return map == null ? null : map.values();
    }

    public static OLAPContext getThreadLocalContextById(int id) {
        Map<Integer, OLAPContext> map = _localContexts.get();
        return map.get(id);
    }

    public static void clearThreadLocalContexts() {
        _localContexts.remove();
    }

    public OLAPContext(int seq) {
        this.id = seq;
        this.storageContext = new StorageContext(seq);
        this.sortColumns = Lists.newArrayList();
        this.sortOrders = Lists.newArrayList();
        Map<String, String> parameters = _localPrarameters.get();
        if (parameters != null) {
            String acceptPartialResult = parameters.get(PRM_ACCEPT_PARTIAL_RESULT);
            if (acceptPartialResult != null) {
                this.storageContext.setAcceptPartialResult(Boolean.parseBoolean(acceptPartialResult));
            }
            String acceptUserInfo = parameters.get(PRM_USER_AUTHEN_INFO);
            if (null != acceptUserInfo)
                this.olapAuthen.parseUserInfo(acceptUserInfo);
        }
    }

    public final int id;
    public final StorageContext storageContext;

    // query info
    public OLAPSchema olapSchema = null;
    public OLAPTableScan firstTableScan = null; // to be fact table scan except "select * from lookupTable"
    public Set<OLAPTableScan> allTableScans = new HashSet<>();
    public Set<OLAPJoinRel> allOlapJoins = new HashSet<>();
    public Set<MeasureDesc> involvedMeasure = new HashSet<>();
    public TupleInfo returnTupleInfo = null;
    public boolean afterAggregate = false;
    public boolean afterHavingClauseFilter = false;
    public boolean afterLimit = false;
    public boolean limitPrecedesAggr = false;
    public boolean afterJoin = false;
    public boolean hasJoin = false;
    public boolean hasLimit = false;
    public boolean hasWindow = false;
    public boolean groupByExpression = false; // checkout if group by column has operator
    public boolean afterOuterAggregate = false;
    public boolean disableLimitPushdown = !KylinConfig.getInstanceFromEnv().isLimitPushDownEnabled();
    public boolean isExactlyAggregate = false;

    // cube metadata
    public IRealization realization;
    public RealizationCheck realizationCheck;
    public boolean fixedModel;

    public Set<TblColRef> allColumns = new HashSet<>();
    public List<TblColRef> groupByColumns = new ArrayList<>();
    public Set<TblColRef> subqueryJoinParticipants = new HashSet<>();//subqueryJoinParticipants will be added to groupByColumns(only when other group by co-exists) and allColumns
    public Set<TblColRef> metricsColumns = new HashSet<>();
    public List<FunctionDesc> aggregations = new ArrayList<>(); // storage level measure type, on top of which various sql aggr function may apply
    public List<TblColRef> aggrOutCols = new ArrayList<>(); // aggregation output (inner) columns
    public List<SQLCall> aggrSqlCalls = new ArrayList<>(); // sql level aggregation function call
    public Set<TblColRef> filterColumns = new HashSet<>();
    public TupleFilter filter;
    public TupleFilter havingFilter;
    public List<JoinDesc> joins = new LinkedList<>();
    public JoinsTree joinsTree;
    public boolean isBorrowedContext = false; // Whether preparedContext is borrowed from cache
    List<TblColRef> sortColumns;
    List<SQLDigest.OrderEnum> sortOrders;

    // rewrite info
    public Map<String, RelDataType> rewriteFields = new HashMap<>();

    // dynamic columns info, note that the name of TblColRef will be the field name
    public Map<TblColRef, RelDataType> dynamicFields = new HashMap<>();

    public Map<TblColRef, TupleExpression> dynGroupBy = new HashMap<>();

    // hive query
    public String sql = "";

    public OLAPAuthentication olapAuthen = new OLAPAuthentication();

    public boolean isSimpleQuery() {
        return (joins.isEmpty()) && (groupByColumns.isEmpty()) && (aggregations.isEmpty());
    }

    SQLDigest sqlDigest;

    public SQLDigest getSQLDigest() {
        if (sqlDigest == null) {
            Set<TblColRef> rtDimColumns = new HashSet<>();
            for (TupleExpression tupleExpr : dynGroupBy.values()) {
                rtDimColumns.addAll(ExpressionColCollector.collectColumns(tupleExpr));
            }
            Set<TblColRef> rtMetricColumns = new HashSet<>();
            List<DynamicFunctionDesc> dynFuncs = Lists.newLinkedList();
            for (FunctionDesc functionDesc : aggregations) {
                if (functionDesc instanceof DynamicFunctionDesc) {
                    DynamicFunctionDesc dynFunc = (DynamicFunctionDesc) functionDesc;
                    rtMetricColumns.addAll(dynFunc.getMeasureColumnSet());
                    rtDimColumns.addAll(dynFunc.getFilterColumnSet());
                    dynFuncs.add(dynFunc);
                }
            }
            sqlDigest = new SQLDigest(firstTableScan.getTableName(), allColumns, joins, // model
                    groupByColumns, subqueryJoinParticipants, dynGroupBy, groupByExpression, // group by
                    metricsColumns, aggregations, aggrSqlCalls, dynFuncs, // aggregation
                    rtDimColumns, rtMetricColumns, // runtime related columns
                    filterColumns, filter, havingFilter, // filter
                    sortColumns, sortOrders, limitPrecedesAggr, hasLimit, isBorrowedContext, // sort & limit
                    involvedMeasure);
        }
        return sqlDigest;
    }

    public boolean isDynamicColumnEnabled() {
        return olapSchema != null && olapSchema.getProjectInstance().getConfig().isDynamicColumnEnabled();
    }

    public boolean hasPrecalculatedFields() {
        return realization instanceof CubeInstance || realization instanceof HybridInstance;
    }

    public void resetSQLDigest() {
        this.sqlDigest = null;
    }

    public boolean belongToContextTables(TblColRef tblColRef) {
        for (OLAPTableScan olapTableScan : this.allTableScans) {
            if (olapTableScan.getColumnRowType().getAllColumns().contains(tblColRef)) {
                return true;
            }
        }

        return false;
    }

    public boolean belongToFactTableDims(TblColRef tblColRef) {
        if (!belongToContextTables(tblColRef)) {
            return false;
        }
        KylinConfig kylinConfig = olapSchema.getConfig();
        String projectName = olapSchema.getProjectName();
        String factTableName = firstTableScan.getOlapTable().getTableName();
        Set<IRealization> realizations = ProjectManager.getInstance(kylinConfig).getRealizationsByTable(projectName,
                factTableName);
        for (IRealization real : realizations) {
            DataModelDesc model = real.getModel();
            TblColRef.fixUnknownModel(model, tblColRef.getTableRef().getTableIdentity(), tblColRef);

            // cannot be a measure column
            Set<String> metrics = Sets.newHashSet(model.getMetrics());
            if (metrics.contains(tblColRef.getIdentity())) {
                tblColRef.unfixTableRef();
                return false;
            }

            // must belong to a fact table
            for (TableRef factTable : model.getFactTables()) {
                if (factTable.getColumns().contains(tblColRef)) {
                    tblColRef.unfixTableRef();
                    return true;
                }
            }
            tblColRef.unfixTableRef();
        }
        return false;
    }

    public void setReturnTupleInfo(RelDataType rowType, ColumnRowType columnRowType) {
        TupleInfo info = new TupleInfo();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        for (int i = 0; i < fieldList.size(); i++) {
            RelDataTypeField field = fieldList.get(i);
            TblColRef col = columnRowType == null ? null : columnRowType.getColumnByIndex(i);
            info.setField(field.getName(), col, i);
        }
        this.returnTupleInfo = info;
    }

    public void addSort(TblColRef col, SQLDigest.OrderEnum order) {
        if (col != null) {
            sortColumns.add(col);
            sortOrders.add(order);
        }
    }

    public void fixModel(DataModelDesc model, Map<String, String> aliasMap) {
        if (fixedModel)
            return;

        for (OLAPTableScan tableScan : this.allTableScans) {
            tableScan.fixColumnRowTypeWithModel(model, aliasMap);
        }
        fixedModel = true;
    }

    public void unfixModel() {
        if (!fixedModel)
            return;

        for (OLAPTableScan tableScan : this.allTableScans) {
            tableScan.unfixColumnRowTypeWithModel();
        }
        fixedModel = false;
    }

    public void bindVariable(DataContext dataContext) {
        bindVariable(this.filter, dataContext);
    }

    private void bindVariable(TupleFilter filter, DataContext dataContext) {
        if (filter == null) {
            return;
        }

        for (TupleFilter childFilter : filter.getChildren()) {
            bindVariable(childFilter, dataContext);
        }

        if (filter instanceof CompareTupleFilter && dataContext != null) {
            CompareTupleFilter compFilter = (CompareTupleFilter) filter;
            for (Map.Entry<String, Object> entry : compFilter.getVariables().entrySet()) {
                String variable = entry.getKey();
                Object value = dataContext.get(variable);
                if (value != null) {
                    String str = value.toString();
                    str = transferDateTimeColumnToMillis(compFilter, str);
                    compFilter.clearPreviousVariableValues(variable);
                    compFilter.bindVariable(variable, str);
                }

            }
        }
    }

    private String transferDateTimeColumnToMillis(CompareTupleFilter compFilter, String value) {
        TblColRef column = compFilter.getColumn();
        // To fix KYLIN-4157, when using PrepareStatement query, functions within WHERE will cause InternalErrorException
        if (Objects.isNull(column)){
            return value;
        }

        if (column.getType().isDateTimeFamily()){
            value = String.valueOf(DateFormat.stringToMillis(value));
        }
        return value;
    }
    // ============================================================================

    public interface IAccessController {
        void check(List<OLAPContext> contexts, KylinConfig config);
    }

}
