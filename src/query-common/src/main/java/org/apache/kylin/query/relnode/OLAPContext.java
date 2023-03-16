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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.realization.HybridRealization;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.graph.JoinsGraph;
import org.apache.kylin.metadata.query.NativeQueryRealization;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.query.routing.RealizationCheck;
import org.apache.kylin.query.schema.OLAPSchema;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

/**
 */
public class OLAPContext {

    public static final String PRM_ACCEPT_PARTIAL_RESULT = "AcceptPartialResult";
    static final ThreadLocal<Map<String, String>> _localPrarameters = new ThreadLocal<>();
    static final ThreadLocal<Map<Integer, OLAPContext>> _localContexts = new ThreadLocal<>();
    private static final Logger logger = LoggerFactory.getLogger(OLAPContext.class);
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
    public boolean hasWindow = false;
    // cube metadata
    public IRealization realization;
    public RealizationCheck realizationCheck = new RealizationCheck();
    public Set<TblColRef> allColumns = new HashSet<>();
    public Set<TblColRef> metricsColumns = new HashSet<>();
    public List<FunctionDesc> aggregations = new ArrayList<>(); // storage level measure type, on top of which various sql aggr function may apply
    public Set<TblColRef> filterColumns = new LinkedHashSet<>();
    public List<JoinDesc> joins = new LinkedList<>();
    // rewrite info
    public Map<String, RelDataType> rewriteFields = new HashMap<>();
    // hive query
    public String sql = "";
    protected boolean isExactlyAggregate = false;
    @Getter
    protected boolean hasBitmapMeasure = false;
    protected boolean isExactlyFastBitmap = false;
    boolean afterTopJoin = false;
    boolean fixedModel;
    List<SQLDigest.OrderEnum> sortOrders;
    SQLDigest sqlDigest;
    @Setter
    @Getter
    private OLAPRel topNode = null; // the context's toppest node
    @Setter
    @Getter
    private RelNode parentOfTopNode = null; // record the JoinRel that cuts off its children into new context(s), in other case it should be null
    @Setter
    @Getter
    private int limit = Integer.MAX_VALUE;
    @Setter
    @Getter
    private boolean hasJoin = false;
    @Setter
    @Getter
    private boolean hasPreCalcJoin = false;
    @Setter
    @Getter
    private boolean hasAgg = false;
    @Getter
    @Setter
    private boolean hasSelected = false;
    @Setter
    @Getter
    private Set<TblColRef> groupByColumns = Sets.newLinkedHashSet();
    @Setter
    @Getter
    // collect inner columns in group keys
    // this filed is used by CC proposer only
    private Set<TableColRefWithRel> innerGroupByColumns = Sets.newLinkedHashSet();
    @Setter
    @Getter
    // collect inner columns in filter
    // this filed is used by CC proposer only
    private Set<TblColRef> innerFilterColumns = Sets.newLinkedHashSet();
    @Setter
    @Getter
    private Set<TblColRef> subqueryJoinParticipants = new HashSet<>();//subqueryJoinParticipants will be added to groupByColumns(only when other group by co-exists) and allColumns
    @Setter
    @Getter
    private Set<TblColRef> outerJoinParticipants = new HashSet<>();// join keys in the direct outer join (without agg, union etc in between)
    @Setter
    @Getter
    private List<FunctionDesc> constantAggregations = new ArrayList<>(); // agg like min(2),max(2),avg(2), not including count(1)
    @Getter
    private List<RexNode> expandedFilterConditions = new LinkedList<>();
    @Getter
    private Set<TableRef> notNullTables = new HashSet<>(); // tables which have not null filter(s), can be used in join-match-optimization
    @Getter
    @Setter
    private JoinsGraph joinsGraph;
    @Getter
    @Setter
    private List<TblColRef> sortColumns;
    @Setter
    @Getter
    private Set<String> containedNotSupportedFunc = Sets.newHashSet();
    @Getter
    @Setter
    private Map<TblColRef, TblColRef> groupCCColRewriteMapping = new HashMap<>();
    @Getter
    @Setter
    private boolean hasAdminPermission = false;
    @Setter
    @Getter
    private boolean needToManyDerived;
    @Setter
    @Getter
    private String modelAlias;

    public OLAPContext(int seq) {
        this.id = seq;
        this.storageContext = new StorageContext(seq);
        this.sortColumns = Lists.newArrayList();
        this.sortOrders = Lists.newArrayList();
    }

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

    public static void clearThreadLocalContextById(int id) {
        Map<Integer, OLAPContext> map = _localContexts.get();
        map.remove(id);
        _localContexts.set(map);
    }

    public static List<NativeQueryRealization> getNativeRealizations() {
        List<NativeQueryRealization> realizations = Lists.newArrayList();

        // contexts can be null in case of 'explain plan for'
        if (getThreadLocalContexts() == null) {
            return realizations;
        }

        for (OLAPContext ctx : getThreadLocalContexts()) {
            if (ctx.realization == null) {
                continue;
            }

            final String realizationType;
            Set<String> tableSets = Sets.newHashSet();
            if (ctx.storageContext.isEmptyLayout() && ctx.storageContext.isFilterCondAlwaysFalse()) {
                realizationType = QueryMetrics.FILTER_CONFLICT;
            } else if (ctx.storageContext.isEmptyLayout()) {
                realizationType = null;
            } else if (ctx.storageContext.isUseSnapshot()) {
                realizationType = QueryMetrics.TABLE_SNAPSHOT;
                tableSets.add(ctx.getFirstTableIdentity());
            } else if (!ctx.storageContext.getCandidate().isEmptyCandidate()
                    && ctx.storageContext.getCandidate().getLayoutEntity().getIndex().isTableIndex()) {
                realizationType = QueryMetrics.TABLE_INDEX;
                addTableSnapshots(tableSets, ctx);
            } else {
                realizationType = QueryMetrics.AGG_INDEX;
                addTableSnapshots(tableSets, ctx);
            }

            val ctxRealizationModel = ctx.realization.getModel();
            String modelId = ctxRealizationModel.getUuid();
            //use fusion model alias
            String modelAlias = ctxRealizationModel.getFusionModelAlias();

            List<String> snapshots = Lists.newArrayList(tableSets);

            if (ctx.storageContext.getStreamingLayoutId() != -1L) {
                realizations.add(getStreamingNativeRealization(ctx, realizationType, modelId, modelAlias, snapshots));

                if (ctx.realization instanceof HybridRealization) {
                    String batchModelId = ((HybridRealization) ctx.realization).getBatchRealization().getUuid();
                    realizations
                            .add(getBatchNativeRealization(ctx, realizationType, batchModelId, modelAlias, snapshots));
                }

            } else {
                realizations.add(getBatchNativeRealization(ctx, realizationType, modelId, modelAlias, snapshots));
            }
        }
        return realizations;
    }

    private static NativeQueryRealization getStreamingNativeRealization(OLAPContext ctx, String realizationType,
            String modelId, String modelAlias, List<String> snapshots) {
        val streamingRealization = new NativeQueryRealization(modelId, modelAlias,
                ctx.storageContext.getStreamingLayoutId(), realizationType, ctx.storageContext.isPartialMatchModel(),
                snapshots);
        streamingRealization.setSecondStorage(QueryContext.current().getSecondStorageUsageMap()
                .getOrDefault(streamingRealization.getLayoutId(), false));
        streamingRealization.setStreamingLayout(true);
        return streamingRealization;
    }

    private static NativeQueryRealization getBatchNativeRealization(OLAPContext ctx, String realizationType,
            String modelId, String modelAlias, List<String> snapshots) {
        val realization = new NativeQueryRealization(modelId, modelAlias, ctx.storageContext.getLayoutId(),
                realizationType, ctx.storageContext.isPartialMatchModel(), snapshots);
        realization.setSecondStorage(
                QueryContext.current().getSecondStorageUsageMap().getOrDefault(realization.getLayoutId(), false));
        realization.setRecommendSecondStorage(
                recommendSecondStorage(ctx.realization.getProject(), modelId, realizationType));
        return realization;
    }

    private static void addTableSnapshots(Set<String> tableSets, OLAPContext ctx) {
        tableSets.addAll(ctx.storageContext.getCandidate().getDerivedTableSnapshots());
    }

    private static boolean recommendSecondStorage(String project, String modelId, String realizationType) {
        return QueryMetrics.TABLE_INDEX.equals(realizationType) && SecondStorageUtil.isProjectEnable(project)
                && !SecondStorageUtil.isModelEnable(project, modelId);
    }

    public static RexInputRef createUniqueInputRefAmongTables(OLAPTableScan table, int columnIdx,
            Collection<OLAPTableScan> tables) {
        List<TableScan> sorted = new ArrayList<>(tables);
        sorted.sort(Comparator.comparingInt(AbstractRelNode::getId));
        int offset = 0;
        for (TableScan tableScan : sorted) {
            if (tableScan == table) {
                return new RexInputRef(
                        table.getTableName() + "." + table.getRowType().getFieldList().get(columnIdx).getName(),
                        offset + columnIdx, table.getRowType().getFieldList().get(columnIdx).getType());
            }
            offset += tableScan.getRowType().getFieldCount();
        }
        return null;
    }

    public boolean isExactlyAggregate() {
        return isExactlyAggregate;
    }

    public void setExactlyAggregate(boolean exactlyAggregate) {
        isExactlyAggregate = exactlyAggregate;
    }

    public boolean isExactlyFastBitmap() {
        return isExactlyFastBitmap;
    }

    public void setExactlyFastBitmap(boolean isExactlyFastBitmap) {
        this.isExactlyFastBitmap = isExactlyFastBitmap;
    }

    public void setHasBitmapMeasure(boolean bitmapMeasure) {
        hasBitmapMeasure = bitmapMeasure;
    }

    public boolean isConstantQuery() {
        return allColumns.isEmpty() && aggregations.isEmpty();
    }

    public boolean isConstantQueryWithAggregations() {
        // deal with probing query like select min(2+2), max(2) from Table
        return allColumns.isEmpty() && aggregations.isEmpty() && !constantAggregations.isEmpty();
    }

    public SQLDigest getSQLDigest() {
        if (sqlDigest == null) {
            sqlDigest = new SQLDigest(firstTableScan.getTableName(), Sets.newHashSet(allColumns),
                    Lists.newLinkedList(joins), // model
                    Lists.newArrayList(groupByColumns), Sets.newHashSet(subqueryJoinParticipants), // group by
                    Sets.newHashSet(metricsColumns), Lists.newArrayList(aggregations), // aggregation
                    Sets.newLinkedHashSet(filterColumns), // filter
                    Lists.newArrayList(sortColumns), Lists.newArrayList(sortOrders), limit, limitPrecedesAggr, // sort & limit
                    Sets.newHashSet(involvedMeasure));
        }
        return sqlDigest;
    }

    public String getFirstTableIdentity() {
        return firstTableScan.getTableRef().getTableIdentity();
    }

    public boolean isFirstTableLookupTableInModel(NDataModel model) {
        return joins.isEmpty() && model.isLookupTable(getFirstTableIdentity());
    }

    public boolean hasPrecalculatedFields() {
        NLayoutCandidate candidate = storageContext.getCandidate();
        if (candidate.isEmptyCandidate()) {
            return false;
        }
        boolean isTableIndex = candidate.getLayoutEntity().getIndex().isTableIndex();
        boolean isLookupTable = isFirstTableLookupTableInModel(realization.getModel());
        return !isTableIndex && !isLookupTable;
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

    public boolean isOriginAndBelongToCtxTables(TblColRef tblColRef) {
        return belongToContextTables(tblColRef) && !tblColRef.getName().startsWith("_KY_");
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

    public void fixModel(NDataModel model, Map<String, String> aliasMap) {
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

    public void clearCtxInfo() {
        //query info
        this.afterAggregate = false;
        this.afterHavingClauseFilter = false;
        this.afterLimit = false;
        this.limitPrecedesAggr = false;
        this.afterTopJoin = false;
        this.hasJoin = false;
        this.hasPreCalcJoin = false;
        this.hasAgg = false;
        this.hasWindow = false;

        this.allColumns.clear();
        this.groupByColumns.clear();
        this.subqueryJoinParticipants.clear();
        this.metricsColumns.clear();
        this.involvedMeasure.clear();
        this.allOlapJoins.clear();
        this.joins.clear();
        this.allTableScans.clear();
        this.filterColumns.clear();

        this.aggregations.clear();

        this.sortColumns.clear();
        this.sortOrders.clear();

        this.joinsGraph = null;

        this.sqlDigest = null;
        this.getConstantAggregations().clear();
    }

    public void addInnerGroupColumns(KapRel rel, Collection<TblColRef> innerGroupColumns) {
        Set<TblColRef> innerGroupColumnsSet = new HashSet<>(innerGroupColumns);
        for (TblColRef tblColRef : innerGroupColumnsSet) {
            this.innerGroupByColumns.add(new TableColRefWithRel(rel, tblColRef));
        }
    }

    // For streaming dataflow and fusion model, use streaming layout candidate of storage context
    public boolean isAnsweredByTableIndex() {
        NLayoutCandidate candidate;
        if (this.realization.isStreaming()) {
            candidate = this.storageContext.getStreamingCandidate();
        } else {
            candidate = this.storageContext.getCandidate();
        }
        return candidate != null && !candidate.isEmptyCandidate()
                && candidate.getLayoutEntity().getIndex().isTableIndex();
    }

    /**
     * Only used for recommendation or modeling.
     */
    public void simplify() {
        if (firstTableScan != null) {
            firstTableScan = firstTableScan.cleanRelOptCluster();
        }
        Set<OLAPTableScan> simplifiedTableScans = Sets.newHashSet();
        allTableScans.forEach(olapTableScan -> olapTableScan.getCluster().getPlanner().clear());
        allTableScans.forEach(olapTableScan -> simplifiedTableScans.add(olapTableScan.cleanRelOptCluster()));
        this.allTableScans = simplifiedTableScans;
    }

    /**
     * It's very dangerous, only used for recommendation or modeling.
     */
    public void clean() {
        topNode = null;
        parentOfTopNode = null;
        allOlapJoins.clear();
    }

    @Override
    public String toString() {
        return "OLAPContext{" + "firstTableScan=" + firstTableScan + ", allTableScans=" + allTableScans
                + ", allOlapJoins=" + allOlapJoins + ", groupByColumns=" + groupByColumns + ", innerGroupByColumns="
                + innerGroupByColumns + ", innerFilterColumns=" + innerFilterColumns + ", aggregations=" + aggregations
                + ", filterColumns=" + filterColumns + '}';
    }

    public void matchJoinWithFilterTransformation() {
        Set<TableRef> leftOrInnerTables = getNotNullTables();
        if (CollectionUtils.isEmpty(leftOrInnerTables)) {
            return;
        }

        for (JoinDesc join : joins) {
            if (leftOrInnerTables.contains(join.getPKSide())) {
                joinsGraph.setJoinToLeftOrInner(join);
                logger.info("Current join: {} is set to LEFT_OR_INNER", join);
            }
        }
    }

    public void matchJoinWithEnhancementTransformation() {
        joinsGraph.normalize();
    }

    public RexInputRef createUniqueInputRefContextTables(OLAPTableScan table, int columnIdx) {
        return createUniqueInputRefAmongTables(table, columnIdx, allTableScans);
    }

    public interface IAccessController {
        void check(List<OLAPContext> contexts, OLAPRel tree, KylinConfig config);
    }
}
