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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.cuboid.NLookupCandidate;
import org.apache.kylin.metadata.cube.model.DimensionRangeInfo;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.graph.JoinsGraph;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.query.schema.OlapSchema;
import org.apache.kylin.query.schema.OlapTable;
import org.apache.kylin.storage.StorageContext;
import org.apache.logging.log4j.util.Strings;
import org.apache.spark.sql.util.SparderTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;

@Getter
public class OlapContext {

    private static final Logger logger = LoggerFactory.getLogger(OlapContext.class);
    public static final String PRM_ACCEPT_PARTIAL_RESULT = "AcceptPartialResult";
    public static final HashSet<String> UNSUPPORTED_FUNCTION_IN_LOOKUP = new HashSet<>(
            Collections.singleton(FunctionDesc.FUNC_INTERSECT_COUNT));

    private final int id;
    private final StorageContext storageContext;
    // query info
    @Setter
    private OlapSchema olapSchema = null;
    /** To be fact table scan except "select * from lookupTable". */
    @Setter
    private OlapTableScan firstTableScan = null;
    private Set<OlapTableScan> allTableScans = new LinkedHashSet<>();
    private final Set<OlapJoinRel> allOlapJoins = new HashSet<>();
    private TupleInfo returnTupleInfo = null;
    @Setter
    private boolean afterAggregate = false;
    @Setter
    private boolean afterHavingClauseFilter = false;
    @Setter
    private boolean afterLimit = false;
    @Setter
    private boolean limitPrecedesAggr = false;
    @Setter
    private boolean hasWindow = false;
    // cube metadata
    @Setter
    private IRealization realization;
    private final IncapableInfo incapableInfo = new IncapableInfo();
    @Setter
    private Set<TblColRef> allColumns = new HashSet<>();
    private final Set<TblColRef> metricsColumns = new HashSet<>();
    // storage level measure type, on top of which various sql aggr function may apply
    @Setter
    private List<FunctionDesc> aggregations = new ArrayList<>();
    private final Set<TblColRef> filterColumns = new LinkedHashSet<>();
    private final List<JoinDesc> joins = new LinkedList<>();
    // rewrite info
    private final Map<String, RelDataType> rewriteFields = new HashMap<>();
    // hive query
    @Setter
    private String sql = "";
    @Setter
    private boolean isExactlyAggregate = false;
    @Setter
    private boolean hasBitmapMeasure = false;
    @Setter
    private boolean isExactlyFastBitmap = false;
    private boolean fixedModel;
    private final List<SQLDigest.OrderEnum> sortOrders;
    private SQLDigest sqlDigest;
    /** OlapContext's top node(outermost one). */
    @Setter
    private OlapRel topNode = null;
    /**
     * If the join relNode is split into more sub-olapContexts,
     * then record this as the parentOfTopNode, otherwise, it should be null.
     */
    @Setter
    private RelNode parentOfTopNode;
    @Setter
    private int limit = Integer.MAX_VALUE;
    @Setter
    private boolean hasJoin;
    @Setter
    private boolean hasPreCalcJoin;
    @Setter
    private boolean hasAgg;
    @Setter
    private boolean hasSelected;
    @Setter
    private Set<TblColRef> groupByColumns = new LinkedHashSet<>();
    /** Collect inner columns in group keys, only for ComputedColumn recommendation. */
    @Setter
    private Set<TableColRefWithRel> innerGroupByColumns = new LinkedHashSet<>();
    /** Collect inner columns in filter, only for ComputedColumn recommendation. */
    @Setter
    private Set<TableColRefWithRel> innerFilterColumns = new LinkedHashSet<>();
    /**
     * subqueryJoinParticipants will be added to groupByColumns(only
     * when other group by co-exists) and allColumns.
     */
    private final Set<TblColRef> subqueryJoinParticipants = new HashSet<>();
    /** Join keys in the direct outer join (without agg, union etc. in between). */
    private final Set<TblColRef> outerJoinParticipants = new HashSet<>();
    /** Aggregations like min(2),max(2),avg(2), not including count(1). */
    private final List<FunctionDesc> constantAggregations = new ArrayList<>();
    private final List<RexNode> expandedFilterConditions = new LinkedList<>();
    private final List<OlapFilterRel> allFilterRels = new LinkedList<>();
    /**
     * Tables without `not null` filters can be optimized for graph matching in the query,
     * see configuration item `kylin.query.join-match-optimization-enabled`.
     */
    private final Set<TableRef> notNullTables = new HashSet<>();
    @Setter
    private JoinsGraph joinsGraph;
    @Setter
    private List<TblColRef> sortColumns;
    private final Set<String> containedNotSupportedFunc = new HashSet<>();
    @Setter
    private Map<TblColRef, TblColRef> groupCCColRewriteMapping = new HashMap<>();
    @Setter
    private boolean needToManyDerived;
    @Setter
    private String boundedModelAlias;

    public OlapContext(int seq) {
        this.id = seq;
        this.storageContext = new StorageContext(seq);
        this.sortColumns = Lists.newArrayList();
        this.sortOrders = Lists.newArrayList();
    }

    public boolean isConstantQuery() {
        return allColumns.isEmpty() && aggregations.isEmpty();
    }

    /**
     * Deal with probing query like: select min(2+2), max(2) from Table.
     */
    public boolean isConstantQueryWithAggregations() {
        return allColumns.isEmpty() && aggregations.isEmpty() && !constantAggregations.isEmpty();
    }

    public SQLDigest getSQLDigest() {
        if (sqlDigest == null) {
            sqlDigest = new SQLDigest(firstTableScan.getTableName(), Sets.newHashSet(allColumns),
                    Lists.newLinkedList(joins), // model
                    Lists.newArrayList(groupByColumns), //
                    Sets.newHashSet(subqueryJoinParticipants), // group by
                    Sets.newHashSet(metricsColumns), //
                    Lists.newArrayList(aggregations), // aggregation
                    Sets.newLinkedHashSet(filterColumns), // filter
                    Lists.newArrayList(sortColumns), Lists.newArrayList(sortOrders), // sort
                    limit, limitPrecedesAggr // limit
            );
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
        NLayoutCandidate candidate = storageContext.getBatchCandidate();
        if (candidate.isEmpty()) {
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
        for (OlapTableScan olapTableScan : this.allTableScans) {
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

        for (OlapTableScan tableScan : this.allTableScans) {
            tableScan.fixColumnRowTypeWithModel(model, aliasMap);
        }
        fixedModel = true;
    }

    public void unfixModel() {
        if (!fixedModel)
            return;

        for (OlapTableScan tableScan : this.allTableScans) {
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
        this.hasJoin = false;
        this.hasPreCalcJoin = false;
        this.hasAgg = false;
        this.hasWindow = false;

        this.allColumns.clear();
        this.groupByColumns.clear();
        this.subqueryJoinParticipants.clear();
        this.metricsColumns.clear();
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

    /**
     * For streaming dataflow and fusion model, use streaming layout candidate of storage context.
     */
    public boolean isAnsweredByTableIndex() {
        NLayoutCandidate candidate;
        if (this.realization.isStreaming()) {
            candidate = this.storageContext.getStreamCandidate();
        } else {
            candidate = this.storageContext.getBatchCandidate();
        }
        return candidate.isTableIndex();
    }

    /**
     * Only used for recommendation or modeling.
     */
    public void simplify() {
        if (firstTableScan != null) {
            firstTableScan = firstTableScan.cleanRelOptCluster();
        }
        Set<OlapTableScan> simplifiedTableScans = Sets.newHashSet();
        allTableScans.forEach(olapTableScan -> olapTableScan.getCluster().getPlanner().clear());
        allTableScans.forEach(olapTableScan -> simplifiedTableScans.add(olapTableScan.cleanRelOptCluster()));
        this.allTableScans = simplifiedTableScans;
    }

    @Override
    public String toString() {
        return "OlapContext{firstTableScan=" + firstTableScan //
                + ", allTableScans=" + allTableScans //
                + ", allOlapJoins=" + allOlapJoins //
                + ", groupByColumns=" + groupByColumns //
                + ", innerGroupByColumns=" + innerGroupByColumns //
                + ", innerFilterColumns=" + innerFilterColumns //
                + ", aggregations=" + aggregations //
                + ", filterColumns=" + filterColumns + '}';
    }

    public boolean isBoundedModel(NDataModel model) {
        if (boundedModelAlias == null) {
            // If there is no bound model, any model can be bound to the OlapContext
            return true;
        }
        return StringUtils.equalsIgnoreCase(model.getAlias(), boundedModelAlias);
    }

    public Map<String, String> matchJoins(NDataModel model, boolean isInnerJoinPartial, boolean isNonEquivJoinPartial) {

        if (joinsGraph == null) {
            joinsGraph = new JoinsGraph(firstTableScan.getTableRef(), joins);
        }

        Map<String, String> tableAliasMap = new HashMap<>();
        boolean matched = joinsGraph.match(model.getJoinsGraph(), tableAliasMap, isInnerJoinPartial,
                isNonEquivJoinPartial);
        if (!matched) {
            logger.debug("Context joinsGraph missed model {}, model join graph {}", model, model.getJoinsGraph());
            logger.debug("Mismatch nodes - Context {}, Model {}", joinsGraph.unmatched(model.getJoinsGraph()),
                    model.getJoinsGraph().unmatched(joinsGraph));
            if (olapSchema.getConfig().isJoinMatchOptimizationEnabled()) {
                String project = model.getProject();
                String mAlias = model.getAlias();
                logger.info("Rewrite the joinsGraph by filter conditions and match with model({}/{}).", project,
                        mAlias);
                this.transformJoinsGraphByFilterConditions();
                matched = joinsGraph.match(model.getJoinsGraph(), tableAliasMap, isInnerJoinPartial,
                        isNonEquivJoinPartial);
                logger.info("Match result of rewritten joinsGraph of model({}/{}): {}", project, mAlias, matched);
            }
        }

        if (!matched) {
            incapableInfo.addIncapableReason(model, IncapableInfo.Type.MODEL_UNMATCHED_JOIN);
            tableAliasMap.clear();
        }
        return tableAliasMap;
    }

    public boolean isInvalidContext() {
        return !allTableScans.isEmpty() && joins.size() != allTableScans.size() - 1;
    }

    public void markInvalid() {
        incapableInfo.addIncapableReason(IncapableInfo.Type.BAD_OLAP_CONTEXT);
    }

    public static final String SEP = System.getProperty("line.separator");
    public static final String INDENT = "  ";

    public static final String olapContextFormat = SEP + "{" + SEP + INDENT + "\"Fact Table\" = \"%s\"," + SEP + INDENT
            + "\"Dimension Tables\" = [%s]," + SEP + INDENT + "\"Recommend Dimension(Group by)\" = [%s]," + SEP + INDENT
            + "\"Recommend Dimension(Filter cond)\" = [%s]," + SEP + INDENT + "\"Measures\" = [%s]," + SEP + "}" + SEP;

    public String tipsForUser() {
        Set<String> allTables = allTableScans.stream().map(OlapTableScan::getTableName).collect(Collectors.toSet());
        if (!allTables.isEmpty() && firstTableScan != null) {
            allTables.remove(firstTableScan.getTableName());
            return String.format(Locale.ROOT, olapContextFormat, firstTableScan.getTableName(),
                    Strings.join(allTables.stream().map(c -> "\"" + c + "\"").iterator(), ','),
                    Strings.join(
                            groupByColumns.stream().map(c -> "\"" + c.getColumnWithTableAndSchema() + "\"").iterator(),
                            ','),
                    Strings.join(
                            filterColumns.stream().map(c -> "\"" + c.getColumnWithTableAndSchema() + "\"").iterator(),
                            ','),
                    Strings.join(aggregations.stream().map(c -> "\"" + c.getFullExpression() + "\"").iterator(), ','));
        } else {
            return "empty";
        }
    }

    public String toHumanReadString() {
        String r = realization == null ? "not matched" : realization.getCanonicalName();
        return "{id = " + id + ", model = " + r + ", fact table = "
                + (firstTableScan != null ? firstTableScan.getTableName() : "?") + "}";
    }

    private void transformJoinsGraphByFilterConditions() {
        Set<TableRef> leftOrInnerTables = getNotNullTables();
        if (CollectionUtils.isNotEmpty(leftOrInnerTables)) {
            for (JoinDesc join : joins) {
                if (leftOrInnerTables.contains(join.getPKSide())) {
                    joinsGraph.setJoinToLeftOrInner(join);
                    logger.info("Current join: {} is set to LEFT_OR_INNER", join);
                }
            }
        }
        joinsGraph.normalize();
    }

    public NLookupCandidate.Policy deduceLookupTableType() {
        NLookupCandidate.Policy policy = NLookupCandidate.Policy.NONE;
        boolean noUnsupportedAgg = getSQLDigest().getAggregations().stream()
                .noneMatch(fun -> UNSUPPORTED_FUNCTION_IN_LOOKUP.contains(fun.getExpression()));
        boolean noCc = getSQLDigest().getAllColumns().stream().noneMatch(col -> col.getColumnDesc().isComputedColumn());
        if (noUnsupportedAgg && noCc && getSQLDigest().getJoinDescs().isEmpty()) {
            KylinConfig olapConfig = olapSchema.getConfig();
            String project = olapSchema.getProject();
            String factTable = getSQLDigest().getFactTable();
            NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(olapConfig, project);
            TableDesc tableDesc = tableMgr.getTableDesc(factTable);
            if (tableDesc == null) {
                return policy;
            }
            if (olapConfig.isInternalTableEnabled() && tableDesc.isHasInternal() && !isAsyncQuery(olapConfig)) {
                logger.info("Hit internal table {}", factTable);
                policy = getSQLDigest().isDigestOfRawQuery()//
                        ? NLookupCandidate.Policy.INTERNAL_TABLE
                        : NLookupCandidate.Policy.AGG_THEN_INTERNAL_TABLE;
            } else if (!olapConfig.isInternalTableEnabled() && !StringUtils.isBlank(tableDesc.getLastSnapshotPath())) {
                logger.info("Hit the snapshot {}, the path is: {}", factTable, tableDesc.getLastSnapshotPath());
                policy = getSQLDigest().isDigestOfRawQuery() //
                        ? NLookupCandidate.Policy.SNAPSHOT
                        : NLookupCandidate.Policy.AGG_THEN_SNAPSHOT;
            }
        }
        return policy;
    }

    public boolean isAsyncQuery(KylinConfig olapConfig) {
        return QueryContext.current().getQueryTagInfo().isAsyncQuery() && olapConfig.isUniqueAsyncQueryYarnQueue();
    }

    public String incapableMsg() {
        StringBuilder buf = new StringBuilder("OlapContext");
        if (incapableInfo.getReason() != null) {
            buf.append(", ").append(incapableInfo.getReason());
        }
        for (Set<IncapableInfo.Type> types : incapableInfo.getReasons().values()) {
            types.forEach(type -> buf.append(", ").append(type));
        }

        buf.append(", ").append(firstTableScan);
        joins.forEach(join -> buf.append(", ").append(join));
        return buf.toString();
    }

    public String genExecFunc(OlapRel rel) {
        setReturnTupleInfo(rel.getRowType(), rel.getColumnRowType());

        if (isConstantQueryWithAggregations()) {
            return "executeSimpleAggregationQuery";
        }

        // If the table being scanned is not a fact table, then it is a lookup table.
        if (this.getStorageContext().getLookupCandidate() != null) {
            return "executeLookupTableQuery";
        }

        if (canMinMaxDimAnsweredByMetadata(rel, true)) {
            return "executeMetadataQuery";
        }

        return "executeOlapQuery";
    }

    public boolean checkOlapContextAnsweredByMetadata() {
        OlapRel aggChildTableOrJoin = getAggChildTableOrJoin(topNode);
        return (null != aggChildTableOrJoin) && canMinMaxDimAnsweredByMetadata(aggChildTableOrJoin, false);
    }

    private OlapRel getAggChildTableOrJoin(RelNode parentNode) {
        if (CollectionUtils.isEmpty(parentNode.getInputs())) {
            return null;
        }
        if (!(parentNode instanceof OlapAggregateRel)) {
            return getAggChildTableOrJoin(parentNode.getInput(0));
        }
        RelNode aggChildNode = parentNode.getInput(0);
        if (!(aggChildNode instanceof OlapProjectRel)) {
            return getAggChildTableOrJoin(aggChildNode);
        }
        RelNode projectChildNode = aggChildNode.getInput(0);
        if ((projectChildNode instanceof OlapTableScan) || (projectChildNode instanceof OlapJoinRel)) {
            return (OlapRel) projectChildNode;
        }
        return getAggChildTableOrJoin(projectChildNode);
    }

    private boolean canMinMaxDimAnsweredByMetadata(OlapRel rel, boolean resetAggWhenMatch) {
        if (!KylinConfig.getInstanceFromEnv().isRouteToMetadataEnabled()) {
            return false;
        }

        if (!(realization instanceof NDataflow) || !(rel instanceof OlapJoinRel || rel instanceof OlapTableScan)) {
            logger.info("Can't route to metadata, the realization is {} and this OlapRel is {}", realization, rel);
            return false;
        }

        /*
         * Find the target pattern as shown below.
         *       (other rel)
         *            |
         *           Agg
         *            |
         *          Project
         *            |
         *   (TableScan or JoinRel)
         */
        List<OlapRel> relStack = new ArrayList<>();
        OlapRel current = this.topNode;
        while (current != rel && current.getInputs().size() == 1 && current.getInput(0) instanceof OlapRel) {
            relStack.add(current);
            current = (OlapRel) current.getInput(0);
        }
        if (current != rel || relStack.size() < 2 || !(relStack.get(relStack.size() - 1) instanceof OlapProjectRel)
                || !(relStack.get(relStack.size() - 2) instanceof OlapAggregateRel)) {
            logger.info("Can't route to query metadata, the rel stack is not matched");
            return false;
        }

        OlapAggregateRel aggregateRel = (OlapAggregateRel) relStack.get(relStack.size() - 2);
        if (aggregateRel.getGroups().size() > 1
                || aggregateRel.getGroups().size() == 1 && !TblColRef.InnerDataTypeEnum.LITERAL.getDataType()
                        .equals(aggregateRel.getGroups().get(0).getDatatype())) {
            logger.info("Cannot route to query metadata, only group by constants are supported.");
            return false;
        }

        if (aggregations.isEmpty() || !aggregations.stream().allMatch(agg -> agg.isMin() || agg.isMax())) {
            logger.info("Cannot route to query metadata, only min/max aggregate functions are supported.");
            return false;
        }

        if (aggregations.stream()
                .anyMatch(agg -> TblColRef.InnerDataTypeEnum.contains(agg.getColRefs().get(0).getDatatype()))) {
            logger.info("Cannot route to query metadata, not support min(expression), such as min(id+1)");
            return false;
        }

        if (!Sets.newHashSet(realization.getAllDimensions()).containsAll(allColumns)) {
            logger.info("Cannot route to query metadata, not all columns queried are treated as dimensions of index.");
            return false;
        }

        if (resetAggWhenMatch) {
            // reset rewriteAggCalls to aggCall, to avoid using measures.
            aggregateRel.getRewriteAggCalls().clear();
            aggregateRel.getRewriteAggCalls().addAll(aggregateRel.getAggCallList());
            logger.info("Use kylin metadata to answer query with realization : {}", realization);
        } else {
            logger.info("OlapContext can be answered by kylin metadata with realization : {}", realization);
        }
        return true;
    }

    public List<Object[]> getColValuesRange() {
        return getColValuesRange(false);
    }

    public List<Object[]> getColValuesRange(boolean isCalciteEngine) {
        Preconditions.checkState(realization instanceof NDataflow, "Only support dataflow");
        // As it is a min/max aggregate function, it only has one parameter.
        List<TblColRef> cols = aggregations.stream() //
                .map(FunctionDesc::getColRefs) //
                .filter(tblColRefs -> tblColRefs.size() == 1) //
                .map(tblColRefs -> tblColRefs.get(0)) //
                .collect(Collectors.toList());
        List<TblColRef> allFields = new ArrayList<>();
        allTableScans.forEach(tableScan -> {
            List<TblColRef> colRefs = tableScan.getColumnRowType().getAllColumns();
            allFields.addAll(colRefs);
        });
        RelDataTypeFactory typeFactory = this.getTopNode().getCluster().getTypeFactory();
        List<Object[]> result = new ArrayList<>();
        for (NDataSegment segment : ((NDataflow) realization).getSegments()) {
            if (segment.getStatus() != SegmentStatusEnum.READY) {
                continue;
            }
            Map<String, DimensionRangeInfo> infoMap = segment.getDimensionRangeInfoMap();
            Object[] minList = new Object[allFields.size()];
            Object[] maxList = new Object[allFields.size()];
            for (TblColRef col : cols) {
                int colId = allFields.indexOf(col);
                String tblColRefIndex = getTblColRefIndex(col, realization);
                DimensionRangeInfo rangeInfo = infoMap.get(tblColRefIndex);
                if (rangeInfo == null) {
                    minList[colId] = null;
                    maxList[colId] = null;
                    continue;
                }
                ColumnDesc c = col.getColumnDesc();
                if (isCalciteEngine) {
                    DataType dataType = c.getUpgradedType();
                    minList[colId] = convertToColumnDataType(rangeInfo.getMin(), dataType);
                    maxList[colId] = convertToColumnDataType(rangeInfo.getMax(), dataType);
                } else {
                    RelDataType sqlType = OlapTable.createSqlType(typeFactory, c.getUpgradedType(), c.isNullable());
                    minList[colId] = SparderTypeUtil.convertToStringWithCalciteType(rangeInfo.getMin(), sqlType,
                            false);
                    maxList[colId] = SparderTypeUtil.convertToStringWithCalciteType(rangeInfo.getMax(), sqlType,
                            false);
                }
            }

            result.add(minList);
            result.add(maxList);
        }
        return result;
    }

    private Object convertToColumnDataType(String strVal, DataType dataType) {
        String dataTypeName = dataType.getName();
        Object value = Tuple.convertOptiqCellValue(strVal, dataTypeName);
        if ("decimal".equals(dataTypeName)) {
            BigDecimal decimalVal = (BigDecimal) value;
            value = decimalVal.setScale(dataType.getScale(), RoundingMode.HALF_EVEN);
        }
        return value;
    }

    private String getTblColRefIndex(TblColRef colRef, IRealization df) {
        NDataModel model = df.getModel();
        return String.valueOf(model.getColumnIdByColumnName(colRef.getAliasDotName()));
    }

    @Getter
    public static class IncapableInfo {
        private Type reason;
        private final Map<NDataModel, Set<Type>> reasons = Maps.newHashMap();

        public void addIncapableReason(Type reason) {
            this.reason = reason;

        }

        public void addIncapableReason(NDataModel model, Type reason) {
            reasons.putIfAbsent(model, new LinkedHashSet<>());
            reasons.get(model).add(reason);
        }

        public enum Type {
            MODEL_UNMATCHED_JOIN, //
            BAD_OLAP_CONTEXT
        }
    }

}
