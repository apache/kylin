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

package org.apache.kylin.rec.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableBiMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.cuboid.ChooserContext;
import org.apache.kylin.metadata.cube.cuboid.ComparatorUtils;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexEntity.IndexIdentifier;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.model.AntiFlatChecker;
import org.apache.kylin.metadata.model.ColExcludedChecker;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.recommendation.entity.DimensionRecItemV2;
import org.apache.kylin.metadata.recommendation.util.RawRecUtil;
import org.apache.kylin.query.exception.NotSupportedSQLException;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.exception.PendingException;
import org.apache.kylin.rec.model.ModelTree;
import org.apache.kylin.rec.util.CubeUtils;
import org.apache.kylin.rec.util.EntityBuilder;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IndexSuggester {

    public static final String COMPUTED_COLUMN_ON_ANTI_FLATTEN_LOOKUP = "Computed column depends on anti flatten lookup table, stop the process of generate index.";
    public static final String COMPUTED_COLUMN_OF_EXCLUDED_COLUMNS = "Computed columns depends on excluded columns, stop the process of generate index.";
    public static final String MEASURE_ON_ANTI_FLATTEN_LOOKUP = "Unsupported measure of anti flatten lookup table, stop the process of generate index. ";
    public static final String MEASURE_OF_EXCLUDED_COLUMNS = "Unsupported measure of excluded columns, stop the process of generate index. ";
    public static final String OTHER_UNSUPPORTED_MEASURE = "Unsupported measure may caused by turning on only reusing used defined computed column.";
    public static final String FK_ON_ANTI_FLATTEN_LOOKUP = "Unsupported foreign join key of anti flatten lookup table, stop the process of generate index. The foreign key is: ";
    public static final String FK_OF_EXCLUDED_COLUMNS = "Unsupported foreign join key of excluded columns, stop the process of generate index. The foreign key is: ";
    private static final String COLUMN_NOT_FOUND_PTN = "The model [%s] matches this query, but the dimension [%s] is missing. ";
    private static final String MEASURE_NOT_FOUND_PTN = "The model [%s] matches this query, but the measure [%s] is missing. ";
    private static final String JOIN_NOT_MATCHED = "The join of model [%s] has some difference with the joins of this query. ";
    private final AbstractContext proposeContext;
    private final AbstractContext.ModelContext modelContext;
    private final IndexPlan indexPlan;
    private final NDataModel model;

    private final Map<FunctionDesc, Integer> aggFuncIdMap;
    private final Map<IndexIdentifier, IndexEntity> collector;
    private final SortedSet<Long> layoutIds = Sets.newTreeSet();
    private final Set<String> additionalNamedColumns = new HashSet<>();

    IndexSuggester(AbstractContext.ModelContext modelContext, IndexPlan indexPlan,
            Map<IndexIdentifier, IndexEntity> collector) {

        this.modelContext = modelContext;
        this.proposeContext = modelContext.getProposeContext();
        this.model = modelContext.getTargetModel();
        this.indexPlan = indexPlan;
        this.collector = collector;

        aggFuncIdMap = Maps.newHashMap();
        model.getEffectiveMeasures().forEach((measureId, measure) -> {
            FunctionDesc function = measure.getFunction();
            aggFuncIdMap.put(function, measureId);
        });

        collector.forEach((indexIdentifier, indexEntity) -> indexEntity.getLayouts()
                .forEach(layout -> layoutIds.add(layout.getId())));
    }

    void suggestIndexes(ModelTree modelTree) {
        KylinConfig kylinConfig = proposeContext.getKapConfig().getKylinConfig();
        boolean partialMatch = kylinConfig.isQueryMatchPartialInnerJoinModel();
        boolean nonEquiPartialMatch = kylinConfig.partialMatchNonEquiJoins();

        for (OlapContext ctx : modelTree.getOlapContexts()) {

            // check keySet of sql2AccelerateInfo contains ctx.getSql()
            AccelerateInfo accelerateInfo = proposeContext.getAccelerateInfoMap().get(ctx.getSql());
            Preconditions.checkNotNull(accelerateInfo);
            if (accelerateInfo.isNotSucceed()) {
                continue;
            }

            try {
                if (CollectionUtils.isNotEmpty(ctx.getContainedNotSupportedFunc())) {
                    throw new NotSupportedSQLException(
                            StringUtils.join(ctx.getContainedNotSupportedFunc(), ", ") + " function not supported");
                }

                Map<String, String> aliasMap = ctx.matchJoins(model, partialMatch, nonEquiPartialMatch);
                if (MapUtils.isEmpty(aliasMap)) {
                    throw new PendingException(String.format(Locale.ROOT,
                            getMsgTemplateByModelMaintainType(JOIN_NOT_MATCHED, Type.TABLE), model.getAlias()));
                }
                ctx.fixModel(model, aliasMap);
                AccelerateInfo.QueryLayoutRelation queryLayoutRelation = ingest(ctx, model);
                accelerateInfo.getRelatedLayouts().add(queryLayoutRelation);
            } catch (Exception e) {
                log.error("Unable to suggest cuboid for IndexPlan", e);
                // under expert mode
                if (e instanceof PendingException) {
                    accelerateInfo.setPendingMsg(e.getMessage());
                } else {
                    accelerateInfo.setFailedCause(e);
                }
                accelerateInfo.getRelatedLayouts().clear();
            } finally {
                ctx.unfixModel();
            }
        }
    }

    private List<Integer> suggestShardBy(List<Integer> sortedDimIds) {
        // for recommend dims order by filterLevel and Cardinality
        List<Integer> shardBy = Lists.newArrayList();
        if (CollectionUtils.isEmpty(sortedDimIds))
            return shardBy;

        TblColRef colRef = model.getEffectiveCols().get(sortedDimIds.get(0));
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                proposeContext.getProject());
        TableExtDesc.ColumnStats colStats = TableExtDesc.ColumnStats.getColumnStats(tableMgr, colRef);
        if (colStats != null
                && colStats.getCardinality() > proposeContext.getSmartConfig().getRowkeyUHCCardinalityMin()) {
            shardBy.add(sortedDimIds.get(0));
        }
        return shardBy;
    }

    private AccelerateInfo.QueryLayoutRelation ingest(OlapContext ctx, NDataModel model) {

        List<Integer> dimIds;
        SortedSet<Integer> measureIds;
        IndexEntity indexEntity;
        if (ctx.getSQLDigest().isRawQuery) {
            dimIds = suggestTableIndexDimensions(ctx);
            measureIds = Sets.newTreeSet();
            indexEntity = createIndexEntity(suggestDescId(true), dimIds, measureIds);
        } else {
            dimIds = suggestAggIndexDimensions(ctx);
            measureIds = suggestMeasures(dimIds, ctx);
            indexEntity = createIndexEntity(suggestDescId(false), dimIds, measureIds);
        }

        final IndexIdentifier cuboidIdentifier = indexEntity.createIndexIdentifier();
        if (collector.containsKey(cuboidIdentifier)) {
            indexEntity = collector.get(cuboidIdentifier);
        } else {
            collector.put(cuboidIdentifier, indexEntity);
        }

        if (model.getDataStorageType().isV3Storage() && model.isIncrementBuildOnExpertMode()
                && !dimIds.contains(model.getPartitionColumnId())) {
            // V3 model layout need partition column
            dimIds.add(model.getPartitionColumnId(), 0);
        }

        LayoutEntity layout = new EntityBuilder.LayoutEntityBuilder(suggestLayoutId(indexEntity), indexEntity)
                .colOrderIds(suggestColOrder(dimIds, measureIds, Lists.newArrayList())).isAuto(true).build();
        layout.setInProposing(true);
        if (!indexEntity.isTableIndex() && CollectionUtils.isNotEmpty(indexPlan.getAggShardByColumns())
                && layout.getColOrder().containsAll(indexPlan.getAggShardByColumns())) {
            layout.setShardByColumns(indexPlan.getAggShardByColumns());
        } else if (isQualifiedSuggestShardBy(ctx)) {
            layout.setShardByColumns(suggestShardBy(dimIds));
        }

        String modelId = model.getUuid();
        int semanticVersion = model.getSemanticVersion();
        for (LayoutEntity l : indexEntity.getLayouts()) {
            List<Integer> sortByColumns = layout.getSortByColumns();
            layout.setSortByColumns(l.getSortByColumns());
            if (l.equals(layout)) {
                return new AccelerateInfo.QueryLayoutRelation(ctx.getSql(), modelId, l.getId(), semanticVersion);
            }
            layout.setSortByColumns(sortByColumns);
        }

        indexEntity.getLayouts().add(layout);
        layoutIds.add(layout.getId());
        modelContext.gatherLayoutRecItem(layout);

        return new AccelerateInfo.QueryLayoutRelation(ctx.getSql(), modelId, layout.getId(), semanticVersion);
    }

    private boolean isQualifiedSuggestShardBy(OlapContext context) {
        for (TblColRef colRef : context.getSQLDigest().getFilterColumns()) {
            if (TblColRef.FilterColEnum.EQUAL_FILTER == colRef.getFilterLevel()) {
                return true;
            }
        }
        return false;
    }

    private List<Integer> suggestTableIndexDimensions(OlapContext context) {
        // 1. determine filter columns and non-filter columns
        Set<TblColRef> filterColumns = Sets.newHashSet(context.getFilterColumns());
        Set<TblColRef> nonFilterColumnSet = new HashSet<>(context.getAllColumns());
        nonFilterColumnSet.addAll(context.getSubqueryJoinParticipants());

        // 2. add extra non-filter column if necessary
        // no column selected in raw query , select the first column of the model (i.e. select 1 from table1)
        if (nonFilterColumnSet.isEmpty() && context.getFilterColumns().isEmpty()) {
            Preconditions.checkState(CollectionUtils.isNotEmpty(model.getAllNamedColumns()),
                    "Cannot suggest any columns in table index.");
            Optional<NDataModel.NamedColumn> optional = model.getAllNamedColumns().stream().filter(column -> {
                TblColRef tblColRef = model.getEffectiveCols().get(column.getId());
                return tblColRef.getTable().equalsIgnoreCase(model.getRootFactTableName());
            }).findFirst();

            if (optional.isPresent()) { // always true
                NDataModel.NamedColumn namedColumn = optional.get();
                nonFilterColumnSet.add(model.getEffectiveCols().get(namedColumn.getId()));
                // set extra-column as dimension in model
                if (namedColumn.getStatus() != NDataModel.ColumnStatus.DIMENSION) {
                    addDimRecommendationForTableIndex(namedColumn);
                }
            }
        }
        nonFilterColumnSet.removeAll(context.getFilterColumns());
        replaceDimOfLookupTableWithFK(context, filterColumns, nonFilterColumnSet);

        // 3. sort filter columns and non-filter columns
        List<TblColRef> sortedDims = sortDimensionColumns(filterColumns, nonFilterColumnSet);

        // 4. generate dimension ids
        return generateDimensionIds(sortedDims, model.getEffectiveCols().inverse());
    }

    private void addDimRecommendationForTableIndex(NDataModel.NamedColumn column) {
        TblColRef tblColRef = model.getEffectiveCols().get(column.getId());
        if (additionalNamedColumns.contains(tblColRef.getIdentity())) {
            return;
        }
        column.setStatus(NDataModel.ColumnStatus.DIMENSION);
        Set<String> newCcUuids = modelContext.getCcRecItemMap().values().stream().map(item -> item.getCc().getUuid())
                .collect(Collectors.toSet());
        String uniqueContent = RawRecUtil.dimensionUniqueContent(tblColRef, model.getCcMap(), newCcUuids);
        if (!modelContext.getUniqueContentToFlag().containsKey(uniqueContent)) {
            DimensionRecItemV2 item = new DimensionRecItemV2(column, tblColRef, uniqueContent);
            modelContext.getDimensionRecItemMap().putIfAbsent(item.getUuid(), item);
            additionalNamedColumns.add(tblColRef.getIdentity());
        }
    }

    private List<Integer> suggestAggIndexDimensions(OlapContext context) {
        // 1. determine filter columns and non-filter columns
        Set<TblColRef> filterColumns = Sets.newHashSet(context.getFilterColumns());
        Set<TblColRef> nonFilterColumnSet = new HashSet<>(context.getGroupByColumns());
        nonFilterColumnSet.addAll(context.getSubqueryJoinParticipants());
        nonFilterColumnSet.removeAll(context.getFilterColumns());
        replaceDimOfLookupTableWithFK(context, filterColumns, nonFilterColumnSet);

        // 2. sort filter columns and non-filter columns
        List<TblColRef> sortedDims = sortDimensionColumns(filterColumns, nonFilterColumnSet);

        // 3. generate dimension ids
        return generateDimensionIds(sortedDims, model.getEffectiveDimensions().inverse());
    }

    private void replaceDimOfLookupTableWithFK(OlapContext olapContext, Set<TblColRef> filterColumns,
            Set<TblColRef> nonFilterColumnSet) {
        AntiFlatChecker antiFlatChecker = modelContext.getAntiFlatChecker();
        Preconditions.checkNotNull(antiFlatChecker, "Initialization of anti-flatten lookups is not ready.");
        boolean constraintFilterOfCC = removeAntiLookupCols(antiFlatChecker, filterColumns);
        boolean constraintNonFilterOfCC = removeAntiLookupCols(antiFlatChecker, nonFilterColumnSet);
        log.debug("Some computed-columns of anti-flatten lookup tables? {}",
                constraintFilterOfCC || constraintNonFilterOfCC);
        if (constraintFilterOfCC || constraintNonFilterOfCC) {
            throw new PendingException(COMPUTED_COLUMN_ON_ANTI_FLATTEN_LOOKUP);
        }

        ColExcludedChecker excludedChecker = modelContext.getExcludedChecker();
        Preconditions.checkNotNull(excludedChecker, "Initialization of excluded columns is not ready.");
        boolean excludedFilterOfCC = removeExcludedColumns(excludedChecker, filterColumns);
        boolean excludedNonFilterOfCC = removeExcludedColumns(excludedChecker, nonFilterColumnSet);
        log.debug("Some computed-columns depend on excluded column? {}", excludedFilterOfCC || excludedNonFilterOfCC);
        if (excludedFilterOfCC || excludedNonFilterOfCC) {
            throw new PendingException(COMPUTED_COLUMN_OF_EXCLUDED_COLUMNS);
        }

        // foreign key column as non-filter column
        Map<String, TblColRef> fKAsDimensionMap = modelContext.collectFkDimensionMap(olapContext);
        fKAsDimensionMap.forEach((name, tblColRef) -> {
            if (!filterColumns.contains(tblColRef)) {
                nonFilterColumnSet.add(tblColRef);
            }
        });
    }

    private boolean removeExcludedColumns(ColExcludedChecker checker, Set<TblColRef> tblColRefs) {
        AtomicBoolean hasExcludedCC = new AtomicBoolean();

        tblColRefs.removeIf(tblColRef -> {
            boolean isExcludedCol = checker.isExcludedCol(tblColRef);
            if (isExcludedCol) {
                log.debug("Remove column {} depends on excluded column.", tblColRef);
            }
            ColumnDesc col = tblColRef.getColumnDesc();
            if (col.isComputedColumn() && isExcludedCol && !hasExcludedCC.get()) {
                hasExcludedCC.set(true);
            }
            return isExcludedCol;
        });

        return hasExcludedCC.get();
    }

    private boolean removeAntiLookupCols(AntiFlatChecker checker, Set<TblColRef> tblColRefs) {
        AtomicBoolean hasConstraintCC = new AtomicBoolean();
        tblColRefs.removeIf(tblColRef -> {
            boolean constrained = checker.isColOfAntiLookup(tblColRef);
            if (constrained) {
                log.debug("Removed column {} of anti-flatten lookup table.", tblColRef);
            }
            if (tblColRef.getColumnDesc().isComputedColumn() && constrained && !hasConstraintCC.get()) {
                hasConstraintCC.set(true);
            }
            return constrained;
        });
        return hasConstraintCC.get();
    }

    private List<TblColRef> sortDimensionColumns(Collection<TblColRef> filterColumnsCollection,
            Collection<TblColRef> nonFilterColumnsCollection) {
        List<TblColRef> filterColumns = new ArrayList<>(filterColumnsCollection);
        List<TblColRef> nonFilterColumns = new ArrayList<>(nonFilterColumnsCollection);

        val chooserContext = new ChooserContext(model);
        val filterColComparator = ComparatorUtils.filterColComparator(chooserContext);
        filterColumns.sort(filterColComparator);
        nonFilterColumns.sort(ComparatorUtils.nonFilterColComparator());

        List<TblColRef> result = new LinkedList<>(filterColumns);
        result.addAll(nonFilterColumns);
        return result;
    }

    private List<Integer> generateDimensionIds(List<TblColRef> dimCols, ImmutableBiMap<TblColRef, Integer> colIdMap) {
        return dimCols.stream().map(dimCol -> {
            if (colIdMap.get(dimCol) == null) {
                throw new PendingException(
                        String.format(Locale.ROOT, getMsgTemplateByModelMaintainType(COLUMN_NOT_FOUND_PTN, Type.COLUMN),
                                model.getAlias(), dimCol.getIdentity()));
            }
            return colIdMap.get(dimCol);
        }).collect(Collectors.toList());
    }

    private SortedSet<Integer> suggestMeasures(List<Integer> dimIds, OlapContext ctx) {
        Map<TblColRef, Integer> colIdMap = model.getEffectiveDimensions().inverse();
        SortedSet<Integer> measureIds = Sets.newTreeSet();
        // Add default measure count(1)
        measureIds.add(calcCountOneMeasureId());

        ctx.getAggregations().forEach(aggFunc -> {
            Integer measureId = aggFuncIdMap.get(aggFunc);
            if (modelContext.getAntiFlatChecker().isMeasureOfAntiLookup(aggFunc)) {
                throw new PendingException(MEASURE_ON_ANTI_FLATTEN_LOOKUP + aggFunc);
            } else if (modelContext.getExcludedChecker().isExcludedMeasure(aggFunc)) {
                throw new PendingException(MEASURE_OF_EXCLUDED_COLUMNS + aggFunc);
            }
            if (measureId != null) {
                measureIds.add(measureId);
            } else if (CollectionUtils.isNotEmpty(aggFunc.getParameters())) {
                if (CubeUtils.isValidMeasure(aggFunc)) {
                    String measure = String.format(Locale.ROOT, "%s(%s)", aggFunc.getExpression(),
                            aggFunc.getParameters());
                    for (TblColRef tblColRef : aggFunc.getColRefs()) {
                        if (colIdMap.get(tblColRef) == null) {
                            throw new PendingException(String.format(Locale.ROOT,
                                    getMsgTemplateByModelMaintainType(MEASURE_NOT_FOUND_PTN, Type.MEASURE),
                                    model.getAlias(), measure));
                        }
                    }
                } else if (aggFunc.canAnsweredByDimensionAsMeasure()) {
                    List<Integer> newDimIds = generateDimensionIds(Lists.newArrayList(aggFunc.getSourceColRefs()),
                            model.getEffectiveDimensions().inverse());
                    newDimIds.removeIf(dimIds::contains);
                    dimIds.addAll(newDimIds);
                }
            }
        });
        return measureIds;
    }

    /**
     * By default, we will add a count(1) measure in our model if agg appears.
     * If current model without measures, the id of count one measure is 10_000;
     * else find existing count one measure's id.
     */
    private Integer calcCountOneMeasureId() {
        Integer countOne = aggFuncIdMap.get(FunctionDesc.newCountOne());
        return countOne == null ? NDataModel.MEASURE_ID_BASE : countOne;
    }

    private String getMsgTemplateByModelMaintainType(String messagePattern, Type type) {
        Preconditions.checkNotNull(model);
        String suggestion;
        if (type == Type.COLUMN) {
            suggestion = "Please add the above dimension before attempting to accelerate this query.";
        } else if (type == Type.MEASURE) {
            suggestion = "Please add the above measure before attempting to accelerate this query.";
        } else {
            suggestion = "Please adjust model's join to match the query.";
        }

        return messagePattern + suggestion;
    }

    private IndexEntity createIndexEntity(long id, List<Integer> dimIds, SortedSet<Integer> measureIds) {
        EntityBuilder.checkDimensionsAndMeasures(dimIds, Lists.newArrayList(measureIds));
        IndexEntity indexEntity = new IndexEntity();
        indexEntity.setId(id);
        indexEntity.setDimensions(dimIds);
        indexEntity.setMeasures(Lists.newArrayList(measureIds));
        indexEntity.setIndexPlan(indexPlan);
        return indexEntity;
    }

    private List<Integer> suggestColOrder(final List<Integer> orderedDimIds, Set<Integer> measureIds,
            List<Integer> shardBy) {
        ArrayList<Integer> copyDimension = new ArrayList<>(orderedDimIds);
        copyDimension.removeAll(shardBy);
        List<Integer> colOrder = Lists.newArrayList();
        colOrder.addAll(shardBy);
        colOrder.addAll(copyDimension);
        colOrder.addAll(measureIds);
        return colOrder;
    }

    private long suggestDescId(boolean isTableIndex) {
        return EntityBuilder.IndexEntityBuilder.findAvailableIndexEntityId(indexPlan, collector.values(), isTableIndex);
    }

    private long suggestLayoutId(IndexEntity indexEntity) {
        long s = indexEntity.getId() + indexEntity.getNextLayoutOffset();
        while (layoutIds.contains(s)) {
            s++;
        }
        return s;
    }

    private enum Type {
        TABLE, COLUMN, MEASURE
    }
}
