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

package org.apache.kylin.metadata.recommendation.ref;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.ExcludedLookupChecker;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.candidate.RawRecManager;
import org.apache.kylin.metadata.recommendation.candidate.RawRecSelection;
import org.apache.kylin.metadata.recommendation.entity.CCRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.DimensionRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.MeasureRecItemV2;
import org.apache.kylin.metadata.recommendation.util.RawRecUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class OptRecV2 {

    private static final int CONSTANT = Integer.MAX_VALUE;
    private static final String MEASURE_NAME_PREFIX = "MEASURE_AUTO_";
    private static final String CC_AS_DIMENSION_PREFIX = "DIMENSION_AUTO_";

    private final String uuid;
    private final KylinConfig config;
    private final String project;

    private final Map<String, RawRecItem> uniqueFlagToRecItemMap;
    private final BiMap<String, Integer> uniqueFlagToId = HashBiMap.create();
    private final List<Integer> rawIds = Lists.newArrayList();

    // Ref map. If key >= 0, ref in model else ref in raw item.
    private final Map<Integer, RecommendationRef> columnRefs = Maps.newHashMap();
    private final Map<Integer, RecommendationRef> ccRefs = Maps.newHashMap();
    private final Map<Integer, RecommendationRef> dimensionRefs = Maps.newHashMap();
    private final Map<Integer, RecommendationRef> measureRefs = Maps.newHashMap();
    private final Map<Integer, LayoutRef> additionalLayoutRefs = Maps.newHashMap();
    private final Map<Integer, LayoutRef> removalLayoutRefs = Maps.newHashMap();
    private final Map<Integer, RawRecItem> rawRecItemMap = Maps.newHashMap();
    private final Set<Integer> brokenRefIds = Sets.newHashSet();

    @Getter(lazy = true)
    private final List<LayoutEntity> layouts = getAllLayouts();
    @Getter(lazy = true)
    private final NDataModel model = initModel();
    @Getter(lazy = true)
    private final Map<String, ComputedColumnDesc> projectCCMap = initAllCCMap();
    private final ExcludedLookupChecker checker;
    private final boolean needLog;

    public OptRecV2(String project, String uuid, boolean needLog) {
        this.needLog = needLog;
        this.config = KylinConfig.getInstanceFromEnv();
        this.uuid = uuid;
        this.project = project;

        uniqueFlagToRecItemMap = RawRecManager.getInstance(project).queryNonLayoutRecItems(Sets.newHashSet(uuid));
        uniqueFlagToRecItemMap.forEach((k, recItem) -> uniqueFlagToId.put(k, recItem.getId()));
        Set<String> excludedTables = FavoriteRuleManager.getInstance(config, project).getExcludedTables();
        checker = new ExcludedLookupChecker(excludedTables, getModel().getJoinTables(), getModel());
        if (!getModel().isBroken()) {
            initModelColumnRefs(getModel());
            initModelMeasureRefs(getModel());
        }
    }

    public void initRecommendation() {
        log.info("Start to initialize recommendation({}/{}}", project, getUuid());

        NDataModel dataModel = getModel();
        if (dataModel.isBroken()) {
            log.warn("Discard all related recommendations for model({}/{}) is broken.", project, uuid);
            RawRecManager.getInstance(project).discardRecItemsOfBrokenModel(dataModel.getUuid());
            return;
        }

        initLayoutRefs(queryBestLayoutRecItems());
        initLayoutRefs(queryImportedRawRecItems());
        initRemovalLayoutRefs(queryBestRemovalLayoutRecItems());

        autoNameForMeasure();
        brokenRefIds.addAll(collectBrokenRefs());
        log.info("Initialize recommendation({}/{}) successfully.", project, uuid);
    }

    public List<RawRecItem> filterExcludedRecPatterns(List<RawRecItem> rawRecItems) {
        log.info("Start to initialize recommendation patterns({}/{}}", project, getUuid());
        NDataModel dataModel = getModel();
        if (dataModel.isBroken()) {
            log.warn("Discard all related recommendations for model({}/{}) is broken.", project, uuid);
            RawRecManager.getInstance(project).discardRecItemsOfBrokenModel(dataModel.getUuid());
            return Lists.newArrayList();
        }
        List<RawRecItem> reserved = Lists.newArrayList();
        initLayoutRefs(rawRecItems);
        brokenRefIds.addAll(collectBrokenRefs());
        log.info("Initialize recommendation patterns({}/{}) successfully.", project, uuid);
        return reserved;
    }

    private void autoNameForMeasure() {
        AtomicInteger maxMeasureIndex = new AtomicInteger(getBiggestAutoMeasureIndex(getModel()));
        List<RecommendationRef> allMeasureRefs = getEffectiveRefs(measureRefs);
        for (RecommendationRef entry : allMeasureRefs) {
            MeasureRef measureRef = (MeasureRef) entry;
            String measureName = OptRecV2.MEASURE_NAME_PREFIX + maxMeasureIndex.incrementAndGet();
            measureRef.getMeasure().setName(measureName);
            measureRef.setName(measureName);
            measureRef.setContent(JsonUtil.writeValueAsStringQuietly(measureRef.getMeasure()));
        }
    }

    public int getBiggestAutoMeasureIndex(NDataModel dataModel) {
        int biggest = 0;
        List<String> allAutoMeasureNames = dataModel.getAllMeasures() //
                .stream().map(MeasureDesc::getName) //
                .filter(name -> name.startsWith(MEASURE_NAME_PREFIX)) //
                .collect(Collectors.toList());
        for (String name : allAutoMeasureNames) {
            int idx;
            try {
                String idxStr = name.substring(MEASURE_NAME_PREFIX.length());
                idx = StringUtils.isEmpty(idxStr) ? -1 : Integer.parseInt(idxStr);
            } catch (NumberFormatException e) {
                idx = -1;
            }
            if (idx > biggest) {
                biggest = idx;
            }
        }
        return biggest;
    }

    /**
     * Init ModelColumnRefs and DimensionRefs from model
     */
    private void initModelColumnRefs(NDataModel model) {
        List<ComputedColumnDesc> ccList = model.getComputedColumnDescs();
        Map<String, String> ccNameToExpressionMap = Maps.newHashMap();
        ccList.forEach(cc -> ccNameToExpressionMap.put(cc.getFullName(), cc.getExpression()));

        for (NDataModel.NamedColumn column : model.getAllNamedColumns()) {
            if (!column.isExist()) {
                continue;
            }

            int id = column.getId();
            String columnName = column.getAliasDotColumn();
            String content = ccNameToExpressionMap.getOrDefault(columnName, columnName);
            TblColRef tblColRef = model.getEffectiveCols().get(column.getId());
            RecommendationRef columnRef = new ModelColumnRef(column, tblColRef.getDatatype(), content);
            if (checker.isColRefDependsLookupTable(tblColRef)) {
                columnRef.setExcluded(true);
            }
            columnRefs.put(id, columnRef);

            if (column.isDimension()) {
                dimensionRefs.put(id, new DimensionRef(columnRef, id, tblColRef.getDatatype(), true));
            }
        }
    }

    /**
     * Init MeasureRefs from model
     */
    private void initModelMeasureRefs(NDataModel model) {
        for (NDataModel.Measure measure : model.getAllMeasures()) {
            if (measure.isTomb()) {
                continue;
            }
            MeasureRef measureRef = new MeasureRef(measure, measure.getId(), true);
            measure.getFunction().getParameters().stream().filter(ParameterDesc::isColumnType).forEach(p -> {
                int id = model.getColumnIdByColumnName(p.getValue());
                if (checker.isColRefDependsLookupTable(p.getColRef())) {
                    measureRef.setExcluded(true);
                }
                measureRef.getDependencies().add(columnRefs.get(id));
            });
            measureRefs.put(measure.getId(), measureRef);
        }
    }

    /**
     * Init LayoutRefs and they derived dependencies(DimensionRef, MeasureRef, CCRef)
     */
    private void initLayoutRefs(List<RawRecItem> bestRecItems) {
        bestRecItems.forEach(rawRecItem -> rawIds.add(rawRecItem.getId()));
        bestRecItems.forEach(rawRecItem -> rawRecItemMap.put(rawRecItem.getId(), rawRecItem));
        bestRecItems.forEach(this::initLayoutRef);
    }

    private void initRemovalLayoutRefs(List<RawRecItem> removalLayoutRecItems) {
        removalLayoutRecItems.forEach(rawRecItem -> {
            rawIds.add(rawRecItem.getId());
            rawRecItemMap.put(rawRecItem.getId(), rawRecItem);

            logTranslateInfo(rawRecItem);
            LayoutRef ref = convertToLayoutRef(rawRecItem);
            removalLayoutRefs.put(-rawRecItem.getId(), ref);
        });
    }

    private List<RawRecItem> queryBestLayoutRecItems() {
        FavoriteRule favoriteRule = FavoriteRuleManager.getInstance(config, project)
                .getOrDefaultByName(FavoriteRule.REC_SELECT_RULE_NAME);
        int topN = Integer.parseInt(((FavoriteRule.Condition) favoriteRule.getConds().get(0)).getRightThreshold());
        return RawRecSelection.getInstance().selectBestLayout(topN, uuid, project);
    }

    private List<RawRecItem> queryImportedRawRecItems() {
        return RawRecManager.getInstance(project).queryImportedRawRecItems(project, uuid);
    }

    private List<RawRecItem> queryBestRemovalLayoutRecItems() {
        Map<String, RawRecItem> recItemMap = RawRecManager.getInstance(project).queryNonAppliedLayoutRawRecItems(uuid,
                false);
        List<RawRecItem> initialRemovalLayoutRecItems = Lists.newArrayList();
        recItemMap.forEach((key, value) -> {
            if (value.getState() == RawRecItem.RawRecState.INITIAL) {
                initialRemovalLayoutRecItems.add(value);
            }
        });
        return initialRemovalLayoutRecItems;
    }

    private void initLayoutRef(RawRecItem rawRecItem) {
        logTranslateInfo(rawRecItem);
        LayoutRef ref = convertToLayoutRef(rawRecItem);
        additionalLayoutRefs.put(-rawRecItem.getId(), ref);
        if (ref.isBroken()) {
            return;
        }
        checkLayoutExists(rawRecItem);
    }

    private void checkLayoutExists(RawRecItem recItem) {
        int negRecItemId = -recItem.getId();
        LayoutRef layoutRef = additionalLayoutRefs.get(negRecItemId);
        LayoutEntity layout = JsonUtil.deepCopyQuietly(layoutRef.getLayout(), LayoutEntity.class);
        List<Integer> colOrder = Lists.newArrayList();
        List<Integer> sortColumns = Lists.newArrayList();
        List<Integer> partitionColumns = Lists.newArrayList();
        List<Integer> shardColumns = Lists.newArrayList();
        boolean containNotExistsColumn = translate(colOrder, layout.getColOrder());
        if (!containNotExistsColumn) {
            translate(sortColumns, layout.getSortByColumns());
            translate(shardColumns, layout.getShardByColumns());
            translate(partitionColumns, layout.getPartitionByColumns());
            layout.setColOrder(colOrder);
            layout.setShardByColumns(shardColumns);
            layout.setPartitionByColumns(partitionColumns);
            long layoutId = getLayouts().stream() //
                    .filter(layoutEntity -> layoutEntity.equals(layout)) //
                    .map(LayoutEntity::getId) //
                    .findFirst().orElse(-1L);
            if (layoutId > 0) {
                logConflictWithRealEntity(recItem, layoutId);
                layoutRef.setExisted(true);
                return;
            }
        }

        // avoid the same LayoutRef
        for (RecommendationRef entry : getEffectiveRefs(additionalLayoutRefs)) {
            if (entry.getId() == negRecItemId) {
                continue;
            }
            if (Objects.equals(entry, layoutRef)) {
                logDuplicateRawRecItem(recItem, -entry.getId());
                layoutRef.setExisted(true);
                return;
            }
        }
    }

    // Translate existing column from RawRecItem to column in model.
    // Return true if there is a not exist column/measure in cols,
    // so we can skip check with layout in index.
    private boolean translate(List<Integer> toColIds, List<Integer> fromColIds) {
        for (Integer id : fromColIds) {
            RecommendationRef ref = dimensionRefs.containsKey(id) ? dimensionRefs.get(id) : measureRefs.get(id);
            if (ref == null || !ref.isExisted()) {
                return true;
            }
            toColIds.add(ref.getId());
        }
        return false;
    }

    private LayoutRef convertToLayoutRef(RawRecItem rawRecItem) {
        int negRecItemId = -rawRecItem.getId();
        NDataModel dataModel = getModel();
        if (rawRecItem.isOutOfDate(dataModel.getSemanticVersion())) {
            logSemanticNotMatch(rawRecItem, dataModel);
            return BrokenRefProxy.getProxy(LayoutRef.class, negRecItemId);
        }

        LayoutEntity layout = RawRecUtil.getLayout(rawRecItem);
        if (RawRecItem.RawRecType.REMOVAL_LAYOUT == rawRecItem.getType()) {
            NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            Map<Long, LayoutEntity> allLayoutsMap = indexMgr.getIndexPlan(uuid).getAllLayoutsMap();
            if (!allLayoutsMap.containsKey(layout.getId())) {
                return BrokenRefProxy.getProxy(LayoutRef.class, negRecItemId);
            }
        }
        LayoutRef layoutRef = new LayoutRef(layout, negRecItemId, rawRecItem.isAgg());
        for (int dependId : rawRecItem.getDependIDs()) {
            initDependencyRef(dependId, dataModel);

            // normal case: all dependId can be found in dimensionRefs or measureRefs
            if (dimensionRefs.containsKey(dependId) || measureRefs.containsKey(dependId)) {
                RecommendationRef ref = dimensionRefs.containsKey(dependId) //
                        ? dimensionRefs.get(dependId)
                        : measureRefs.get(dependId);
                if (ref.isBroken()) {
                    logDependencyLost(rawRecItem, dependId);
                    return BrokenRefProxy.getProxy(LayoutRef.class, layoutRef.getId());
                }
                if (ref.isExcluded()) {
                    layoutRef.setExcluded(true);
                }
                layoutRef.getDependencies().add(ref);
                continue;
            }

            // abnormal case: maybe this column has been deleted in model, mark this ref to deleted.
            if (dependId > 0) {
                logDependencyLost(rawRecItem, dependId);
                return BrokenRefProxy.getProxy(LayoutRef.class, layoutRef.getId());
            }
        }
        return layoutRef;
    }

    private void initDependencyRef(int dependId, NDataModel dataModel) {
        if (dependId >= 0) {
            log.info("DependId({}) is derived from model({}/{})", //
                    dependId, getProject(), dataModel.getUuid());
            return;
        }

        int rawRecItemId = -dependId;
        if (rawRecItemMap.containsKey(rawRecItemId)) {
            logRawRecItemHasBeenInitialized(dataModel, rawRecItemId);
            return;
        }

        String uniqueFlag = uniqueFlagToId.inverse().get(rawRecItemId);
        RawRecItem rawRecItem = uniqueFlag == null ? null : uniqueFlagToRecItemMap.get(uniqueFlag);
        if (rawRecItem == null) {
            logRawRecItemNotFoundError(rawRecItemId);
            ccRefs.put(dependId, BrokenRefProxy.getProxy(CCRef.class, dependId));
            dimensionRefs.put(dependId, BrokenRefProxy.getProxy(DimensionRef.class, dependId));
            measureRefs.put(dependId, BrokenRefProxy.getProxy(MeasureRef.class, dependId));
            rawRecItemMap.put(dependId, null);
            return;
        }
        switch (rawRecItem.getType()) {
        case COMPUTED_COLUMN:
            initCCRef(rawRecItem, dataModel);
            break;
        case DIMENSION:
            initDimensionRef(rawRecItem, dataModel);
            break;
        case MEASURE:
            initMeasureRef(rawRecItem, dataModel);
            break;
        default:
            throw new IllegalStateException("id: " + rawRecItemId + " type is illegal");
        }
        rawRecItemMap.put(rawRecItemId, rawRecItem);
    }

    private void initCCRef(RawRecItem rawRecItem, NDataModel dataModel) {
        logTranslateInfo(rawRecItem);

        int negRecItemId = -rawRecItem.getId();
        if (rawRecItem.isOutOfDate(dataModel.getSemanticVersion())) {
            logSemanticNotMatch(rawRecItem, dataModel);
            ccRefs.put(negRecItemId, BrokenRefProxy.getProxy(CCRef.class, negRecItemId));
            return;
        }
        Map<String, ComputedColumnDesc> ccMapOnModel = Maps.newHashMap();
        dataModel.getComputedColumnDescs().forEach(cc -> ccMapOnModel.put(cc.getInnerExpression(), cc));

        ComputedColumnDesc cc = RawRecUtil.getCC(rawRecItem);
        CCRef ccRef = new CCRef(cc, negRecItemId);
        if (ccMapOnModel.containsKey(cc.getInnerExpression())) {
            ComputedColumnDesc existCC = ccMapOnModel.get(cc.getInnerExpression());
            ccRef = new CCRef(existCC, negRecItemId);
            ccRef.setExisted(true);
            ccRef.setCrossModel(false);
            dataModel.getEffectiveCols().forEach((key, tblColRef) -> {
                if (tblColRef.getIdentity().equalsIgnoreCase(existCC.getFullName())) {
                    ccRefs.put(negRecItemId, columnRefs.get(key));
                }
            });
            return;
        } else if (getProjectCCMap().containsKey(cc.getInnerExpression())) {
            ComputedColumnDesc existCC = getProjectCCMap().get(cc.getInnerExpression());
            if (existCC.getTableIdentity().equalsIgnoreCase(cc.getTableIdentity())) {
                ccRef = new CCRef(existCC, negRecItemId);
                ccRef.setExisted(false);
                ccRef.setCrossModel(true);
            } else {
                ccRef = new CCRef(cc, negRecItemId);
                ccRef.setExisted(false);
                ccRef.setCrossModel(false);
            }
        }

        int[] dependIds = rawRecItem.getDependIDs();
        for (int dependId : dependIds) {
            TranslatedState state = initDependencyWithState(dependId, ccRef);
            if (state == TranslatedState.BROKEN) {
                logDependencyLost(rawRecItem, dependId);
                ccRefs.put(negRecItemId, BrokenRefProxy.getProxy(CCRef.class, negRecItemId));
                return;
            }
        }

        CCRecItemV2 recEntity = (CCRecItemV2) rawRecItem.getRecEntity();
        int[] newDependIds = recEntity.genDependIds(dataModel);
        if (!Arrays.equals(newDependIds, rawRecItem.getDependIDs())) {
            logIllegalRawRecItem(rawRecItem, rawRecItem.getDependIDs(), newDependIds);
            measureRefs.put(negRecItemId, BrokenRefProxy.getProxy(MeasureRef.class, negRecItemId));
            return;
        }

        ccRefs.put(negRecItemId, ccRef);
        checkCCExist(rawRecItem);
    }

    private void checkCCExist(RawRecItem recItem) {
        int negRecItemId = -recItem.getId();
        RecommendationRef ref = ccRefs.get(negRecItemId);
        if (ref.isExisted() || !(ref instanceof CCRef)) {
            return;
        }

        // check in other raw items.
        CCRef ccRef = (CCRef) ref;
        for (RecommendationRef entry : getEffectiveRefs(ccRefs)) {
            if (entry.getId() == negRecItemId) {
                // pass itself
                continue;
            }

            CCRef anotherCCRef = (CCRef) entry;
            if (ccRef.isIdentical(anotherCCRef)) {
                logDuplicateRawRecItem(recItem, -entry.getId());
                ccRef.setExisted(true);
                ccRefs.put(negRecItemId, ccRefs.get(entry.getId()));
                return;
            }
        }
    }

    private void initDimensionRef(RawRecItem rawRecItem, NDataModel dataModel) {
        logTranslateInfo(rawRecItem);

        // check semanticVersion
        int negRecItemId = -rawRecItem.getId();
        if (rawRecItem.isOutOfDate(dataModel.getSemanticVersion())) {
            logSemanticNotMatch(rawRecItem, dataModel);
            dimensionRefs.put(negRecItemId, BrokenRefProxy.getProxy(DimensionRef.class, negRecItemId));
            return;
        }

        DimensionRef dimensionRef = new DimensionRef(negRecItemId);
        final int[] dependIDs = rawRecItem.getDependIDs();
        Preconditions.checkArgument(dependIDs.length == 1);
        int dependID = dependIDs[0];
        TranslatedState state = initDependencyWithState(dependID, dimensionRef);
        if (state == TranslatedState.BROKEN) {
            logDependencyLost(rawRecItem, dependID);
            dimensionRefs.put(negRecItemId, BrokenRefProxy.getProxy(DimensionRef.class, negRecItemId));
            return;
        }

        DimensionRecItemV2 recEntity = (DimensionRecItemV2) rawRecItem.getRecEntity();
        if (recEntity.getUniqueContent() == null) {
            logIncompatibleRawRecItem(rawRecItem);
            measureRefs.put(negRecItemId, BrokenRefProxy.getProxy(MeasureRef.class, negRecItemId));
            return;
        }
        int[] newDependIds = recEntity.genDependIds(uniqueFlagToRecItemMap, recEntity.getUniqueContent(), dataModel);
        if (!Arrays.equals(newDependIds, rawRecItem.getDependIDs())) {
            logIllegalRawRecItem(rawRecItem, rawRecItem.getDependIDs(), newDependIds);
            measureRefs.put(negRecItemId, BrokenRefProxy.getProxy(MeasureRef.class, negRecItemId));
            return;
        }

        dimensionRef.init();
        if (dependID < 0) {
            String dimRefName = dimensionRef.getName();
            dimensionRef.setName(dimRefName.replace(ComputedColumnUtil.CC_NAME_PREFIX, CC_AS_DIMENSION_PREFIX));
        }
        dimensionRefs.put(negRecItemId, reuseIfAvailable(dimensionRef));
        checkDimensionExist(rawRecItem);
    }

    private DimensionRef reuseIfAvailable(DimensionRef dimensionRef) {
        RecommendationRef recommendationRef = dimensionRef.getDependencies().get(0);
        if (recommendationRef instanceof ModelColumnRef) {
            NDataModel.NamedColumn column = ((ModelColumnRef) recommendationRef).getColumn();
            if (column.isDimension()) {
                dimensionRef = (DimensionRef) dimensionRefs.get(column.getId());
            }
        }
        return dimensionRef;
    }

    private void checkDimensionExist(RawRecItem recItem) {
        int negRecItemId = -recItem.getId();
        RecommendationRef dimensionRef = dimensionRefs.get(negRecItemId);

        // check two raw recommendations share same content
        for (RecommendationRef entry : getEffectiveRefs(dimensionRefs)) {
            if (entry.getId() == negRecItemId) {
                // pass itself
                continue;
            }

            // if reference of this raw recommendation has been approved, forward to the approved one
            if (Objects.equals(entry, dimensionRef)) {
                logDuplicateRawRecItem(recItem, -entry.getId());
                dimensionRef.setExisted(true);
                dimensionRefs.put(negRecItemId, dimensionRefs.get(entry.getId()));
                return;
            }
        }
    }

    private void initMeasureRef(RawRecItem rawRecItem, NDataModel dataModel) {
        logTranslateInfo(rawRecItem);

        int negRecItemId = -rawRecItem.getId();
        if (rawRecItem.isOutOfDate(dataModel.getSemanticVersion())) {
            logSemanticNotMatch(rawRecItem, dataModel);
            measureRefs.put(negRecItemId, BrokenRefProxy.getProxy(MeasureRef.class, negRecItemId));
            return;
        }

        RecommendationRef ref = new MeasureRef(RawRecUtil.getMeasure(rawRecItem), negRecItemId, false);
        for (int value : rawRecItem.getDependIDs()) {
            TranslatedState state = initDependencyWithState(value, ref);
            if (state == TranslatedState.BROKEN) {
                logDependencyLost(rawRecItem, value);
                measureRefs.put(negRecItemId, BrokenRefProxy.getProxy(MeasureRef.class, negRecItemId));
                return;
            }
        }

        MeasureRecItemV2 recEntity = (MeasureRecItemV2) rawRecItem.getRecEntity();
        if (recEntity.getUniqueContent() == null) {
            logIncompatibleRawRecItem(rawRecItem);
            measureRefs.put(negRecItemId, BrokenRefProxy.getProxy(MeasureRef.class, negRecItemId));
            return;
        }
        int[] newDependIds = recEntity.genDependIds(uniqueFlagToRecItemMap, recEntity.getUniqueContent(), dataModel);
        if (!Arrays.equals(newDependIds, rawRecItem.getDependIDs())) {
            logIllegalRawRecItem(rawRecItem, rawRecItem.getDependIDs(), newDependIds);
            measureRefs.put(negRecItemId, BrokenRefProxy.getProxy(MeasureRef.class, negRecItemId));
            return;
        }

        measureRefs.put(negRecItemId, ref);
        checkMeasureExist(rawRecItem);
    }

    private void checkMeasureExist(RawRecItem recItem) {
        int negRecItemId = -recItem.getId();
        MeasureRef measureRef = (MeasureRef) measureRefs.get(negRecItemId);
        for (RecommendationRef entry : getLegalRefs(measureRefs)) {
            if (entry.getId() == negRecItemId) {
                // pass itself
                continue;
            }

            /* Parameters of measure can only ordinary columns or computed columns,
             * so if the function name and dependencies of two measureRefs are the same,
             * they are identical, then the second measureRef should forward to the first one.
             */
            if (measureRef.isIdentical(entry)) {
                logDuplicateRawRecItem(recItem, -entry.getId());
                measureRef.setExisted(true);
                measureRefs.put(negRecItemId, measureRefs.get(entry.getId()));
                return;
            }
        }
    }

    private TranslatedState initDependencyWithState(int dependId, RecommendationRef ref) {
        if (dependId == OptRecV2.CONSTANT) {
            return TranslatedState.CONSTANT;
        }
        NDataModel dataModel = getModel();
        initDependencyRef(dependId, dataModel);

        if (columnRefs.containsKey(dependId)) {
            RecommendationRef e = columnRefs.get(dependId);
            if (e.isBroken()) {
                return TranslatedState.BROKEN;
            } else if (e.isExcluded()) {
                ref.setExcluded(true);
            }
            ref.getDependencies().add(e);
        } else if (ccRefs.containsKey(dependId)) {
            RecommendationRef e = ccRefs.get(dependId);
            if (e.isBroken()) {
                return TranslatedState.BROKEN;
            } else if (e.isExcluded()) {
                ref.setExcluded(true);
            }
            ref.getDependencies().add(e);
        } else {
            return TranslatedState.BROKEN;
        }
        return TranslatedState.NORMAL;
    }

    private List<RecommendationRef> getEffectiveRefs(Map<Integer, ? extends RecommendationRef> refMap) {
        List<RecommendationRef> effectiveRefs = Lists.newArrayList();
        refMap.forEach((key, ref) -> {
            if (ref.isEffective()) {
                effectiveRefs.add(ref);
            }
        });
        effectiveRefs.sort(Comparator.comparingInt(RecommendationRef::getId));
        return effectiveRefs;
    }

    private List<RecommendationRef> getLegalRefs(Map<Integer, ? extends RecommendationRef> refMap) {
        Set<RecommendationRef> effectiveRefs = Sets.newHashSet();
        refMap.forEach((key, ref) -> {
            if (ref.isLegal()) {
                effectiveRefs.add(ref);
            }
        });
        List<RecommendationRef> effectiveRefList = Lists.newArrayList(effectiveRefs);
        effectiveRefList.sort(Comparator.comparingInt(RecommendationRef::getId));
        return effectiveRefList;
    }

    private Set<Integer> collectBrokenRefs() {
        Set<Integer> brokenIds = Sets.newHashSet();
        additionalLayoutRefs.forEach((id, ref) -> {
            if (ref.isBroken() && id < 0) {
                brokenIds.add(-id);
            }
        });
        removalLayoutRefs.forEach((id, ref) -> {
            if (ref.isBroken() && id < 0) {
                brokenIds.add(-id);
            }
        });
        fillBrokenRef(brokenIds, ccRefs);
        fillBrokenRef(brokenIds, dimensionRefs);
        fillBrokenRef(brokenIds, measureRefs);
        return brokenIds;
    }

    private void fillBrokenRef(Set<Integer> brokenIds, Map<Integer, RecommendationRef> refs) {
        refs.forEach((id, ref) -> {
            if (ref.isBroken() && id < 0) {
                brokenIds.add(-id);
            }
        });
    }

    private Map<String, ComputedColumnDesc> initAllCCMap() {
        Map<String, ComputedColumnDesc> ccMap = Maps.newHashMap();
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.readSystemKylinConfig(), project);
        List<NDataModel> allModels = modelManager.listAllModels();
        allModels.stream().filter(m -> !m.isBroken()).forEach(m -> {
            List<ComputedColumnDesc> ccList = m.getComputedColumnDescs();
            for (ComputedColumnDesc cc : ccList) {
                ccMap.putIfAbsent(cc.getInnerExpression(), cc);
            }
        });
        return ccMap;
    }

    private NDataModel initModel() {
        NDataModelManager modelManager = NDataModelManager.getInstance(Objects.requireNonNull(config), project);
        NDataModel dataModel = modelManager.getDataModelDesc(getUuid());
        return dataModel.isBroken() ? dataModel : modelManager.copyForWrite(dataModel);
    }

    private List<LayoutEntity> getAllLayouts() {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(Objects.requireNonNull(config), project);
        return indexPlanManager.getIndexPlan(getUuid()).getAllLayouts();
    }

    private void logRawRecItemHasBeenInitialized(NDataModel dataModel, int rawRecItemId) {
        log.info("RawRecItem({}) already initialized for Recommendation({}/{})", //
                rawRecItemId, getProject(), dataModel.getUuid());
    }

    private void logRawRecItemNotFoundError(int rawRecItemId) {
        log.error("RawRecItem({}) is not found in recommendation({}/{})", rawRecItemId, project, getUuid());
    }

    private void logTranslateInfo(RawRecItem recItem) {
        String type;
        switch (recItem.getType()) {
        case MEASURE:
            type = "MeasureRef";
            break;
        case COMPUTED_COLUMN:
            type = "CCRef";
            break;
        case ADDITIONAL_LAYOUT:
        case REMOVAL_LAYOUT:
            type = "LayoutRef";
            break;
        case DIMENSION:
            type = "DimensionRef";
            break;
        default:
            throw new IllegalArgumentException();
        }
        log.info("RawRecItem({}) will be translated to {} in Recommendation({}/{})", //
                recItem.getId(), type, project, getUuid());
    }

    private void logDependencyLost(RawRecItem rawRecItem, int dependId) {
        log.info("RawRecItem({}) lost dependency of {} in recommendation({}/{})", //
                rawRecItem.getId(), dependId, getProject(), getUuid());
    }

    private void logSemanticNotMatch(RawRecItem rawRecItem, NDataModel dataModel) {
        log.info("RawRecItem({}) has an outdated semanticVersion({}) less than {} in recommendation({}/{})",
                rawRecItem.getId(), rawRecItem.getSemanticVersion(), //
                dataModel.getSemanticVersion(), getProject(), getUuid());
    }

    private void logConflictWithRealEntity(RawRecItem recItem, long existingId) {
        log.info("RawRecItem({}) encounters an existing {}({}) in recommendation({}/{})", //
                recItem.getId(), recItem.getType().name(), existingId, getProject(), getUuid());
    }

    private void logDuplicateRawRecItem(RawRecItem recItem, int anotherRecItemId) {
        log.info("RawRecItem({}) duplicates with another RawRecItem({}) in recommendation({}/{})", //
                recItem.getId(), anotherRecItemId, getProject(), getUuid());
    }

    private void logIllegalRawRecItem(RawRecItem recItem, int[] oldDependIds, int[] newDependIds) {
        log.error("RawRecItem({}) illegal now for dependIds changed, old dependIds({}), new dependIds({})",
                recItem.getId(), Arrays.toString(oldDependIds), Arrays.toString(newDependIds));
    }

    private void logIncompatibleRawRecItem(RawRecItem recItem) {
        log.info("RawRecItem({}) incompatible now for uniqueContent missing", recItem.getId());
    }

    private enum TranslatedState {
        CONSTANT, BROKEN, NORMAL, UNDEFINED
    }

}
