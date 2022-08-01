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

package org.apache.kylin.metadata.cube.model;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.kylin.metadata.cube.model.IndexEntity.INDEX_ID_STEP;
import static org.apache.kylin.metadata.cube.model.IndexEntity.isAggIndex;
import static org.apache.kylin.metadata.cube.model.IndexEntity.isTableIndex;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.MissingRootPersistentEntity;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.guava20.shaded.common.collect.ImmutableSortedSet;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@SuppressWarnings("serial")
public class IndexPlan extends RootPersistentEntity implements Serializable, IEngineAware {

    @JsonProperty("description")
    @Getter
    private String description;

    @JsonProperty("index_plan_override_indexes")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<Integer, String> indexPlanOverrideIndexes = Maps.newHashMap();

    @Getter
    @JsonManagedReference
    @JsonProperty("rule_based_index")
    private RuleBasedIndex ruleBasedIndex;

    @Getter
    @JsonManagedReference
    @JsonProperty("indexes")
    private List<IndexEntity> indexes = Lists.newArrayList();
    @JsonProperty("override_properties")
    private LinkedHashMap<String, String> overrideProps = Maps.newLinkedHashMap();

    @Getter
    @JsonProperty("to_be_deleted_indexes")
    private final List<IndexEntity> toBeDeletedIndexes = Lists.newArrayList();

    @JsonProperty("auto_merge_time_ranges")
    private long[] autoMergeTimeRanges;
    @Getter
    @JsonProperty("retention_range")
    private long retentionRange = 0;
    @JsonProperty("engine_type")
    private int engineType = IEngineAware.ID_KAP_NSPARK;
    @JsonProperty("dictionaries")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<NDictionaryDesc> dictionaries;
    @Getter
    @JsonProperty("next_aggregation_index_id")
    private long nextAggregationIndexId = 0;
    @Getter
    @JsonProperty("next_table_index_id")
    private long nextTableIndexId = IndexEntity.TABLE_INDEX_START_ID;
    @Getter
    @JsonProperty("agg_shard_by_columns")
    private List<Integer> aggShardByColumns = Lists.newArrayList();

    @Getter
    @JsonProperty("extend_partition_columns")
    private List<Integer> extendPartitionColumns = Lists.newArrayList();

    @Setter
    @Getter
    @JsonProperty("layout_bucket_num")
    private Map<Long, Integer> layoutBucketNumMapping = Maps.newHashMap();

    @Getter
    @Setter
    @JsonProperty("approved_additional_recs")
    private int approvedAdditionalRecs;

    @Getter
    @Setter
    @JsonProperty("approved_removal_recs")
    private int approvedRemovalRecs;

    // computed fields below
    @Setter
    private String project;

    @Setter
    private KylinConfigExt config = null;
    private long prjMvccWhenConfigInitted = -1;
    private long indexPlanMvccWhenConfigInitted = -1;

    private transient BiMap<Integer, TblColRef> effectiveDimCols; // BiMap impl (com.google.common.collect.Maps$FilteredEntryBiMap) is not serializable
    private transient BiMap<Integer, NDataModel.Measure> effectiveMeasures; // BiMap impl (com.google.common.collect.Maps$FilteredEntryBiMap) is not serializable

    //TODO: should move allColumns and allColumnDescs to model? no need to exist in cubeplan
    private final LinkedHashSet<TblColRef> allColumns = Sets.newLinkedHashSet();
    private final LinkedHashSet<ColumnDesc> allColumnDescs = Sets.newLinkedHashSet();

    private List<LayoutEntity> ruleBasedLayouts = Lists.newArrayList();
    @Setter
    @Getter
    private LayoutEntity baseAggLayout;
    @Getter
    private LayoutEntity baseTableLayout;

    @Getter(value = AccessLevel.PRIVATE, lazy = true)
    private final IdMapping idMapping = initIdMapping();

    /**
     * Error messages during resolving json metadata
     */
    private final List<String> errors = Lists.newLinkedList();

    public void initAfterReload(KylinConfig config, String p) {
        this.project = p;
        initConfig4IndexPlan(config);

        checkNotNull(getModel(), "NDataModel(%s) not found", uuid);
        initAllCuboids();
        initDimensionAndMeasures();
        initAllColumns();
        initDictionaryDesc();
        initBaseLayouts();

        this.setDependencies(calcDependencies());
    }

    private void initBaseLayouts() {
        for (IndexEntity index : indexes) {
            for (LayoutEntity layout : index.getLayouts()) {
                if (layout.isBase() && isTableIndex(layout.getId())) {
                    baseTableLayout = layout;
                }
                if (layout.isBase() && isAggIndex(layout.getId())) {
                    baseAggLayout = layout;
                }
            }
        }
    }

    @Override
    public List<RootPersistentEntity> calcDependencies() {
        val manager = NDataModelManager.getInstance(config, project);
        NDataModel dataModelDesc = manager.getDataModelDesc(uuid);

        return Lists.newArrayList(dataModelDesc != null ? dataModelDesc
                : new MissingRootPersistentEntity(NDataModel.concatResourcePath(uuid, project)));
    }

    private void initConfig4IndexPlan(KylinConfig config) {

        Map<String, String> newOverrides = Maps.newLinkedHashMap(this.overrideProps);
        ProjectInstance ownerPrj = NProjectManager.getInstance(config).getProject(project);
        // cube inherit the project override props
        Map<String, String> prjOverrideProps = ownerPrj.getLegalOverrideKylinProps();
        for (Map.Entry<String, String> entry : prjOverrideProps.entrySet()) {
            if (!newOverrides.containsKey(entry.getKey())) {
                newOverrides.put(entry.getKey(), entry.getValue());
            }
        }

        this.config = KylinConfigExt.createInstance(config, newOverrides);
        this.prjMvccWhenConfigInitted = ownerPrj.getMvcc();
        this.indexPlanMvccWhenConfigInitted = this.getMvcc();
    }

    private void initAllCuboids() {
        if (ruleBasedIndex == null) {
            return;
        }
        ruleBasedIndex.init();
        ruleBasedLayouts.addAll(ruleBasedIndex.genCuboidLayouts());
        if (config.base().isSystemConfig() && isCachedAndShared) {
            ruleBasedIndex.getCuboidScheduler().validateOrder();
        }
    }

    private void initDimensionAndMeasures() {
        val indexes = getAllIndexes();
        int size1 = 1;
        int size2 = 1;
        for (IndexEntity cuboid : indexes) {
            size1 = Math.max(cuboid.getDimensionBitset().size(), size1);
            size2 = Math.max(cuboid.getMeasureBitset().size(), size2);
        }

        final BitSet dimBitSet = new BitSet(size1);
        final BitSet measureBitSet = new BitSet(size2);

        for (IndexEntity cuboid : indexes) {
            dimBitSet.or(cuboid.getDimensionBitset().mutable());
            measureBitSet.or(cuboid.getMeasureBitset().mutable());
        }

        this.effectiveDimCols = Maps.filterKeys(getModel().getEffectiveCols(),
                input -> input != null && dimBitSet.get(input));
        this.effectiveMeasures = Maps.filterKeys(getModel().getEffectiveMeasures(),
                input -> input != null && measureBitSet.get(input));
    }

    private void initAllColumns() {
        allColumns.clear();
        allColumnDescs.clear();

        allColumns.addAll(effectiveDimCols.values());
        for (NDataModel.Measure measure : effectiveMeasures.values()) {
            allColumns.addAll(measure.getFunction().getColRefs());
        }
        for (JoinTableDesc join : getModel().getJoinTables()) {
            CollectionUtils.addAll(allColumns, join.getJoin().getForeignKeyColumns());
            //all lookup tables are automatically derived
            allColumns.addAll(join.getTableRef().getColumns());
        }

        for (TblColRef colRef : allColumns) {
            allColumnDescs.add(colRef.getColumnDesc());
        }
    }

    private void initDictionaryDesc() {
        if (dictionaries != null) {
            for (NDictionaryDesc dictDesc : dictionaries) {
                dictDesc.init(getModel());
                allColumns.add(dictDesc.getColumnRef());
                if (dictDesc.getResuseColumnRef() != null) {
                    allColumns.add(dictDesc.getResuseColumnRef());
                }
            }
        }
    }

    @Override
    public String resourceName() {
        return uuid;
    }

    public IndexPlan copy() {
        return NIndexPlanManager.getInstance(config, project).copy(this);
    }

    public IndexEntity getIndexEntity(long indexId) {
        return getIdMapping().getIndexEntity(indexId);
    }

    public LayoutEntity getLayoutEntity(Long layoutId) {
        return getIdMapping().getLayoutEntity(layoutId);
    }

    public KylinConfig getConfig() {
        if(config == null){
            // when the indexplan is not inited
            return null;
        }

        ProjectInstance ownerPrj = NProjectManager.getInstance(config).getProject(project);
        if (ownerPrj.getMvcc() != prjMvccWhenConfigInitted || this.getMvcc() != indexPlanMvccWhenConfigInitted) {
            initConfig4IndexPlan(this.config);
        }
        return config;
    }

    public void addError(String message) {
        this.errors.add(message);
    }

    public List<String> getError() {
        return this.errors;
    }

    String getErrorMsg() {
        return Joiner.on(" ").join(errors);
    }

    @Override
    public String getResourcePath() {
        return concatResourcePath(getUuid(), project);
    }

    public static String concatResourcePath(String name, String project) {
        return new StringBuilder().append("/").append(project).append(ResourceStore.INDEX_PLAN_RESOURCE_ROOT)
                .append("/").append(name).append(MetadataConstants.FILE_SURFIX).toString();
    }

    public String getProject() {
        return project;
    }

    public NDataModel getModel() {
        return NDataModelManager.getInstance(config, project).getDataModelDesc(uuid);
    }

    public String getModelAlias() {
        val model = getModel();
        return model == null ? null : model.getAlias();
    }

    public BiMap<Integer, TblColRef> getEffectiveDimCols() {
        return effectiveDimCols;
    }

    public BiMap<Integer, NDataModel.Measure> getEffectiveMeasures() {
        return effectiveMeasures;
    }

    public Set<ColumnDesc> listAllColumnDescs() {
        return allColumnDescs;
    }

    public Set<TblColRef> listAllTblColRefs() {
        return allColumns;
    }

    private void addLayout2TargetIndex(LayoutEntity sourceLayout, IndexEntity targetIndex) {
        addLayout2TargetIndex(sourceLayout, targetIndex, false);
    }

    private void addLayout2TargetIndex(LayoutEntity sourceLayout, IndexEntity targetIndex, boolean toBeDeleted) {
        Preconditions.checkNotNull(sourceLayout);
        Preconditions.checkNotNull(targetIndex);

        val originLayouts = targetIndex.getLayouts();

        boolean isMatch = originLayouts.stream().filter(originLayout -> originLayout.equals(sourceLayout))
                .peek(originLayout -> {
                    if (!originLayout.isBaseIndex()) {
                        originLayout.setManual(true);
                    }
                }).count() > 0;

        LayoutEntity copy = JsonUtil.deepCopyQuietly(sourceLayout, LayoutEntity.class);
        copy.setToBeDeleted(toBeDeleted);
        copy.setIndex(targetIndex);
        if (!isMatch) {
            originLayouts.add(copy);
        }

        targetIndex.setNextLayoutOffset(Math.max(targetIndex.getNextLayoutOffset(),
                originLayouts.stream().mapToLong(LayoutEntity::getId).max().orElse(0) % INDEX_ID_STEP + 1));
    }

    public List<IndexEntity> getAllIndexes() {
        return getAllIndexes(true);
    }

    /*
        Get a copy of all IndexEntity List, which is a time cost operation
     */
    public List<IndexEntity> getAllIndexes(boolean withToBeDeletedIndexes) {
        Map<Long, Integer> retSubscriptMap = Maps.newHashMap();
        List<IndexEntity> mergedIndexes = Lists.newArrayList();
        int retSubscript = 0;
        for (IndexEntity indexEntity : indexes) {
            val copy = JsonUtil.deepCopyQuietly(indexEntity, IndexEntity.class);
            retSubscriptMap.put(indexEntity.getId(), retSubscript);
            mergedIndexes.add(copy);
            Map<Long, LayoutEntity> layouts = Maps.newHashMap();
            indexEntity.getLayouts().forEach(layout -> layouts.putIfAbsent(layout.getId(), layout));
            copy.getLayouts().forEach(layout -> layout.setInProposing(layouts.get(layout.getId()).isInProposing()));
            retSubscript++;
        }
        for (LayoutEntity ruleBasedLayout : ruleBasedLayouts) {
            val ruleRelatedIndex = ruleBasedLayout.getIndex();
            if (!retSubscriptMap.containsKey(ruleRelatedIndex.getId())) {
                val copy = JsonUtil.deepCopyQuietly(ruleRelatedIndex, IndexEntity.class);
                retSubscriptMap.put(ruleRelatedIndex.getId(), retSubscript);
                mergedIndexes.add(copy);
                retSubscript++;
            }
            val subscript = retSubscriptMap.get(ruleRelatedIndex.getId());
            val targetIndex = mergedIndexes.get(subscript);
            addLayout2TargetIndex(ruleBasedLayout, targetIndex);
        }

        if (withToBeDeletedIndexes) {
            for (IndexEntity indexEntity : toBeDeletedIndexes) {
                if (!retSubscriptMap.containsKey(indexEntity.getId())) {
                    IndexEntity copy = JsonUtil.deepCopyQuietly(indexEntity, IndexEntity.class);
                    retSubscriptMap.put(indexEntity.getId(), retSubscript);
                    mergedIndexes.add(copy);
                    copy.getLayouts().forEach(layoutEntity -> layoutEntity.setToBeDeleted(true));
                    retSubscript++;
                } else {
                    val subscript = retSubscriptMap.get(indexEntity.getId());
                    val targetIndex = mergedIndexes.get(subscript);
                    for (LayoutEntity layoutEntity : indexEntity.getLayouts()) {
                        addLayout2TargetIndex(layoutEntity, targetIndex, true);
                    }
                }
            }
        }

        mergedIndexes.forEach(value -> value.setIndexPlan(this));
        return mergedIndexes;
    }

    public List<LayoutEntity> getAllLayouts() {
        List<LayoutEntity> r = Lists.newArrayList();

        for (IndexEntity cd : getAllIndexes()) {
            r.addAll(cd.getLayouts());
        }
        return r;
    }

    public List<ImmutablePair<LayoutEntity, Boolean>> getAllLayoutsReadOnly() {
        Map<Long, Map<LayoutEntity, Boolean>> resultMap = Maps.newHashMap();
        for (IndexEntity indexEntity : indexes) {
            indexEntity.getLayouts().forEach(layout -> classifyByIndexId(layout, resultMap, layout.isToBeDeleted()));
        }
        for (LayoutEntity ruleBasedLayout : ruleBasedLayouts) {
            classifyByIndexId(ruleBasedLayout, resultMap, false);
        }
        for (IndexEntity indexEntity : toBeDeletedIndexes) {
            indexEntity.getLayouts().forEach(layout -> classifyByIndexId(layout, resultMap, true));
            resultMap.get(indexEntity.getId()).entrySet().forEach(layoutMap -> layoutMap.setValue(true));
        }
        return resultMap.values().stream()//
                .flatMap(layoutMap -> //
                layoutMap.entrySet().stream().map(p -> ImmutablePair.of(p.getKey(), p.getValue())))
                .collect(Collectors.toList());
    }

    private void classifyByIndexId(LayoutEntity layout, Map<Long, Map<LayoutEntity, Boolean>> resultMap,
            boolean toBeDeleted) {
        resultMap.compute(layout.getIndexId(), (indexId, layoutMap) -> {
            layoutMap = layoutMap == null ? Maps.newHashMap() : layoutMap;
            layoutMap.put(layout, toBeDeleted);
            return layoutMap;
        });
    }

    public Map<Long, LayoutEntity> getAllLayoutsMap() {
        Map<Long, LayoutEntity> map = Maps.newHashMap();
        getAllLayouts().forEach(layout -> map.putIfAbsent(layout.getId(), layout));
        return map;
    }

    public List<LayoutEntity> getWhitelistLayouts() {
        List<LayoutEntity> r = Lists.newArrayList();

        for (IndexEntity cd : indexes) {
            r.addAll(cd.getLayouts());
        }
        return r;
    }

    public List<LayoutEntity> getRuleBaseLayouts() {
        return isCachedAndShared ? ImmutableList.copyOf(ruleBasedLayouts) : ruleBasedLayouts;
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================
    public void setIndexes(List<IndexEntity> indexes) {
        checkIsNotCachedAndShared();
        this.indexes = indexes;
        updateNextId();
    }

    private Set<LayoutEntity> layoutsNotIn(Set<LayoutEntity> source, Set<LayoutEntity> target) {
        Preconditions.checkNotNull(source);
        Preconditions.checkNotNull(target);
        return source.stream().filter(layout -> !target.contains(layout)).collect(Collectors.toSet());
    }

    public UpdateRuleImpact diffRuleBasedIndex(RuleBasedIndex ruleBasedIndex) {
        ruleBasedIndex.adjustMeasures();
        if (CollectionUtils.isEmpty(ruleBasedIndex.getMeasures())) {
            ruleBasedIndex.setMeasures(Lists.newArrayList(getModel().getEffectiveMeasures().keySet()));
        }
        ruleBasedIndex.setIndexStartId(nextAggregationIndexId);
        ruleBasedIndex.setIndexPlan(this);
        ruleBasedIndex.init();

        Set<LayoutEntity> sourceLayouts = null == this.ruleBasedIndex ? Sets.newHashSet()
                : this.ruleBasedIndex.genCuboidLayouts();

        Set<LayoutEntity> previousDeletedLayouts = null == this.ruleBasedIndex ? Sets.newHashSet()
                : this.ruleBasedIndex.getBlacklistLayouts();

        ruleBasedIndex.genCuboidLayouts(Sets.newHashSet(sourceLayouts), previousDeletedLayouts);
        Set<LayoutEntity> targetLayouts = ruleBasedIndex.genCuboidLayouts();

        return new UpdateRuleImpact(layoutsNotIn(sourceLayouts, targetLayouts),
                layoutsNotIn(targetLayouts, sourceLayouts), ruleBasedIndex.getBlacklistLayouts());
    }

    public void setRuleBasedIndex(RuleBasedIndex ruleBasedIndex) {
        setRuleBasedIndex(ruleBasedIndex, false);
    }

    public void setRuleBasedIndex(RuleBasedIndex ruleBasedIndex, boolean reuseStartId) {
        setRuleBasedIndex(ruleBasedIndex, reuseStartId, false);
    }

    public void setRuleBasedIndex(RuleBasedIndex ruleBasedIndex, boolean reuseStartId, boolean markToBeDeleted) {
        setRuleBasedIndex(ruleBasedIndex, Sets.newHashSet(), reuseStartId, markToBeDeleted, false);
    }

    public void setRuleBasedIndex(RuleBasedIndex ruleBasedIndex, Set<LayoutEntity> reloadLayouts, boolean reuseStartId,
            boolean markToBeDeleted, boolean restoreDeletedIndex) {
        checkIsNotCachedAndShared();
        ruleBasedIndex.adjustMeasures();
        if (CollectionUtils.isEmpty(ruleBasedIndex.getMeasures())) {
            ruleBasedIndex.setMeasures(Lists.newArrayList(getModel().getEffectiveMeasures().keySet()));
        }
        ruleBasedIndex.setIndexStartId(reuseStartId ? ruleBasedIndex.getIndexStartId() : nextAggregationIndexId);
        ruleBasedIndex.setIndexPlan(this);
        ruleBasedIndex.init();
        Set<LayoutEntity> originSet = this.ruleBasedIndex == null ? Sets.newHashSet()
                : this.ruleBasedIndex.genCuboidLayouts();

        Set<LayoutEntity> needDelLayouts = Sets.newHashSet();
        if (!restoreDeletedIndex) {
            needDelLayouts = getBlacklistLayouts();
            needDelLayouts = needDelLayouts.stream().filter(l -> !reloadLayouts.contains(l))
                    .collect(Collectors.toSet());
        }
        ruleBasedIndex.genCuboidLayouts(Sets.newHashSet(originSet), needDelLayouts);

        this.ruleBasedIndex = ruleBasedIndex;

        Set<LayoutEntity> targetSet = this.ruleBasedIndex.genCuboidLayouts();

        this.ruleBasedLayouts = Lists.newArrayList(targetSet);
        if (markToBeDeleted && CollectionUtils.isNotEmpty(layoutsNotIn(targetSet, originSet))) {
            Set<LayoutEntity> toBeDeletedSet = layoutsNotIn(originSet, targetSet);
            if (CollectionUtils.isNotEmpty(toBeDeletedSet)) {
                markIndexesToBeDeleted(ruleBasedIndex.getIndexPlan().getUuid(), toBeDeletedSet);
            }
        }

        updateNextId();
    }

    private Set<LayoutEntity> getBlacklistLayouts() {
        if (this.ruleBasedIndex == null) {
            return Sets.newHashSet();
        }
        return this.ruleBasedIndex.getBlacklistLayouts();
    }

    public void setAggShardByColumns(List<Integer> aggShardByColumns) {
        checkIsNotCachedAndShared();
        if (ruleBasedIndex != null) {
            val ruleLayouts = ruleBasedIndex.genCuboidLayouts();
            this.aggShardByColumns = aggShardByColumns;
            ruleBasedIndex.setIndexStartId(nextAggregationIndexId);
            ruleBasedIndex.setLayoutIdMapping(Lists.newArrayList());
            ruleBasedIndex.genCuboidLayouts(ruleLayouts);
            this.ruleBasedLayouts = Lists.newArrayList(ruleBasedIndex.genCuboidLayouts());
        }
        this.aggShardByColumns = aggShardByColumns;
        updateNextId();
    }

    public void setExtendPartitionColumns(List<Integer> extendPartitionColumns) {
        checkIsNotCachedAndShared();
        if (ruleBasedIndex != null) {
            val ruleLayouts = ruleBasedIndex.genCuboidLayouts();
            this.extendPartitionColumns = extendPartitionColumns;
            ruleBasedIndex.setLayoutIdMapping(Lists.newArrayList());
            ruleBasedIndex.genCuboidLayouts(ruleLayouts);
            this.ruleBasedLayouts = Lists.newArrayList(ruleBasedIndex.genCuboidLayouts());
        }
        this.extendPartitionColumns = extendPartitionColumns;
        updateNextId();
    }

    private Map<IndexEntity.IndexIdentifier, IndexEntity> getToBeDeletedIndexesMap() {
        return getIndexesMap(toBeDeletedIndexes);
    }

    @VisibleForTesting
    public void markIndexesToBeDeleted(String indexPlanId, final Set<LayoutEntity> toBeDeletedSet) {
        Preconditions.checkNotNull(indexPlanId);
        Preconditions.checkNotNull(toBeDeletedSet);
        checkIsNotCachedAndShared();

        if (CollectionUtils.isEmpty(toBeDeletedSet)) {
            return;
        }

        NDataflow df = NDataflowManager.getInstance(config, project).getDataflow(indexPlanId);
        val readySegs = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        NDataSegment lastReadySegment = readySegs.getLatestReadySegment();
        if (null == lastReadySegment) {
            return;
        }

        val toBeDeletedMap = getToBeDeletedIndexesMap();
        for (LayoutEntity layoutEntity : toBeDeletedSet) {
            if (null == lastReadySegment.getLayout(layoutEntity.getId())) {
                continue;
            }

            val identifier = layoutEntity.getIndex().createIndexIdentifier();
            if (!toBeDeletedMap.containsKey(identifier)) {
                IndexEntity newIndexEntity = JsonUtil.deepCopyQuietly(layoutEntity.getIndex(), IndexEntity.class);
                newIndexEntity.setLayouts(Lists.newArrayList());
                this.toBeDeletedIndexes.add(newIndexEntity);
                toBeDeletedMap.put(identifier, newIndexEntity);
            }
            LayoutEntity layout = JsonUtil.deepCopyQuietly(layoutEntity, LayoutEntity.class);
            if (!toBeDeletedMap.get(identifier).getLayouts().contains(layout)) {
                toBeDeletedMap.get(identifier).getLayouts().add(layout);
            }
        }

    }

    public void markWhiteIndexToBeDelete(String indexPlanId, final Set<Long> layoutIds) {
        Preconditions.checkNotNull(indexPlanId);
        Preconditions.checkNotNull(layoutIds);
        checkIsNotCachedAndShared();

        Set<LayoutEntity> toBeDeletedLayouts = Sets.newHashSet();
        for (IndexEntity indexEntity : getIndexes()) {
            for (LayoutEntity layoutEntity : indexEntity.getLayouts()) {
                if (layoutIds.contains(layoutEntity.getId())) {
                    toBeDeletedLayouts.add(layoutEntity);
                }
            }
        }

        markIndexesToBeDeleted(indexPlanId, toBeDeletedLayouts);

        for (LayoutEntity layoutEntity : toBeDeletedLayouts) {
            // delete layouts from indexes.
            layoutEntity.getIndex().removeLayouts(Lists.newArrayList(layoutEntity), null, true, true);
        }
    }

    public void removeTobeDeleteIndexIfNecessary() {
        checkIsNotCachedAndShared();
        val toBeDeleteIds = this.getAllToBeDeleteLayoutId();
        if (toBeDeleteIds.isEmpty()) {
            return;
        }
        NDataflow dataFlow = NDataflowManager.getInstance(config, project).getDataflow(getUuid());
        if (null == dataFlow.getLatestReadySegment()) {
            toBeDeletedIndexes.clear();
            return;
        }

        boolean existTobeDeleteLayout = false;
        for (NDataSegment seg : dataFlow.getSegments()) {
            if (CollectionUtils.isNotEmpty(Sets.intersection(seg.getLayoutsMap().keySet(), toBeDeleteIds))) {
                existTobeDeleteLayout = true;
                break;
            }
        }
        if (!existTobeDeleteLayout) {
            toBeDeletedIndexes.clear();
        }
    }

    public void addRuleBasedBlackList(Collection<Long> blacklist) {
        checkIsNotCachedAndShared();
        if (ruleBasedIndex != null) {
            val originBlacklist = ruleBasedIndex.getLayoutBlackList();
            val newBlacklist = Sets.newHashSet(originBlacklist);
            newBlacklist.addAll(blacklist);
            ruleBasedIndex.setLayoutBlackList(newBlacklist);
            this.ruleBasedLayouts = Lists.newArrayList(ruleBasedIndex.genCuboidLayouts());
        }
        updateNextId();
    }

    public void updateNextId() {
        val allIndexes = getAllIndexes();
        nextAggregationIndexId = Math.max(allIndexes.stream().filter(c -> !c.isTableIndex())
                .mapToLong(IndexEntity::getId).max().orElse(-INDEX_ID_STEP) + INDEX_ID_STEP, nextAggregationIndexId);
        nextTableIndexId = Math.max(allIndexes.stream().filter(IndexEntity::isTableIndex).mapToLong(IndexEntity::getId)
                .max().orElse(IndexEntity.TABLE_INDEX_START_ID - INDEX_ID_STEP) + INDEX_ID_STEP, nextTableIndexId);
        val indexNextIdMap = allIndexes.stream().collect(Collectors.toMap(IndexEntity::getId,
                k -> k.getLayouts().stream().mapToLong(LayoutEntity::getId).max().orElse(0) % INDEX_ID_STEP + 1));
        for (IndexEntity index : indexes) {
            long nextLayoutId = indexNextIdMap.getOrDefault(index.getId(), 1L);
            index.setNextLayoutOffset(Math.max(nextLayoutId, index.getNextLayoutOffset()));
        }
    }

    public void setDescription(String description) {
        checkIsNotCachedAndShared();
        this.description = description;
    }

    public ImmutableMap<Integer, String> getIndexPlanOverrideIndexes() {
        return ImmutableMap.copyOf(this.indexPlanOverrideIndexes);
    }

    public void setIndexPlanOverrideIndexes(Map<Integer, String> m) {
        checkIsNotCachedAndShared();
        this.indexPlanOverrideIndexes = m;
    }

    public LinkedHashMap<String, String> getOverrideProps() {
        return isCachedAndShared ? Maps.newLinkedHashMap(overrideProps) : overrideProps;
    }

    public void setOverrideProps(LinkedHashMap<String, String> overrideProps) {
        checkIsNotCachedAndShared();
        this.overrideProps = KylinConfig.trimKVFromMap(overrideProps);
        initConfig4IndexPlan(this.config);
    }

    public long[] getAutoMergeTimeRanges() {
        return isCachedAndShared ? Arrays.copyOf(autoMergeTimeRanges, autoMergeTimeRanges.length) : autoMergeTimeRanges;
    }

    public void setAutoMergeTimeRanges(long[] autoMergeTimeRanges) {
        checkIsNotCachedAndShared();
        this.autoMergeTimeRanges = autoMergeTimeRanges;
    }

    public void setRetentionRange(long retentionRange) {
        checkIsNotCachedAndShared();
        this.retentionRange = retentionRange;
    }

    /**
     * will ignore rule based indexes;
     */
    public Map<IndexEntity.IndexIdentifier, IndexEntity> getWhiteListIndexesMap() {
        return getIndexesMap(indexes);
    }

    /**
     * include rule based indexes
     */
    public Map<IndexEntity.IndexIdentifier, IndexEntity> getAllIndexesMap() {
        return getIndexesMap(getAllIndexes());
    }

    private Map<IndexEntity.IndexIdentifier, IndexEntity> getIndexesMap(List<IndexEntity> indexEntities) {
        Map<IndexEntity.IndexIdentifier, IndexEntity> originIndexMap = Maps.newLinkedHashMap();
        for (IndexEntity indexEntity : indexEntities) {
            IndexEntity.IndexIdentifier identifier = indexEntity.createIndexIdentifier();
            if (!originIndexMap.containsKey(identifier)) {
                originIndexMap.put(identifier, indexEntity);
            } else {
                originIndexMap.get(identifier).getLayouts().addAll(indexEntity.getLayouts());
            }
        }
        return isCachedAndShared ? ImmutableMap.copyOf(originIndexMap) : originIndexMap;
    }

    @Override
    public int getEngineType() {
        return engineType;
    }

    public void setEngineType(int engineType) {
        checkIsNotCachedAndShared();
        this.engineType = engineType;
    }

    public List<NDictionaryDesc> getDictionaries() {
        return dictionaries == null ? null
                : (isCachedAndShared ? ImmutableList.copyOf(dictionaries) : Collections.unmodifiableList(dictionaries));
    }

    public void setDictionaries(List<NDictionaryDesc> dictionaries) {
        checkIsNotCachedAndShared();
        this.dictionaries = dictionaries;
    }

    @Override
    public String toString() {
        return "IndexPlan [" + uuid + "(" + getModelAlias() + ")]";
    }

    public void removeLayouts(Set<Long> layoutIds, boolean deleteAuto, boolean deleteManual) {
        removeLayouts(indexes, layoutIds, deleteAuto, deleteManual);
        removeLayouts(toBeDeletedIndexes, layoutIds, deleteAuto, deleteManual);
        addRuleBasedBlackList(Sets.intersection(
                getRuleBaseLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet()), layoutIds));
    }

    private void removeLayouts(Collection<IndexEntity> indexes, Set<Long> layoutIds, boolean deleteAuto,
            boolean deleteManual) {
        checkIsNotCachedAndShared();
        val indexIt = indexes.iterator();
        while (indexIt.hasNext()) {
            val index = indexIt.next();
            val it = index.getLayouts().iterator();
            while (it.hasNext()) {
                val layout = it.next();
                if (!layoutIds.contains(layout.getId())) {
                    continue;
                }
                if (deleteAuto) {
                    layout.setAuto(false);
                }
                if (deleteManual) {
                    layout.setManual(false);
                }
                if (layout.isExpired()) {
                    it.remove();
                }
            }
            if (index.getLayouts().isEmpty()) {
                indexIt.remove();
            }
        }
    }

    public boolean isSkipEncodeIntegerFamilyEnabled() {
        return overrideProps.containsKey("kylin.query.skip-encode-integer-enabled")
                && Boolean.parseBoolean(overrideProps.get("kylin.query.skip-encode-integer-enabled"));
    }

    public boolean isFastBitmapEnabled() {
        return overrideProps.containsKey("kylin.query.fast-bitmap-enabled")
                && Boolean.parseBoolean(overrideProps.get("kylin.query.fast-bitmap-enabled"));
    }

    public boolean containBaseTableLayout() {
        return baseTableLayout != null;
    }

    public boolean containBaseAggLayout() {
        return baseAggLayout != null;
    }

    public int getBaseIndexCount() {
        int num = 0;
        if (baseAggLayout != null) {
            num++;
        }
        if (baseTableLayout != null) {
            num++;
        }
        return num;
    }

    public void addIndex(IndexEntity index, boolean needAssignId) {
        checkIsNotCachedAndShared();
        indexes.add(index);
        if (needAssignId) {
            if (!index.isTableIndex()) {
                index.assignId(nextAggregationIndexId);
            } else {
                index.assignId(nextTableIndexId);
            }
            updateNextId();
        }
    }

    public LayoutEntity createBaseAggIndex(NDataModel model) {
        List<Integer> dims = model.getEffectiveDimensions().keySet().asList();
        List<Integer> measures = model.getEffectiveMeasures().keySet().asList();
        List<Integer> colOrder = naturalOrderCombine(dims, measures);
        return createLayout(colOrder, true);
    }

    public LayoutEntity createBaseTableIndex() {
        return createBaseTableIndex(getModel());
    }

    public LayoutEntity createBaseTableIndex(NDataModel model) {
        List<Integer> dims = model.getEffectiveDimensions().keySet().asList();
        List<Integer> measureCols = model.getMeasureRelatedCols();
        List<Integer> colOrder = naturalOrderCombine(dims, measureCols);
        if (colOrder.isEmpty()) {
            return null;
        }
        return createLayout(colOrder, false);
    }

    private LayoutEntity createLayout(List<Integer> colOrder, boolean isAgg) {
        LayoutEntity newBaseLayout = new LayoutEntity();
        newBaseLayout.setColOrder(colOrder);
        newBaseLayout.setUpdateTime(System.currentTimeMillis());
        newBaseLayout.setBase(true);
        newBaseLayout.initalId(isAgg);
        return newBaseLayout;
    }

    private List<Integer> naturalOrderCombine(List<Integer> col1, List<Integer> col2) {
        return ImmutableSortedSet.<Integer> naturalOrder().addAll(col1).addAll(col2).build().asList();
    }

    public Optional<LayoutEntity> removeLayoutSameWith(LayoutEntity layout) {
        Optional<LayoutEntity> oldLayout = getIdMapping().allLayoutMapping.values().stream()
                .filter(l -> l.equals(layout)).findFirst();
        if (oldLayout.isPresent()) {
            removeLayouts(Sets.newHashSet(oldLayout.get().getId()), true, true);
        }
        return oldLayout;
    }

    public boolean needUpdateBaseAggLayout(LayoutEntity replace, boolean isAuto) {
        return needUpdate(baseAggLayout, replace, isAuto);
    }

    public boolean needUpdateBaseTableLayout(LayoutEntity replace, boolean isAuto) {
        return needUpdate(baseTableLayout, replace, isAuto);
    }

    private boolean needUpdate(LayoutEntity curLayout, LayoutEntity replace, boolean isAuto) {
        if (curLayout == null) {
            return false;
        }
        if (!isAuto) {
            return !curLayout.equals(replace);
        }
        return !curLayout.equalsCols(replace);
    }

    public void createAndAddBaseIndex(NDataModel model) {
        checkIsNotCachedAndShared();
        LayoutEntity agg = createBaseAggIndex(model);
        LayoutEntity table = createBaseTableIndex(model);
        List<LayoutEntity> baseLayouts = Lists.newArrayList();
        baseLayouts.add(agg);
        if (table != null) {
            baseLayouts.add(table);
        }
        createAndAddBaseIndex(baseLayouts);
    }

    public void createAndAddBaseIndex(List<LayoutEntity> needCreateBaseLayouts) {
        checkIsNotCachedAndShared();
        val allIndexMapping = getAllIndexesMap();
        for (LayoutEntity baseLayout : needCreateBaseLayouts) {

            //if layout exist in indexes, use its id
            Optional<LayoutEntity> oldLayout = removeLayoutSameWith(baseLayout);
            if (oldLayout.isPresent()) {
                baseLayout.setId(oldLayout.get().getId());
            }

            val newIndex = IndexEntity.from(baseLayout);
            IndexEntity indexInAll = allIndexMapping.get(newIndex.createIndexIdentifier());
            if (indexInAll == null) {
                addIndex(newIndex, baseLayout.notAssignId());
            } else {

                IndexEntity indexInWhiteList = getWhiteListIndexesMap().get(newIndex.createIndexIdentifier());
                if (indexInWhiteList != null) {
                    IndexEntity realIndex = getIndexes().get(getIndexes().indexOf(indexInWhiteList));
                    realIndex.addLayout(baseLayout);
                } else {
                    // clear layouts in indexInAll which in rulebaseindex or tobedelete
                    indexInAll.setLayouts(Lists.newArrayList());
                    indexInAll.addLayout(baseLayout);
                    getIndexes().add(indexInAll);
                }
            }

        }

    }

    public Long getBaseAggLayoutId() {
        return baseAggLayout != null ? baseAggLayout.getId() : null;
    }

    public Long getBaseTableLayoutId() {
        return baseTableLayout != null ? baseTableLayout.getId() : null;
    }

    public Set<Long> getAllLayoutIds(boolean includeTobeDeleted) {
        return getIdMapping().getAllLayoutIds(includeTobeDeleted);
    }

    public Set<Long> getAllToBeDeleteLayoutId() {
        return getIdMapping().getAllToBeDelete();
    }

    public long getAllLayoutsSize(boolean includeTobeDeleted) {
        return getIdMapping().getAllLayoutsSize(includeTobeDeleted);
    }

    public class IndexPlanUpdateHandler {
        IndexPlan indexPlan;
        Map<IndexEntity.IndexIdentifier, IndexEntity> whiteIndexesMap;
        Map<IndexEntity.IndexIdentifier, IndexEntity> allIndexesMap;
        AtomicLong nextAggregationIndexId;
        AtomicLong nextTableIndexId;
        int approvedAdditionalRecs;
        int approvedRemovalRecs;
        @Getter
        private Set<Long> addedLayouts = Sets.newHashSet();

        private IndexPlanUpdateHandler() {
            indexPlan = IndexPlan.this.isCachedAndShared ? JsonUtil.deepCopyQuietly(IndexPlan.this, IndexPlan.class)
                    : IndexPlan.this;
            whiteIndexesMap = indexPlan.getWhiteListIndexesMap();
            allIndexesMap = indexPlan.getAllIndexesMap();
            nextAggregationIndexId = new AtomicLong(indexPlan.getNextAggregationIndexId());
            nextTableIndexId = new AtomicLong(indexPlan.getNextTableIndexId());
        }

        public boolean add(LayoutEntity layout, boolean isAgg) {
            val identifier = createIndexIdentifier(layout, isAgg);
            if (allIndexesMap.get(identifier) != null && allIndexesMap.get(identifier).getLayouts().contains(layout)) {
                return false;
            }
            if (!whiteIndexesMap.containsKey(identifier)) {
                val index = new IndexEntity();
                index.setDimensions(getDimensions(layout));
                index.setMeasures(getMeasures(layout));
                index.setNextLayoutOffset(1);
                if (allIndexesMap.get(identifier) != null) {
                    index.setId(allIndexesMap.get(identifier).getId());
                    index.setNextLayoutOffset(allIndexesMap.get(identifier).getNextLayoutOffset());
                } else {
                    index.setId(isAgg ? nextAggregationIndexId.getAndAdd(IndexEntity.INDEX_ID_STEP)
                            : nextTableIndexId.getAndAdd(IndexEntity.INDEX_ID_STEP));
                }
                layout.setIndex(index);
                layout.setId(index.getId() + index.getNextLayoutOffset());
                index.setLayouts(Lists.newArrayList(layout));
                index.setNextLayoutOffset(index.getNextLayoutOffset() + 1);
                whiteIndexesMap.put(identifier, index);
                addedLayouts.add(layout.getId());
            } else {
                val indexEntity = whiteIndexesMap.get(identifier);
                if (indexEntity.getLayouts().contains(layout)) {
                    return false;
                }
                layout.setId(indexEntity.getId() + indexEntity.getNextLayoutOffset());
                layout.setIndex(indexEntity);
                indexEntity.setNextLayoutOffset(indexEntity.getNextLayoutOffset() + 1);
                indexEntity.getLayouts().add(layout);
                addedLayouts.add(layout.getId());
            }
            approvedAdditionalRecs += 1;
            return true;
        }

        public boolean remove(LayoutEntity layout, boolean isAgg, boolean needAddBlackList) {
            IndexEntity.IndexIdentifier identifier = createIndexIdentifier(layout, isAgg);
            if (allIndexesMap.containsKey(identifier) && allIndexesMap.get(identifier).getLayouts().contains(layout)) {
                IndexEntity indexEntity = allIndexesMap.get(identifier);
                LayoutEntity layoutInIndexPlan = indexEntity.getLayout(layout.getId());
                if (layoutInIndexPlan == null) {
                    return false;
                }

                if (layoutInIndexPlan.isManual()) {
                    if (isAgg && needAddBlackList) {
                        // For similar strategy only works on AggIndex, we need add this to black list.
                        indexPlan.addRuleBasedBlackList(Lists.newArrayList(layout.getId()));
                        if (layoutInIndexPlan.isAuto()) {
                            indexEntity.getLayouts().remove(layoutInIndexPlan);
                            whiteIndexesMap.values().stream().filter(
                                    indexEntityInIndexPlan -> indexEntityInIndexPlan.getId() == indexEntity.getId())
                                    .findFirst().ifPresent(indexEntityInIndexPlan -> indexEntityInIndexPlan.getLayouts()
                                            .remove(layoutInIndexPlan));
                        }
                        this.approvedRemovalRecs += 1;
                        return true;
                    }
                    return false;
                }
                indexEntity.getLayouts().remove(layoutInIndexPlan);
                whiteIndexesMap.values().stream()
                        .filter(indexEntityInIndexPlan -> indexEntityInIndexPlan.getId() == indexEntity.getId())
                        .findFirst().ifPresent(indexEntityInIndexPlan -> indexEntityInIndexPlan.getLayouts()
                                .remove(layoutInIndexPlan));
                this.approvedRemovalRecs += 1;
                return true;
            } else {
                return false;
            }
        }

        public IndexPlan complete() {
            indexPlan.setIndexes(whiteIndexesMap.values().stream().sorted(Comparator.comparingLong(IndexEntity::getId))
                    .collect(Collectors.toList()));
            indexPlan.setApprovedAdditionalRecs(indexPlan.getApprovedAdditionalRecs() + approvedAdditionalRecs);
            indexPlan.setApprovedRemovalRecs(indexPlan.getApprovedRemovalRecs() + approvedRemovalRecs);
            return indexPlan;
        }
    }

    public IndexPlan.IndexPlanUpdateHandler createUpdateHandler() {
        return new IndexPlanUpdateHandler();
    }

    public IndexEntity.IndexIdentifier createIndexIdentifier(LayoutEntity layout, boolean agg) {
        return new IndexEntity.IndexIdentifier(//
                getDimensions(layout), //
                getMeasures(layout), //
                !agg//
        );
    }

    @JsonIgnore
    public List<Integer> getMeasures(LayoutEntity layout) {
        return layout.getColOrder().stream().filter(i -> i >= NDataModel.MEASURE_ID_BASE).collect(Collectors.toList());
    }

    @JsonIgnore
    public List<Integer> getDimensions(LayoutEntity layout) {
        return layout.getColOrder().stream().filter(i -> i < NDataModel.MEASURE_ID_BASE).collect(Collectors.toList());
    }

    public boolean isOfflineManually() {
        return getOverrideProps().getOrDefault(KylinConfig.MODEL_OFFLINE_FLAG, "false").trim().equals(KylinConfig.TRUE);
    }

    private IdMapping initIdMapping() {
        return new IdMapping();
    }

    @AllArgsConstructor
    @Data
    public static class UpdateRuleImpact {
        private Set<LayoutEntity> decreaseLayouts;

        private Set<LayoutEntity> increaseLayouts;

        private Set<LayoutEntity> rollbackLayouts;
    }

    private class IdMapping implements Serializable {
        private Map<Long, IndexEntity> allIndexMapping;
        private Map<Long, LayoutEntity> allLayoutMapping;

        IdMapping() {
            allIndexMapping = getAllIndexes().stream()
                    .collect(Collectors.toMap(IndexEntity::getId, Function.identity()));
            allLayoutMapping = allIndexMapping.values().stream().flatMap(index -> index.getLayouts().stream())
                    .collect(Collectors.toMap(LayoutEntity::getId, Function.identity()));
        }

        public IndexEntity getIndexEntity(long indexId) {
            return allIndexMapping.get(indexId);
        }

        public LayoutEntity getLayoutEntity(Long layoutId) {
            return allLayoutMapping.get(layoutId);
        }

        public Set<Long> getAllLayoutIds(boolean includeTobeDeleted) {
            if (includeTobeDeleted) {
                return allLayoutMapping.keySet();
            }
            return allLayoutMapping.values().stream().filter(layout -> !layout.isToBeDeleted()).map(LayoutEntity::getId)
                    .collect(Collectors.toSet());
        }

        public long getAllLayoutsSize(boolean includeTobeDeleted) {
            if (includeTobeDeleted) {
                return allLayoutMapping.size();
            }
            return allLayoutMapping.values().stream().filter(layout -> !layout.isToBeDeleted()).count();
        }

        public Set<Long> getAllToBeDelete() {
            return allLayoutMapping.values().stream().filter(LayoutEntity::isToBeDeleted).map(LayoutEntity::getId)
                    .collect(Collectors.toSet());
        }
    }
}
