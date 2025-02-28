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

package org.apache.kylin.rec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexEntity.IndexIdentifier;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.index.IndexMaster;
import org.apache.kylin.rec.util.EntityBuilder;

import lombok.val;

public class IndexPlanShrinkProposer extends AbstractProposer {

    public IndexPlanShrinkProposer(AbstractContext proposeContext) {
        super(proposeContext);
    }

    @Override
    public void execute() {
        if (proposeContext.getModelContexts() == null)
            return;

        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(proposeContext.getProject());
        for (AbstractContext.ModelContext modelCtx : proposeContext.getModelContexts()) {
            if (modelCtx.getTargetIndexPlan() == null) {
                continue;
            }

            mergeIndexPlan(modelCtx);
            IndexMaster indexMaster = new IndexMaster(modelCtx);
            IndexPlan indexPlan = projectInstance.isSemiAutoMode()
                    ? indexMaster.reduceCuboids(modelCtx.getTargetIndexPlan())
                    : modelCtx.getTargetIndexPlan(); // at present only semi-auto-mode use this for ci problem
            modelCtx.setTargetIndexPlan(indexPlan);
        }
    }

    @Override
    public String getIdentifierName() {
        return "IndexPlanShrinkProposer";
    }

    private void mergeIndexPlan(AbstractContext.ModelContext modelContext) {
        IndexPlan indexPlan = modelContext.getTargetIndexPlan();
        if (indexPlan == null || CollectionUtils.isEmpty(indexPlan.getIndexes())) {
            return;
        }

        val dim2Indices = recognizeAggIndexGroupOfSameDims(indexPlan);
        List<IndexEntity> readyIndices = collectTableOrReadyIndices(indexPlan);

        val newDim2Index = mergeIndicesGroupOfSameDim(modelContext, dim2Indices, readyIndices);

        dropDuplicateLayouts(readyIndices, newDim2Index);
        mergeProposingIndexToExist(readyIndices, newDim2Index);

        updateLayoutRecItem(modelContext, readyIndices, newDim2Index);
        updateIndexPlan(indexPlan, readyIndices, newDim2Index);
    }

    private void updateIndexPlan(IndexPlan indexPlan, List<IndexEntity> existedTableIndicesOrReadyIndices,
            HashMap<ImmutableBitSet, List<IndexEntity>> newDim2Index) {
        indexPlan.getIndexes().clear();
        indexPlan.getIndexes().addAll(existedTableIndicesOrReadyIndices);
        indexPlan.getIndexes().addAll(newDim2Index.entrySet().stream().map(Map.Entry::getValue).flatMap(List::stream)
                .collect(Collectors.toSet()));
    }

    private void updateLayoutRecItem(AbstractContext.ModelContext modelContext, List<IndexEntity> readyIndices,
            HashMap<ImmutableBitSet, List<IndexEntity>> newDim2Index) {
        newDim2Index.forEach((k, v) -> v.forEach(index -> {
            List<LayoutEntity> layouts = index.getLayouts();
            layouts.forEach(layout -> {
                if (layout.isInProposing()) {
                    modelContext.gatherLayoutRecItem(layout);
                }
            });
        }));
        readyIndices.forEach(index -> {
            List<LayoutEntity> layouts = index.getLayouts();
            layouts.forEach(layout -> {
                if (layout.isInProposing()) {
                    modelContext.gatherLayoutRecItem(layout);
                }
            });
        });
    }

    private HashMap<ImmutableBitSet, List<IndexEntity>> mergeIndicesGroupOfSameDim(
            AbstractContext.ModelContext modelContext, Map<ImmutableBitSet, List<IndexEntity>> sameDimIndicesGroup,
            List<IndexEntity> readyIndices) {
        IndexPlan indexPlan = modelContext.getTargetIndexPlan();

        val newDim2Index = new HashMap<ImmutableBitSet, List<IndexEntity>>();
        sameDimIndicesGroup.entrySet().stream().filter(indices -> CollectionUtils.isNotEmpty(indices.getValue()))
                .forEach(indices -> {
                    if (indices.getValue().size() == 1) {
                        newDim2Index.put(indices.getKey(), indices.getValue());
                        return;
                    }

                    // gen available index id
                    List<IndexEntity> existedIndices = Lists.newArrayList(readyIndices);
                    existedIndices.addAll(newDim2Index.entrySet().stream().map(Map.Entry::getValue)
                            .flatMap(List::stream).collect(Collectors.toSet()));
                    long availableId = EntityBuilder.IndexEntityBuilder.findAvailableIndexEntityId(indexPlan,
                            existedIndices, false);

                    // merge each indices group to a new index
                    List<IndexEntity> reservedIndex = Lists.newArrayList();
                    IndexEntity tempIndex = indices.getValue().get(0);
                    for (IndexEntity indexEntity : indices.getValue().subList(1, indices.getValue().size())) {
                        IndexEntity mergedIndex = mergeIndexEntity(tempIndex, indexEntity, availableId);
                        if (CollectionUtils.isNotEmpty(tempIndex.getLayouts())) {
                            readyIndices.add(tempIndex);
                        }
                        if (CollectionUtils.isNotEmpty(indexEntity.getLayouts())) {
                            readyIndices.add(indexEntity);
                        }
                        tempIndex = mergedIndex;
                    }

                    if (CollectionUtils.isNotEmpty(tempIndex.getLayouts())) {
                        reservedIndex.add(tempIndex);
                    }
                    newDim2Index.put(indices.getKey(), reservedIndex);
                });

        return newDim2Index;
    }

    private IndexEntity mergeIndexEntity(IndexEntity originIndex, IndexEntity toBeMergedIndex,
            long availableNextIndexId) {
        Preconditions.checkArgument(originIndex.getDimensionBitset().equals(toBeMergedIndex.getDimensionBitset()),
                "the dimension of Merged IndexPlan MUST be SAME!");
        // if existed ready layout, reserve the ready layouts and create new IndexPlan for newLayout
        val originPair = splitLayoutsToReadyAndNew(originIndex);
        val mergedPair = splitLayoutsToReadyAndNew(toBeMergedIndex);
        originIndex.getLayouts().clear();
        toBeMergedIndex.getLayouts().clear();
        if (CollectionUtils.isNotEmpty(originPair.getFirst())) {
            originIndex.setLayouts(originPair.getFirst());
        }
        if (CollectionUtils.isNotEmpty(mergedPair.getFirst())) {
            toBeMergedIndex.setLayouts(mergedPair.getFirst());
        }

        // merge Layout
        Set<Integer> newMeasures = Sets.newTreeSet(originIndex.getMeasures());
        newMeasures.addAll(toBeMergedIndex.getMeasures());
        IndexEntity mergedIndex = new EntityBuilder.IndexEntityBuilder(availableNextIndexId, originIndex.getIndexPlan())
                .dimIds(originIndex.getDimensions()).measure(Lists.newArrayList(newMeasures)).build();

        val existedList = Lists.newArrayList(originPair.getSecond());
        existedList.addAll(mergedPair.getSecond());
        Map<String, LayoutEntity> dims2LayoutMap = mergeLayoutsOfSameColOrder(existedList);

        for (LayoutEntity layoutEntity : dims2LayoutMap.values()) {
            List<Integer> newLayoutColOrder = new ArrayList<>(layoutEntity.getDimsIds());
            newLayoutColOrder.addAll(mergedIndex.getMeasures());
            List<LayoutEntity> existedLayout = Lists.newArrayList(mergedIndex.getLayouts());
            LayoutEntity newLayout = new EntityBuilder.LayoutEntityBuilder(mergedIndex.searchNextAvailableLayoutId(
                    existedLayout, mergedIndex.getId(), ((int) (mergedIndex.getNextLayoutOffset()))), mergedIndex)
                    .isAuto(layoutEntity.isAuto()).colOrderIds(newLayoutColOrder)
                    .partitionCol(layoutEntity.getPartitionByColumns()).shardByCol(layoutEntity.getShardByColumns())
                    .build();
            newLayout.setInProposing(true);
            mergedIndex.getLayouts().add(newLayout);

            updateAccelerationInfo(proposeContext.getAccelerateInfoMap(),
                    layoutEntity.getIndex().getIndexPlan().getId(), layoutEntity, newLayout);
        }

        return mergedIndex;
    }

    private void mergeProposingIndexToExist(List<IndexEntity> readyIndices,
            HashMap<ImmutableBitSet, List<IndexEntity>> newDim2Index) {
        Map<IndexIdentifier, IndexEntity> readyIndicesMap = readyIndices.stream()
                .collect(Collectors.toMap(IndexEntity::createIndexIdentifier, Function.identity()));

        newDim2Index.forEach(((integers, indexEntities) -> {
            Iterator<IndexEntity> it = indexEntities.iterator();
            while (it.hasNext()) {
                val index = it.next();
                val indexIdentifier = index.createIndexIdentifier();
                if (readyIndicesMap.containsKey(indexIdentifier)) {
                    val readyIndex = readyIndicesMap.get(indexIdentifier);
                    index.getLayouts().forEach(layout -> {
                        layout.initalId(true);
                        readyIndex.addLayout(layout);
                    });
                    it.remove();
                }
            }
        }));
    }

    private void dropDuplicateLayouts(List<IndexEntity> readyIndices,
            HashMap<ImmutableBitSet, List<IndexEntity>> newDim2Index) {
        val existedLayout = readyIndices.stream().map(IndexEntity::getLayouts).flatMap(List::stream)
                .collect(Collectors.toList());

        newDim2Index.forEach((integers, indexEntities) -> {
            Iterator<IndexEntity> it = indexEntities.iterator();
            while (it.hasNext()) {
                val index = it.next();
                Iterator<LayoutEntity> layoutIt = index.getLayouts().iterator();
                while (layoutIt.hasNext()) {
                    val layout = layoutIt.next();
                    int id = existedLayout.indexOf(layout);
                    if (id >= 0) {
                        updateAccelerationInfo(proposeContext.getAccelerateInfoMap(),
                                layout.getIndex().getIndexPlan().getId(), layout, existedLayout.get(id));
                        layoutIt.remove();
                    }
                }

                if (CollectionUtils.isEmpty(index.getLayouts())) {
                    it.remove();
                }
            }
        });
    }

    private Map<ImmutableBitSet, List<IndexEntity>> recognizeAggIndexGroupOfSameDims(IndexPlan indexPlan) {
        // recognize aggIndex group of same dimensions
        val dim2Indices = new HashMap<ImmutableBitSet, List<IndexEntity>>();
        indexPlan.getIndexes().forEach(indexEntity -> {
            if (indexEntity.isTableIndex() || !containNewLayout(indexEntity)) {
                return;
            }

            if (dim2Indices.get(indexEntity.getDimensionBitset()) == null) {
                List<IndexEntity> list = Lists.newArrayList();
                list.add(indexEntity);
                dim2Indices.put(indexEntity.getDimensionBitset(), list);
            } else {
                dim2Indices.get(indexEntity.getDimensionBitset()).add(indexEntity);
            }
        });
        return dim2Indices;
    }

    private List<IndexEntity> collectTableOrReadyIndices(IndexPlan indexPlan) {
        return indexPlan.getIndexes().stream()
                .filter(indexEntity -> indexEntity.isTableIndex() || !containNewLayout(indexEntity))
                .collect(Collectors.toList());
    }

    private Map<String, LayoutEntity> mergeLayoutsOfSameColOrder(List<LayoutEntity> layouts) {
        Map<String, LayoutEntity> dims2LayoutMap = new HashMap<>();
        layouts.forEach(layoutEntity -> {
            String dimStr = getDimsStr(layoutEntity.getDimsIds(), layoutEntity.getShardByColumns());
            if (dims2LayoutMap.get(dimStr) == null) {
                dims2LayoutMap.put(dimStr, layoutEntity);
            } else {
                updateAccelerationInfo(proposeContext.getAccelerateInfoMap(),
                        layoutEntity.getIndex().getIndexPlan().getId(), layoutEntity, dims2LayoutMap.get(dimStr));
            }
        });
        return dims2LayoutMap;
    }

    private String getDimsStr(ImmutableList<Integer> dimIds, List<Integer> shardByIds) {
        StringBuilder builder = new StringBuilder("dim:[");
        for (int i = 0; i < dimIds.size(); i++) {
            builder.append(dimIds.get(i));
            builder.append(i == dimIds.size() - 1 ? "" : ",");
        }
        builder.append("] shardBy:[");
        for (int i = 0; i < shardByIds.size(); i++) {
            builder.append(shardByIds.get(i));
            builder.append(i == dimIds.size() - 1 ? "" : ",");
        }
        builder.append("]");
        return builder.toString();
    }

    private Pair<List<LayoutEntity>, List<LayoutEntity>> splitLayoutsToReadyAndNew(IndexEntity indexEntity) {
        List<LayoutEntity> readyLayouts = Lists.newArrayList();
        List<LayoutEntity> newLayouts = Lists.newArrayList();

        for (LayoutEntity layout : indexEntity.getLayouts()) {
            if (layout.isInProposing()) {
                newLayouts.add(layout);
            } else {
                readyLayouts.add(layout);
            }
        }

        return Pair.newPair(readyLayouts, newLayouts);
    }

    private boolean containNewLayout(IndexEntity index) {
        for (LayoutEntity layout : index.getLayouts()) {
            if (layout.isInProposing())
                return true;
        }
        return false;
    }

    private void updateAccelerationInfo(Map<String, AccelerateInfo> accelerateInfo, String model,
            LayoutEntity originLayout, LayoutEntity newLayout) {
        accelerateInfo.entrySet().stream().filter(acc -> !acc.getValue().isNotSucceed())
                .forEach(entry -> entry.getValue().getRelatedLayouts().stream()
                        .filter(qlr -> qlr.getModelId().equals(model) && qlr.getLayoutId() == originLayout.getId())
                        .forEach(relation -> relation.setLayoutId(newLayout.getId())));

        accelerateInfo.entrySet().stream().filter(acc -> !acc.getValue().isNotSucceed()).forEach(
                entry -> entry.getValue().setRelatedLayouts(Sets.newHashSet(entry.getValue().getRelatedLayouts())));
    }
}
