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

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.cube.cuboid.CuboidScheduler;
import org.apache.kylin.metadata.cube.model.validation.NIndexPlanValidator;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

import lombok.val;

public class NIndexPlanManager {
    private static final Logger logger = LoggerFactory.getLogger(NIndexPlanManager.class);

    public static NIndexPlanManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NIndexPlanManager.class);
    }

    // called by reflection
    static NIndexPlanManager newInstance(KylinConfig config, String project) {
        return new NIndexPlanManager(config, project);
    }

    // ============================================================================

    private KylinConfig config;
    private String project;

    private CachedCrudAssist<IndexPlan> crud;

    private NIndexPlanManager(KylinConfig cfg, final String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing NIndexPlanManager with KylinConfig Id: {} for project {}",
                    System.identityHashCode(cfg), project);
        this.config = cfg;
        this.project = project;
        String resourceRootPath = "/" + project + ResourceStore.INDEX_PLAN_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<IndexPlan>(getStore(), resourceRootPath, IndexPlan.class) {
            @Override
            protected IndexPlan initEntityAfterReload(IndexPlan indexPlan, String resourceName) {
                indexPlan.initAfterReload(config, project);
                return indexPlan;
            }

            @Override
            protected IndexPlan initBrokenEntity(IndexPlan entity, String resourceName) {
                val indexPlan = super.initBrokenEntity(entity, resourceName);
                indexPlan.setProject(project);
                indexPlan.setConfig(KylinConfigExt.createInstance(config, Maps.newHashMap()));
                indexPlan.setDependencies(indexPlan.calcDependencies());

                return indexPlan;
            }

        };
        this.crud.setCheckCopyOnWrite(true);
    }

    public IndexPlan copy(IndexPlan plan) {
        return crud.copyBySerialization(plan);
    }

    public IndexPlan getIndexPlan(String id) {
        if (StringUtils.isEmpty(id)) {
            return null;
        }
        return crud.get(id);
    }

    public IndexPlan getIndexPlanByModelAlias(String name) {
        return listAllIndexPlans(true).stream().filter(indexPlan -> Objects.equals(indexPlan.getModelAlias(), name))
                .findFirst().orElse(null);
    }

    // listAllIndexPlans only get the healthy indexPlans, the broken ones need to be invisible in the auto-suggestion process
    public List<IndexPlan> listAllIndexPlans() {
        return listAllIndexPlans(false);
    }

    // list all indexPlans include broken ones
    public List<IndexPlan> listAllIndexPlans(boolean includeBroken) {
        return crud.listAll().stream().filter(cp -> includeBroken || !cp.isBroken()).collect(Collectors.toList());
    }

    public IndexPlan createIndexPlan(IndexPlan indexPlan) {
        if (indexPlan.getUuid() == null)
            throw new IllegalArgumentException();
        if (crud.contains(indexPlan.getUuid()))
            throw new IllegalArgumentException("IndexPlan '" + indexPlan.getUuid() + "' already exists");

        try {
            // init the cube plan if not yet
            if (indexPlan.getConfig() == null)
                indexPlan.initAfterReload(config, project);
        } catch (Exception e) {
            logger.warn("Broken cube plan " + indexPlan, e);
            indexPlan.addError(e.getMessage());
        }

        // Check base validation
        if (!indexPlan.getError().isEmpty()) {
            throw new IllegalArgumentException(indexPlan.getErrorMsg());
        }
        // Semantic validation
        NIndexPlanValidator validator = new NIndexPlanValidator();
        ValidateContext context = validator.validate(indexPlan);
        if (!context.ifPass()) {
            throw new IllegalArgumentException(indexPlan.getErrorMsg());
        }

        return save(indexPlan);
    }

    public interface NIndexPlanUpdater {
        void modify(IndexPlan copyForWrite);
    }

    public IndexPlan updateIndexPlan(String indexPlanId, NIndexPlanUpdater updater) {
        IndexPlan cached = getIndexPlan(indexPlanId);
        IndexPlan copy = copy(cached);
        updater.modify(copy);
        return updateIndexPlan(copy);
    }

    // use the NIndexPlanUpdater instead
    @Deprecated
    public IndexPlan updateIndexPlan(IndexPlan indexPlan) {
        if (indexPlan.isCachedAndShared())
            throw new IllegalStateException();

        if (indexPlan.getUuid() == null)
            throw new IllegalArgumentException();

        String name = indexPlan.getUuid();
        if (!crud.contains(name))
            throw new IllegalArgumentException("IndexPlan '" + name + "' does not exist.");

        try {
            // init the cube plan if not yet
            if (indexPlan.getConfig() == null)
                indexPlan.initAfterReload(config, project);
        } catch (Exception e) {
            logger.warn("Broken cube desc " + indexPlan, e);
            indexPlan.addError(e.getMessage());
            throw new IllegalArgumentException(indexPlan.getErrorMsg());
        }

        return save(indexPlan);
    }

    // remove indexPlan
    public void dropIndexPlan(IndexPlan indexPlan) {
        crud.delete(indexPlan);
    }

    public void dropIndexPlan(String planId) {
        val indexPlan = getIndexPlan(planId);
        dropIndexPlan(indexPlan);
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    private IndexPlan save(IndexPlan indexPlan) {
        validatePlan(indexPlan);
        indexPlan.setIndexes(indexPlan.getIndexes().stream()
                .peek(cuboid -> cuboid.setLayouts(cuboid.getLayouts().stream()
                        .filter(l -> l.isBase() || l.isAuto() || IndexEntity.isTableIndex(l.getId()))
                        .collect(Collectors.toList())))
                .filter(cuboid -> cuboid.getLayouts().size() > 0).collect(Collectors.toList()));

        val dataflowManager = NDataflowManager.getInstance(config, project);
        val dataflow = dataflowManager.getDataflow(indexPlan.getUuid());
        if (dataflow != null && dataflow.getLatestReadySegment() != null) {
            val livedIds = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
            val layoutIds = new HashSet<Long>();
            for (NDataSegment segment : dataflow.getSegments()) {
                layoutIds.addAll(segment.getLayoutIds());
            }
            layoutIds.removeAll(livedIds);
            dataflowManager.removeLayouts(dataflow, layoutIds);
        }

        return crud.save(indexPlan);
    }

    private void validatePlan(IndexPlan indexPlan) {
        // make sure layout's measures and dimensions are equal to its index
        for (IndexEntity index : indexPlan.getIndexes()) {
            val layouts = index.getLayouts();
            for (LayoutEntity layout : layouts) {
                Preconditions.checkState(
                        CollectionUtils.isEqualCollection(layout.getColOrder().stream()
                                .filter(col -> col >= NDataModel.MEASURE_ID_BASE).collect(Collectors.toSet()),
                                index.getMeasures()),
                        "layout " + layout.getId() + "'s measure is illegal " + layout.getColOrder() + ", "
                                + index.getMeasures());
                Preconditions.checkState(CollectionUtils.isEqualCollection(layout.getColOrder().stream()
                        .filter(col -> col < NDataModel.MEASURE_ID_BASE).collect(Collectors.toSet()),
                        index.getDimensions()), "layout " + layout.getId() + "'s dimension is illegal");
            }
        }

        // validate columns of table index
        Set<Integer> selectedColumnIds = NDataModelManager.getInstance(config, indexPlan.getProject())
                .getDataModelDesc(indexPlan.getUuid()).getAllSelectedColumns().stream()
                .map(NDataModel.NamedColumn::getId).collect(Collectors.toSet());
        for (IndexEntity index : indexPlan.getAllIndexes(false)) {
            if (index.isTableIndex()) {
                for (Integer dimId : index.getDimensions()) {
                    if (!selectedColumnIds.contains(dimId)) {
                        throw new IllegalStateException(
                                String.format(Locale.ROOT, MsgPicker.getMsg().getDimensionNotfound(),
                                        indexPlan.getModel().getNonDimensionNameById(dimId)));
                    }
                }
            }
        }

        // make sure no layouts have same id
        validateSameIdWithDifferentLayout(indexPlan);
        validateDifferentIdWithSameLayout(indexPlan);

        // make sure cube_plan does not have duplicate indexes, duplicate index means two indexes have same dimensions and measures
        val allIndexes = indexPlan.getAllIndexes(false);
        val tableIndexSize = allIndexes.stream().filter(IndexEntity::isTableIndex).map(IndexEntity::getDimensionBitset)
                .distinct().count();
        val aggIndexSize = allIndexes.stream().filter(i -> !i.isTableIndex())
                .map(index -> index.getMeasureBitset().or(index.getDimensionBitset())).distinct().count();
        Preconditions.checkState(tableIndexSize + aggIndexSize == allIndexes.size(),
                "there are duplicate indexes in index_plan");

        if (indexPlan.getRuleBasedIndex() != null) {
            val scheduler = CuboidScheduler.getInstance(indexPlan, indexPlan.getRuleBasedIndex());
            scheduler.updateOrder();
        }
    }

    private void validateSameIdWithDifferentLayout(IndexPlan indexPlan) {
        val seen = Maps.<Long, LayoutEntity> newHashMap();
        val allDistinct = Stream
                .concat(indexPlan.getRuleBaseLayouts().stream(), indexPlan.getWhitelistLayouts().stream())
                .allMatch(layout -> {
                    if (seen.containsKey(layout.getId())) {
                        return Objects.equals(seen.get(layout.getId()), layout);
                    } else {
                        seen.put(layout.getId(), layout);
                        return true;
                    }
                });
        Preconditions.checkState(allDistinct, "there are different layout that have same id");
    }

    private void validateDifferentIdWithSameLayout(IndexPlan indexPlan) {
        val seen = Maps.<LayoutEntity, Long> newHashMap();
        val allDistinct = Stream
                .concat(indexPlan.getRuleBaseLayouts().stream(), indexPlan.getWhitelistLayouts().stream())
                .allMatch(layout -> {
                    if (seen.containsKey(layout)) {
                        return Objects.equals(seen.get(layout), layout.getId());
                    } else {
                        seen.put(layout, layout.getId());
                        return true;
                    }
                });
        Preconditions.checkState(allDistinct, "there are same layout that have different id");
    }

    public long getAvailableIndexesCount(String project, String id) {
        val dataflowManager = NDataflowManager.getInstance(config, project);
        val dataflow = dataflowManager.getDataflow(id);
        if (dataflow == null) {
            return 0;
        }

        val readySegments = dataflow.getLatestReadySegment();

        if (readySegments == null) {
            return 0;
        }

        val readLayouts = readySegments.getLayoutsMap().keySet();
        return dataflow.getIndexPlan().getAllLayoutsReadOnly().stream() //
                .filter(layoutEntityPair -> readLayouts.contains(layoutEntityPair.getLeft().getId())
                        && !layoutEntityPair.getRight())
                .count();
    }
}
