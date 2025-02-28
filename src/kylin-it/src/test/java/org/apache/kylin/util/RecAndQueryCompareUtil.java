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

package org.apache.kylin.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.BiMap;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableBiMap;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Ordering;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.rec.common.AccelerateInfo;

import lombok.val;

public class RecAndQueryCompareUtil {

    private RecAndQueryCompareUtil() {
    }

    public static String writeQueryLayoutRelationAsString(KylinConfig kylinConfig, String project,
            Collection<AccelerateInfo.QueryLayoutRelation> relatedLayouts) {
        if (CollectionUtils.isEmpty(relatedLayouts)) {
            return "[ ]";
        }

        // get a stable result
        val orderedLayouts = Lists.newArrayList(relatedLayouts);
        Ordering<AccelerateInfo.QueryLayoutRelation> ordering = Ordering.natural()
                .onResultOf(AccelerateInfo.QueryLayoutRelation::getLayoutId)
                .compound((qlr1, qlr2) -> qlr1.getModelId().compareToIgnoreCase(qlr2.getModelId()));
        orderedLayouts.sort(ordering);

        List<String> list = Lists.newArrayList();
        orderedLayouts.forEach(queryLayoutRelation -> {
            List<String> colOrderNames = findColOrderNames(kylinConfig, project, queryLayoutRelation);
            String tmp = String.format(Locale.ROOT, "{model=%s,layout=%s,colOrderName=[%s]}",
                    queryLayoutRelation.getModelId(), queryLayoutRelation.getLayoutId(),
                    String.join(",", colOrderNames));
            list.add(tmp);
        });

        return "[" + String.join(",", list) + "]";
    }

    private static List<String> findColOrderNames(KylinConfig kylinConfig, String project,
            AccelerateInfo.QueryLayoutRelation queryLayoutRelation) {
        List<String> colOrderNames = Lists.newArrayList();

        final IndexPlan indexPlan = NIndexPlanManager.getInstance(kylinConfig, project)
                .getIndexPlan(queryLayoutRelation.getModelId());
        Preconditions.checkNotNull(indexPlan);
        final LayoutEntity cuboidLayout = indexPlan.getLayoutEntity(queryLayoutRelation.getLayoutId());
        final ImmutableList<Integer> colOrder = cuboidLayout.getColOrder();
        final BiMap<Integer, TblColRef> effectiveDimCols = cuboidLayout.getIndex().getEffectiveDimCols();
        final ImmutableBiMap<Integer, NDataModel.Measure> effectiveMeasures = cuboidLayout.getIndex()
                .getEffectiveMeasures();
        colOrder.forEach(column -> {
            if (column < NDataModel.MEASURE_ID_BASE) {
                colOrderNames.add(effectiveDimCols.get(column).getName());
            } else {
                colOrderNames.add(effectiveMeasures.get(column).getName());
            }
        });
        return colOrderNames;
    }

    /**
     * compute the compare level of propose result and query result
     */
    public static void computeCompareRank(KylinConfig kylinConfig, String project,
            Map<String, ExecAndCompExt.CompareEntity> compareEntityMap) {

        compareEntityMap.forEach((sql, entity) -> {
            if (entity.getLevel() == AccelerationMatchedLevel.FAILED_QUERY) {
                return;
            }

            final Collection<OlapContext> olapContexts = entity.getOlapContexts();
            Set<AccelerateInfo.QueryLayoutRelation> layouts = Sets.newHashSet();
            Set<Long> cuboidIds = Sets.newHashSet();
            Set<String> modelIds = Sets.newHashSet();

            olapContexts.forEach(olapContext -> {
                if (olapContext.getAllTableScans().isEmpty()) {
                    entity.setLevel(AccelerationMatchedLevel.SIMPLE_QUERY);
                    return;
                }
                if (olapContext.isConstantQuery()) {
                    entity.setLevel(AccelerationMatchedLevel.CONSTANT_QUERY);
                    return;
                }
                if (olapContext.getStorageContext().getLookupCandidate() != null) {
                    entity.setLevel(AccelerationMatchedLevel.SNAPSHOT_QUERY);
                    return;
                }

                final LayoutEntity cuboidLayout = olapContext.getStorageContext().getBatchCandidate().getLayoutEntity();
                final String modelId = cuboidLayout.getModel().getUuid();
                final long layoutId = cuboidLayout.getId();
                final int semanticVersion = cuboidLayout.getModel().getSemanticVersion();
                AccelerateInfo.QueryLayoutRelation relation = new AccelerateInfo.QueryLayoutRelation(sql, modelId,
                        layoutId, semanticVersion);
                layouts.add(relation);
                cuboidIds.add(cuboidLayout.getIndex().getId());
                modelIds.add(modelId);
            });
            entity.setQueryUsedLayouts(writeQueryLayoutRelationAsString(kylinConfig, project, layouts));

            if (entity.ignoredCompareLevel()) {
                return;
            } else if (Objects.equals(entity.getAccelerateInfo().getRelatedLayouts(), layouts)) {
                entity.setLevel(AccelerationMatchedLevel.ALL_MATCH);
                return;
            }

            final Set<AccelerateInfo.QueryLayoutRelation> relatedLayouts = entity.getAccelerateInfo()
                    .getRelatedLayouts();
            Set<String> proposedModelIds = Sets.newHashSet();
            Set<Long> proposedCuboidIds = Sets.newHashSet();
            relatedLayouts.forEach(layout -> {
                proposedModelIds.add(layout.getModelId());
                proposedCuboidIds.add(layout.getLayoutId() - layout.getLayoutId() % IndexEntity.INDEX_ID_STEP);
            });

            if (Objects.equals(cuboidIds, proposedCuboidIds)) {
                entity.setLevel(AccelerationMatchedLevel.LAYOUT_NOT_MATCH);
            } else if (Objects.equals(modelIds, proposedModelIds)) {
                entity.setLevel(AccelerationMatchedLevel.INDEX_NOT_MATCH);
            } else if (entity.getAccelerateInfo().isFailed()) {
                entity.setLevel(AccelerationMatchedLevel.BLOCKED_QUERY);
            } else {
                entity.setLevel(AccelerationMatchedLevel.MODEL_NOT_MATCH);
            }

        });
    }

    /**
     * summarize rank info
     */
    public static Map<AccelerationMatchedLevel, AtomicInteger> summarizeRankInfo(
            Map<String, ExecAndCompExt.CompareEntity> map) {
        Map<AccelerationMatchedLevel, AtomicInteger> compareResult = Maps.newLinkedHashMap();
        Arrays.stream(AccelerationMatchedLevel.values())
                .forEach(level -> compareResult.putIfAbsent(level, new AtomicInteger()));
        map.values().stream().map(ExecAndCompExt.CompareEntity::getLevel).map(compareResult::get)
                .forEach(AtomicInteger::incrementAndGet);
        return compareResult;
    }

    /**
     * Acceleration matched level
     */
    public enum AccelerationMatchedLevel {

        /**
         * simple query does not need realization
         */
        SIMPLE_QUERY,

        /**
         * constant query
         */
        CONSTANT_QUERY,

        /**
         * query blocked in stage of propose cuboids and layouts
         */
        BLOCKED_QUERY,

        /**
         * failed in matching realizations
         */
        FAILED_QUERY,

        /**
         * query used snapshot or partly used snapshot
         */
        SNAPSHOT_QUERY,

        /**
         * all matched
         */
        ALL_MATCH,

        /**
         * index matched, but layout not matched
         */
        LAYOUT_NOT_MATCH,

        /**
         * model matched, but index not matched
         */
        INDEX_NOT_MATCH,

        /**
         * model not matched
         */
        MODEL_NOT_MATCH
    }
}
