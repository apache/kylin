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
package org.apache.kylin.metadata.cube.cuboid;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.AntiFlatChecker;
import org.apache.kylin.metadata.model.ColExcludedChecker;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.util.scd2.SCD2NonEquiCondSimplification;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableCollection;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMultimap;
import org.apache.kylin.guava30.shaded.common.collect.Iterables;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class IndexMatcher {

    final SQLDigest sqlDigest;
    final String project;
    final NDataflow dataflow;
    final NDataModel model;
    final boolean isBatchFusionModel;
    @Getter
    boolean valid;

    final ChooserContext chooserContext;

    final Map<TblColRef, Integer> tblColMap;
    final Map<String, List<Integer>> primaryKeyColumnIds;
    final Map<String, List<Integer>> foreignKeyColumnIds;
    final ImmutableMultimap<Integer, Integer> fk2Pk;
    Set<Integer> sqlColumns;

    ColExcludedChecker excludedChecker;
    AntiFlatChecker antiFlatChecker;
    final Map<Integer, DeriveInfo> toManyDerivedInfoMap = Maps.newHashMap();

    IndexMatcher(SQLDigest sqlDigest, ChooserContext chooserContext, NDataflow dataflow,
            ColExcludedChecker excludedChecker, AntiFlatChecker antiFlatChecker) {
        this.sqlDigest = sqlDigest;
        this.dataflow = dataflow;
        this.project = dataflow.getProject();
        this.model = chooserContext.getModel();
        this.chooserContext = chooserContext;
        this.isBatchFusionModel = chooserContext.isBatchFusionModel();

        this.fk2Pk = chooserContext.getFk2Pk();
        this.tblColMap = chooserContext.getTblColMap();
        this.primaryKeyColumnIds = chooserContext.getPrimaryKeyColumnIds();
        this.foreignKeyColumnIds = chooserContext.getForeignKeyColumnIds();

        this.excludedChecker = excludedChecker;
        this.antiFlatChecker = antiFlatChecker;

        // suppose: A join B && A join C, the relation of A->C is TO_MANY and C need to derive,
        // then the built index of this join relation only based on the flat table of A join B,
        // in order to get the correct result, the query result must join the snapshot of C.
        model.getJoinTables().forEach(joinTableDesc -> {
            if (antiFlatChecker.getAntiFlattenLookups().contains(joinTableDesc.getTable())) {
                JoinDesc join = joinTableDesc.getJoin();
                if (!joinTableDesc.isToManyJoinRelation() || !needJoinSnapshot(join)) {
                    return;
                }
                int foreignKeyId = foreignKeyColumnIds.get(joinTableDesc.getAlias()).get(0);
                int primaryKeyId = primaryKeyColumnIds.get(joinTableDesc.getAlias()).get(0);
                toManyDerivedInfoMap.put(primaryKeyId,
                        new DeriveInfo(DeriveInfo.DeriveType.LOOKUP, join, Lists.newArrayList(foreignKeyId), false));
            }
        });
    }

    //the integrity check is passed
    protected abstract boolean fastValidCheckBeforeMatch();

    abstract MatchResult match(LayoutEntity layout);

    protected abstract boolean canSkipIndexMatch(IndexEntity index);

    Set<Integer> initUnmatchedColumnIds(LayoutEntity layout) {
        Set<Integer> unmatchedCols = Sets.newHashSet();
        unmatchedCols.addAll(sqlColumns);
        if (isBatchFusionModel) {
            unmatchedCols.removeAll(layout.getStreamingColumns().keySet());
        }
        unmatchedCols.removeAll(layout.getOrderedDimensions().keySet());
        Set<Integer> excludedColSet = filterExcludedDims(layout);
        if (!excludedColSet.isEmpty()) {
            log.debug("Excluded columns of layout need to derive. The id set is: {}", excludedColSet);
            unmatchedCols.addAll(excludedColSet);
        }
        return unmatchedCols;
    }

    Set<Integer> filterExcludedDims(LayoutEntity layout) {
        if (!NProjectManager.getProjectConfig(project).isSnapshotPreferred()) {
            return Sets.newHashSet();
        }
        return layout.getOrderedDimensions().entrySet().stream() //
                .filter(entry -> excludedChecker.isExcludedCol(entry.getValue())) //
                .map(Map.Entry::getKey).collect(Collectors.toSet());
    }

    void goThruDerivedDims(final IndexEntity indexEntity, Map<Integer, DeriveInfo> needDeriveCollector,
            Set<Integer> unmatchedDims) {
        Iterator<Integer> unmatchedDimItr = unmatchedDims.iterator();
        while (unmatchedDimItr.hasNext()) {
            Integer unmatchedDim = unmatchedDimItr.next();
            if (model.isLookupTable(unmatchedDim) && model.isQueryDerivedEnabled(unmatchedDim) //
                    && goThruDerivedDimsFromLookupTable(indexEntity, needDeriveCollector, unmatchedDimItr,
                            unmatchedDim)) {
                continue;
            }

            // in some rare cases, FK needs to be derived from PK
            goThruDerivedDimsFromFactTable(indexEntity, needDeriveCollector, unmatchedDimItr, unmatchedDim);

        }

        needDeriveCollector.putAll(toManyDerivedInfoMap);
    }

    private boolean needJoinSnapshot(JoinDesc join) {
        List<JoinDesc> sqlDigestJoins = sqlDigest.joinDescs == null ? Lists.newArrayList() : sqlDigest.joinDescs;
        for (JoinDesc digestJoin : sqlDigestJoins) {
            Set<TblColRef> digestPKs = Sets.newHashSet(digestJoin.getPrimaryKeyColumns());
            Set<TblColRef> digestFKs = Sets.newHashSet(digestJoin.getForeignKeyColumns());
            Set<TblColRef> joinPKs = Sets.newHashSet(join.getPrimaryKeyColumns());
            Set<TblColRef> joinFKs = Sets.newHashSet(join.getForeignKeyColumns());
            if (!CollectionUtils.isEmpty(digestFKs) && !CollectionUtils.isEmpty(digestPKs)
                    && !CollectionUtils.isEmpty(joinFKs) && !CollectionUtils.isEmpty(joinPKs)
                    && digestFKs.containsAll(joinFKs) && digestPKs.containsAll(joinPKs)
                    && joinFKs.containsAll(digestFKs) && joinPKs.containsAll(digestPKs)) {
                return true;
            }
        }
        return false;
    }

    private void goThruDerivedDimsFromFactTable(IndexEntity indexEntity, Map<Integer, DeriveInfo> needDeriveCollector,
            Iterator<Integer> unmatchedDimItr, Integer unmatchedDim) {
        ImmutableCollection<Integer> pks = fk2Pk.get(unmatchedDim);
        Iterable<Integer> pksOnIndex = Iterables.filter(pks, indexEntity::dimensionsDerive);
        Integer pk = Iterables.getFirst(pksOnIndex, null);
        if (pk != null) {
            JoinDesc joinByPKSide = model.getJoinByPKSide(pk);
            Preconditions.checkNotNull(joinByPKSide);

            //cannot derived fk from pk when left join
            if (!joinByPKSide.isInnerJoin()) {
                return;
            }
            needDeriveCollector.put(unmatchedDim,
                    new DeriveInfo(DeriveInfo.DeriveType.PK_FK, joinByPKSide, Lists.newArrayList(pk), true));
            unmatchedDimItr.remove();
        }
    }

    private boolean goThruDerivedDimsFromLookupTable(IndexEntity indexEntity,
            Map<Integer, DeriveInfo> needDeriveCollector, Iterator<Integer> unmatchedDimItr, Integer unmatchedDim) {
        JoinDesc joinByPKSide = model.getJoinByPKSide(unmatchedDim);
        Preconditions.checkNotNull(joinByPKSide);
        String alias = joinByPKSide.getPKSide().getAlias();
        List<Integer> foreignKeyColumns = foreignKeyColumnIds.get(alias);
        List<Integer> primaryKeyColumns = primaryKeyColumnIds.get(alias);

        if (joinByPKSide.isInnerJoin() && primaryKeyColumns.contains(unmatchedDim)) {
            Integer relatedCol = foreignKeyColumns.get(primaryKeyColumns.indexOf(unmatchedDim));
            if (indexEntity.dimensionsDerive(relatedCol)) {
                needDeriveCollector.put(unmatchedDim, new DeriveInfo(DeriveInfo.DeriveType.PK_FK, joinByPKSide,
                        Lists.newArrayList(relatedCol), true));
                unmatchedDimItr.remove();
                return true;
            }
        } else if (indexEntity.dimensionsDerive(foreignKeyColumns) //
                && model.getColRef(unmatchedDim) != null //
                && Optional.ofNullable(model.getAliasMap().get(alias))
                        .map(ref -> ref.getTableDesc().getLastSnapshotPath()).filter(StringUtils::isNotBlank)
                        .isPresent()) {
            DeriveInfo.DeriveType deriveType = matchNonEquiJoinFks(indexEntity, joinByPKSide)
                    ? DeriveInfo.DeriveType.LOOKUP_NON_EQUI
                    : DeriveInfo.DeriveType.LOOKUP;
            needDeriveCollector.put(unmatchedDim, new DeriveInfo(deriveType, joinByPKSide, foreignKeyColumns, false));
            unmatchedDimItr.remove();
            return true;
        }
        return false;
    }

    private boolean matchNonEquiJoinFks(final IndexEntity indexEntity, final JoinDesc joinDesc) {
        return joinDesc.isNonEquiJoin() && indexEntity.dimensionsDerive(
                Stream.of(SCD2NonEquiCondSimplification.INSTANCE.extractFksFromNonEquiJoinDesc(joinDesc))
                        .map(tblColMap::get).collect(Collectors.toList()));
    }

    @Getter
    @AllArgsConstructor
    @RequiredArgsConstructor
    public static class MatchResult {

        boolean isMatched;

        Map<Integer, DeriveInfo> needDerive = Maps.newHashMap();

        CapabilityResult.IncapableCause cases;

        public List<CapabilityResult.CapabilityInfluence> influences = Lists.newArrayList();

        public MatchResult(boolean isMatched, Map<Integer, DeriveInfo> needDerive) {
            this.isMatched = isMatched;
            this.needDerive = needDerive;
        }
    }
}
