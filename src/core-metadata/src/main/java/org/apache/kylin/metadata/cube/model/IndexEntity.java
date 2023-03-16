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

import static org.apache.kylin.metadata.model.NDataModel.Measure;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.BitSets;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.NDataModel;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.guava30.shaded.common.collect.BiMap;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableBiMap;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class IndexEntity implements Serializable {
    /**
     * Here suppose cuboid's number is not bigger than 1_000_000, so if the id is bigger than 1_000_000 * 1_000
     * means it should be a table index cuboid.
     */
    public static final long TABLE_INDEX_START_ID = 20_000_000_000L;
    public static final long INDEX_ID_STEP = 10000L;

    @JsonBackReference
    private IndexPlan indexPlan;

    @EqualsAndHashCode.Include
    @JsonProperty("id")
    private long id;

    @EqualsAndHashCode.Include
    @JsonProperty("dimensions")
    private List<Integer> dimensions = Lists.newArrayList();

    @EqualsAndHashCode.Include
    @JsonProperty("measures")
    private List<Integer> measures = Lists.newArrayList();

    @EqualsAndHashCode.Include
    @JsonManagedReference
    @JsonProperty("layouts")
    private List<LayoutEntity> layouts = Lists.newArrayList();

    @Setter
    @Getter
    @JsonProperty("next_layout_offset")
    private long nextLayoutOffset = 1;

    // computed fields below
    @Getter(lazy = true)
    private final BiMap<Integer, TblColRef> effectiveDimCols = initEffectiveDimCols();

    public static IndexEntity from(LayoutEntity layout) {
        IndexEntity index = new IndexEntity();
        index.setDimensions(layout.getDimsIds());
        index.setMeasures(layout.getMeasureIds());
        index.setLayouts(Lists.newArrayList(layout));
        index.setId(layout.getIndexId());
        layout.setIndex(index);
        return index;
    }

    private BiMap<Integer, TblColRef> initEffectiveDimCols() {
        return Maps.filterKeys(getModel().getEffectiveCols(),
                input -> input != null && getDimensionBitset().get(input));
    }

    @Getter(lazy = true)
    private final ImmutableBiMap<Integer, Measure> effectiveMeasures = initEffectiveMeasures();

    private ImmutableBiMap<Integer, Measure> initEffectiveMeasures() {
        val model = getModel();
        ImmutableBiMap.Builder<Integer, Measure> measuresBuilder = ImmutableBiMap.builder();
        for (int m : measures) {
            if (model.getEffectiveMeasures().containsKey(m)) {
                measuresBuilder.put(m, model.getEffectiveMeasures().get(m));
            }
        }
        return measuresBuilder.build();
    }

    // BitSet is heavily memory cost, MUST NOT save its value for it easily causes OOM
    public ImmutableBitSet getDimensionBitset() {
        return ImmutableBitSet.valueOf(dimensions);
    }

    public ImmutableBitSet getMeasureBitset() {
        return ImmutableBitSet.valueOf(measures);
    }

    @Getter(lazy = true)
    private final ImmutableSet<TblColRef> dimensionSet = initDimensionSet();

    private ImmutableSet<TblColRef> initDimensionSet() {
        return ImmutableSet.copyOf(getEffectiveDimCols().values());
    }

    @Getter(lazy = true)
    private final ImmutableSet<Measure> measureSet = initMeasureSet();

    private ImmutableSet<Measure> initMeasureSet() {
        return getEffectiveMeasures().values();
    }

    public boolean dimensionsDerive(Collection<Integer> columnIds) {
        if (CollectionUtils.isEmpty(columnIds)) {
            return true;
        }
        for (int fk : columnIds) {
            if (!getEffectiveDimCols().containsKey(fk)) {
                return false;
            }
        }
        return true;
    }

    public boolean dimensionsDerive(Integer... columnIds) {
        for (int fk : columnIds) {
            if (!getEffectiveDimCols().containsKey(fk)) {
                return false;
            }
        }
        return true;
    }

    public boolean fullyDerive(IndexEntity child) {
        // both table index or not.
        if (!this.isTableIndex() == child.isTableIndex()) {
            return false;
        }

        if (totalFieldSize(child) >= totalFieldSize(this)) {
            return false;
        }

        return child.getDimensionBitset().andNot(getDimensionBitset()).isEmpty()
                && child.getMeasureBitset().andNot(getMeasureBitset()).isEmpty();
    }

    private int totalFieldSize(IndexEntity entity) {
        return entity.getDimensions().size() + entity.getMeasures().size();
    }

    public LayoutEntity getLastLayout() {
        List<LayoutEntity> existing = getLayouts();
        if (existing.isEmpty()) {
            return null;
        } else {
            return existing.get(existing.size() - 1);
        }
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public IndexPlan getIndexPlan() {
        return indexPlan;
    }

    public NDataModel getModel() {
        return indexPlan.getModel();
    }

    public void setIndexPlan(IndexPlan indexPlan) {
        checkIsNotCachedAndShared();
        this.indexPlan = indexPlan;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        checkIsNotCachedAndShared();
        this.id = id;
    }

    /**
     * If there is no need to consider the order of dimensions,
     * please use getDimensionBitset() instead of this method.
     */
    public List<Integer> getDimensions() {
        return getColIds(dimensions);
    }

    public void setDimensions(List<Integer> dimensions) {
        checkIsNotCachedAndShared();
        this.dimensions = dimensions;
    }

    /**
     * If there is no need to consider the order of measures,
     * please use getDimensionBitset() instead of this method.
     */
    public List<Integer> getMeasures() {
        return getColIds(measures);
    }

    public void setMeasures(List<Integer> measures) {
        checkIsNotCachedAndShared();
        this.measures = measures;
    }

    private List<Integer> getColIds(List<Integer> cols) {
        return isCachedAndShared() ? Lists.newArrayList(cols) : cols;
    }

    public List<LayoutEntity> getLayouts() {
        return isCachedAndShared() ? ImmutableList.copyOf(layouts) : layouts;
    }

    public LayoutEntity getLayout(long layoutId) {
        if (layoutId < id || layoutId >= id + INDEX_ID_STEP) {
            return null;
        }
        return getLayouts().stream().filter(l -> l.getId() == layoutId).findFirst().orElse(null);
    }

    public void setLayouts(List<LayoutEntity> layouts) {
        checkIsNotCachedAndShared();
        this.layouts = layouts;
    }

    public boolean isCachedAndShared() {
        return indexPlan != null && indexPlan.isCachedAndShared();
    }

    public void checkIsNotCachedAndShared() {
        if (indexPlan != null)
            indexPlan.checkIsNotCachedAndShared();
    }

    public boolean isTableIndex() {
        return isTableIndex(id);
    }

    public static boolean isTableIndex(long id) {
        return id >= TABLE_INDEX_START_ID;
    }

    public static boolean isAggIndex(long id) {
        return !isTableIndex(id);
    }

    public void removeLayouts(List<LayoutEntity> deprecatedLayouts, Predicate<LayoutEntity> isSkip, boolean deleteAuto,
            boolean deleteManual) {
        checkIsNotCachedAndShared();
        List<LayoutEntity> toRemoveLayouts = Lists.newArrayList();
        for (LayoutEntity cuboidLayout : deprecatedLayouts) {
            if (isSkip != null && isSkip.test(cuboidLayout)) {
                continue;
            }
            LayoutEntity toRemoveLayout = getLayouts().stream()
                    .filter(originLayout -> Objects.equals(originLayout, cuboidLayout)).findFirst().orElse(null);
            if (toRemoveLayout != null) {
                if (deleteAuto) {
                    toRemoveLayout.setAuto(false);
                }
                if (deleteManual) {
                    toRemoveLayout.setManual(false);
                }
                if (toRemoveLayout.isExpired()) {
                    toRemoveLayouts.add(toRemoveLayout);
                }
            }
        }
        getLayouts().removeAll(toRemoveLayouts);
    }

    public long searchNextAvailableLayoutId() {
        return searchNextAvailableLayoutId(getLayouts(), getId(), ((int) (getNextLayoutOffset())));
    }

    public long searchNextAvailableLayoutId(List<LayoutEntity> existedLayouts, long baseId, int defaultOffset) {
        HashSet<Long> existedId = new HashSet<>();
        existedLayouts.forEach(l -> existedId.add(l.getId()));

        long candidateId = baseId + defaultOffset;
        while (existedId.contains(candidateId)) {
            candidateId++;
        }
        return candidateId;
    }

    // ============================================================================
    // IndexIdentifier used for auto-modeling
    // ============================================================================

    @Override
    public String toString() {
        return "IndexEntity{ Id=" + id + ", dimBitSet=" + getDimensionBitset() + ", measureBitSet=" + getMeasureBitset()
                + "}.";
    }

    public void assignId(long id) {
        this.id = id;
        int inc = 1;
        for (LayoutEntity layout : layouts) {
            layout.setId(id + inc);
            inc++;
        }
    }

    public void addLayout(LayoutEntity layout) {
        if (layout.notAssignId()) {
            layout.setId(this.getId() + this.getNextLayoutOffset());
            this.setNextLayoutOffset((this.getNextLayoutOffset() + 1) % IndexEntity.INDEX_ID_STEP);
        }
        layout.setIndex(this);
        this.getLayouts().add(layout);
    }

    public enum Status {
        NO_BUILD, BUILDING, LOCKED, ONLINE
    }

    public enum Source {
        RECOMMENDED_AGG_INDEX, RECOMMENDED_TABLE_INDEX, CUSTOM_AGG_INDEX, CUSTOM_TABLE_INDEX, BASE_AGG_INDEX, BASE_TABLE_INDEX
    }

    public enum Range {
        BATCH, STREAMING, HYBRID, EMPTY
    }

    public static class IndexIdentifier {
        int[] dims;
        int[] measures;
        boolean isTableIndex;

        @Getter(value = AccessLevel.PRIVATE, lazy = true)
        private final BitSet dimBitSet = BitSets.valueOf(dims);
        @Getter(value = AccessLevel.PRIVATE, lazy = true)
        private final BitSet measureBitSet = BitSets.valueOf(measures);

        public IndexIdentifier(List<Integer> dimBitSet, List<Integer> measureBitSet, boolean isTableIndex) {
            this(dimBitSet, measureBitSet);
            this.isTableIndex = isTableIndex;
        }

        IndexIdentifier(List<Integer> dimBitSet, List<Integer> measureBitSet) {
            this.dims = dimBitSet.stream().mapToInt(d -> d).toArray();
            this.measures = measureBitSet.stream().mapToInt(d -> d).toArray();
        }

        @Override
        public String toString() {
            return "IndexEntity{" + "dim=" + Arrays.toString(dims) + ", measure=" + Arrays.toString(measures)
                    + ", isTableIndex=" + isTableIndex + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            IndexIdentifier that = (IndexIdentifier) o;
            return isTableIndex == that.isTableIndex && Objects.equals(getDimBitSet(), that.getDimBitSet())
                    && Objects.equals(getMeasureBitSet(), that.getMeasureBitSet());
        }

        @Override
        public int hashCode() {
            // the hash code of primary array with same elements does not equal
            return Objects.hash(getDimBitSet(), getMeasureBitSet(), isTableIndex);
        }
    }

    public IndexIdentifier createIndexIdentifier() {
        // for we should not change the order dimensions and measures of this index, therefore copy and then sort
        List<Integer> dimensions = Lists.newArrayList(getDimensions());
        List<Integer> measures = Lists.newArrayList(getMeasures());
        dimensions.sort(Integer::compareTo);
        measures.sort(Integer::compareTo);
        return new IndexIdentifier(dimensions, measures, isTableIndex());
    }

}
