/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.metadata.cube.model;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.kylin.common.util.ImmutableBitSet;

import java.util.List;

public class IndexEntity {
    /**
     * Here suppose cuboid's number is not bigger than 1_000_000, so if the id is bigger than 1_000_000 * 1_000
     * means it should be a table index cuboid.
     */
    public static final long TABLE_INDEX_START_ID = 20_000_000_000L;
    public static final long INDEX_ID_STEP = 10000L;

    @JsonBackReference
    private Cube cube;

    @JsonProperty("id")
    private long id;

    @JsonProperty("dimensions")
    private List<Integer> dimensions = Lists.newArrayList();

    @JsonProperty("measures")
    private List<Integer> measures = Lists.newArrayList();

    @JsonProperty("layouts")
    private List<LayoutEntity> layouts = Lists.newArrayList();

    private final ImmutableBitSet dimensionBitset = initDimensionBitset();

    private final ImmutableBiMap<Integer, MeasureDesc> effectiveMeasures = initEffectiveMeasures();

    public ImmutableBiMap<Integer, MeasureDesc> getEffectiveMeasures() {
        return effectiveMeasures;
    }

    private ImmutableBiMap<Integer, MeasureDesc> initEffectiveMeasures() {
        ImmutableBiMap.Builder<Integer, MeasureDesc> measuresBuilder = ImmutableBiMap.builder();
        for (int m : measures) {
            if (this.cube.getDataModel().getEffectiveMeasureMap().containsKey(m)) {
                measuresBuilder.put(m, this.cube.getDataModel().getEffectiveMeasureMap().get(m));
            }
        }
        return measuresBuilder.build();
    }

    private ImmutableBitSet initDimensionBitset() {
        return ImmutableBitSet.valueOf(dimensions);
    }

    private final ImmutableBitSet measureBitset = initMeasureBitset();

    private ImmutableBitSet initMeasureBitset() {
        return ImmutableBitSet.valueOf(measures);
    }

    public void checkIsNotCachedAndShared() {
        if (cube != null)
            cube.checkIsNotCachedAndShared();
    }

    public boolean isCachedAndShared() {
        return cube != null && cube.isCachedAndShared();
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

    public void setLayouts(List<LayoutEntity> layouts) {
        checkIsNotCachedAndShared();
        this.layouts = layouts;
    }

    public Cube getCube() {
        return cube;
    }

    public void setCube(Cube cube) {
        this.cube = cube;
    }

    public ImmutableBitSet getDimensionBitset() {
        return dimensionBitset;
    }

    public ImmutableBitSet getMeasureBitset() {
        return measureBitset;
    }

    public boolean isTableIndex() {
        return id >= TABLE_INDEX_START_ID;
    }

    private int totalFieldSize(IndexEntity entity) {
        return entity.getDimensions().size() + entity.getMeasures().size();
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
}
