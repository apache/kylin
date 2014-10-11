/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.metadata.model.cube.MeasureDesc;
import com.kylinolap.metadata.model.cube.TblColRef;

/**
 * @author xjiang
 */
public class StorageContext {

    public static final int HARD_THRESHOLD = 4000000;
    public static final int DEFAULT_THRESHOLD = 1000000;

    public enum OrderEnum {
        ASCENDING, DESCENDING
    }

    private String connUrl;
    private int threshold;
    private int limit;
    private boolean hasSort;
    private List<MeasureDesc> sortMeasures;
    private List<OrderEnum> sortOrders;
    private boolean acceptPartialResult;
    private BiMap<TblColRef, String> aliasMap;

    // If cuboid dimensions matches group by exactly, there's no derived or any
    // other form of post aggregation needed.
    // In case of exactAggregation, holistic count distinct can be used and
    // coprocessor is not beneficial.
    private boolean exactAggregation;
    // To hint records shall be returned at most granular level, avoid
    // aggregation (coprocessor) wherever possible.
    private boolean avoidAggregation;
    private Set<TblColRef> mandatoryColumns;

    private long totalScanCount;
    private Collection<Cuboid> cuboids;
    private boolean enableLimit;
    private boolean partialResultReturned;

    public StorageContext() {
        this.threshold = DEFAULT_THRESHOLD;
        this.limit = DEFAULT_THRESHOLD;
        this.totalScanCount = 0;
        this.cuboids = new HashSet<Cuboid>();
        this.aliasMap = HashBiMap.create();
        this.hasSort = false;
        this.sortOrders = new ArrayList<OrderEnum>();
        this.sortMeasures = new ArrayList<MeasureDesc>();

        this.exactAggregation = false;
        this.avoidAggregation = false;
        this.mandatoryColumns = new HashSet<TblColRef>();

        this.enableLimit = false;
        this.acceptPartialResult = false;
        this.partialResultReturned = false;
    }

    public String getConnUrl() {
        return connUrl;
    }

    public void setConnUrl(String connUrl) {
        this.connUrl = connUrl;
    }

    // the name that maps to optiq row
    public String getFieldName(TblColRef col) {
        String name = null;
        if (aliasMap != null) {
            name = aliasMap.get(col);
        }
        if (name == null) {
            name = col.getName();
        }
        return name;
    }

    public int getThreshold() {
        return threshold;
    }

    public void setThreshold(int t) {
        threshold = Math.min(t, HARD_THRESHOLD);
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int l) {
        this.limit = l;
    }

    public void enableLimit() {
        this.enableLimit = true;
    }

    public boolean isLimitEnable() {
        return this.enableLimit;
    }

    public void addAlias(TblColRef column, String alias) {
        this.aliasMap.put(column, alias);
    }

    public BiMap<TblColRef, String> getAliasMap() {
        return aliasMap;
    }

    public void addSort(MeasureDesc measure, OrderEnum order) {
        if (measure != null) {
            sortMeasures.add(measure);
            sortOrders.add(order);
        }
    }

    public void markSort() {
        this.hasSort = true;
    }

    public boolean hasSort() {
        return this.hasSort;
    }

    public void addCuboid(Cuboid c) {
        cuboids.add(c);
    }

    public Collection<Cuboid> getCuboids() {
        return cuboids;
    }

    public long getTotalScanCount() {
        return totalScanCount;
    }

    public void setTotalScanCount(long totalScanCount) {
        this.totalScanCount = totalScanCount;
    }

    public boolean isAcceptPartialResult() {
        return acceptPartialResult;
    }

    public void setAcceptPartialResult(boolean acceptPartialResult) {
        this.acceptPartialResult = acceptPartialResult;
    }

    public boolean isPartialResultReturned() {
        return partialResultReturned;
    }

    public void setPartialResultReturned(boolean partialResultReturned) {
        this.partialResultReturned = partialResultReturned;
    }

    public boolean isExactAggregation() {
        return exactAggregation;
    }

    public void markExactAggregation() {
        this.exactAggregation = true;
    }

    public boolean requireNoPostAggregation() {
        assert cuboids.size() == 1; // all scans must hit the same cuboid to
                                    // avoid dedup overlaps of records
        return exactAggregation // is an exact aggregation from query point of
                                // view
                && cuboids.iterator().next().requirePostAggregation() == false; // and
                                                                                // use
                                                                                // an
                                                                                // exact
                                                                                // cuboid
    }

    public boolean isAvoidAggregation() {
        return avoidAggregation;
    }

    public void markAvoidAggregation() {
        this.avoidAggregation = true;
    }

    public void mandateColumn(TblColRef col) {
        this.mandatoryColumns.add(col);
    }

    public Set<TblColRef> getMandatoryColumns() {
        return this.mandatoryColumns;
    }
}
