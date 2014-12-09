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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.cube.model.MeasureDesc;
import com.kylinolap.metadata.model.realization.TblColRef;

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

    // To hint records shall be returned at most granular level, avoid aggregation (coprocessor) wherever possible.
    private boolean avoidAggregation;
    private boolean exactAggregation;
    private Set<TblColRef> otherMandatoryColumns;
    private boolean enableLimit;
    private boolean enableCoprocessor;

    private long totalScanCount;
    private Cuboid cuboid;
    private boolean partialResultReturned;

    public StorageContext() {
        this.threshold = DEFAULT_THRESHOLD;
        this.limit = DEFAULT_THRESHOLD;
        this.totalScanCount = 0;
        this.cuboid = null;
        this.aliasMap = HashBiMap.create();
        this.hasSort = false;
        this.sortOrders = new ArrayList<OrderEnum>();
        this.sortMeasures = new ArrayList<MeasureDesc>();

        this.avoidAggregation = false;
        this.exactAggregation = false;
        this.otherMandatoryColumns = new HashSet<TblColRef>();
        this.enableLimit = false;
        this.enableCoprocessor = false;

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

    public boolean isLimitEnabled() {
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

    public void setCuboid(Cuboid c) {
        cuboid = c;
    }

    public Cuboid getCuboid() {
        return cuboid;
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

    public boolean isAvoidAggregation() {
        return avoidAggregation;
    }

    public void markAvoidAggregation() {
        this.avoidAggregation = true;
    }

    public void setExactAggregation(boolean isExactAggregation) {
        this.exactAggregation = isExactAggregation;
    }
    
    public boolean isExactAggregation() {
        return this.exactAggregation;
    }
    
    public void addOtherMandatoryColumns(TblColRef col) {
        this.otherMandatoryColumns.add(col);
    }
    
    public Set<TblColRef> getOtherMandatoryColumns() {
        return this.otherMandatoryColumns;
    }
    
    public void enableCoprocessor() {
        this.enableCoprocessor = true;
    }
    
    public boolean isCoprocessorEnabled() {
        return this.enableCoprocessor;
    }

}
