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

package org.apache.kylin.storage;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

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

    private boolean exactAggregation;
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

        this.exactAggregation = false;
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

    public void setExactAggregation(boolean isExactAggregation) {
        this.exactAggregation = isExactAggregation;
    }

    public boolean isExactAggregation() {
        return this.exactAggregation;
    }

    public void enableCoprocessor() {
        this.enableCoprocessor = true;
    }

    public boolean isCoprocessorEnabled() {
        return this.enableCoprocessor;
    }

}
