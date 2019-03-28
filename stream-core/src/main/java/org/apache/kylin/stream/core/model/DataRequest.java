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

package org.apache.kylin.stream.core.model;

import java.util.List;
import java.util.Set;

import org.apache.kylin.metadata.model.FunctionDesc;

public class DataRequest {
    private String cubeName;
    private String queryId;
    private long minSegmentTime;
    private String tupleFilter; // Base64 encoded serialized TupleFilter
    private String havingFilter;
    private Set<String> dimensions; // what contains in Pair is <tableName, columnName>
    private Set<String> groups;
    private List<FunctionDesc> metrics;
    private int storagePushDownLimit = Integer.MAX_VALUE;
    private boolean allowStorageAggregation;

    private long requestSendTime;
    private boolean enableDetailProfile;
    private String storageBehavior;

    public String getCubeName() {
        return cubeName;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    public long getMinSegmentTime() {
        return minSegmentTime;
    }

    public void setMinSegmentTime(long minSegmentTime) {
        this.minSegmentTime = minSegmentTime;
    }

    public String getTupleFilter() {
        return tupleFilter;
    }

    public void setTupleFilter(String tupleFilter) {
        this.tupleFilter = tupleFilter;
    }

    public Set<String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(Set<String> dimensions) {
        this.dimensions = dimensions;
    }

    public Set<String> getGroups() {
        return groups;
    }

    public void setGroups(Set<String> groups) {
        this.groups = groups;
    }

    public List<FunctionDesc> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<FunctionDesc> metrics) {
        this.metrics = metrics;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public int getStoragePushDownLimit() {
        return storagePushDownLimit;
    }

    public void setStoragePushDownLimit(int storagePushDownLimit) {
        this.storagePushDownLimit = storagePushDownLimit;
    }

    public boolean isAllowStorageAggregation() {
        return allowStorageAggregation;
    }

    public void setAllowStorageAggregation(boolean allowStorageAggregation) {
        this.allowStorageAggregation = allowStorageAggregation;
    }

    public boolean isEnableDetailProfile() {
        return enableDetailProfile;
    }

    public void setEnableDetailProfile(boolean enableDetailProfile) {
        this.enableDetailProfile = enableDetailProfile;
    }

    public long getRequestSendTime() {
        return requestSendTime;
    }

    public void setRequestSendTime(long requestSendTime) {
        this.requestSendTime = requestSendTime;
    }

    public String getStorageBehavior() {
        return storageBehavior;
    }

    public void setStorageBehavior(String storageBehavior) {
        this.storageBehavior = storageBehavior;
    }

    public String getHavingFilter() {
        return havingFilter;
    }

    public void setHavingFilter(String havingFilter) {
        this.havingFilter = havingFilter;
    }
}
