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

package org.apache.kylin.storage.gtrecord;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.expression.TupleExpression;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.DynamicFunctionDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;

@SuppressWarnings("serial")
public class GTCubeStorageQueryRequest implements Serializable {
    private Cuboid cuboid;
    private Set<TblColRef> dimensions;
    private Set<TblColRef> groups;
    private Set<TblColRef> filterCols;
    private Set<FunctionDesc> metrics;
    private List<TblColRef> dynGroups;
    private List<TupleExpression> dynGroupExprs;
    private List<DynamicFunctionDesc> dynFuncs;
    private TupleFilter filter;
    private TupleFilter havingFilter;
    private StorageContext context;

    public GTCubeStorageQueryRequest(Cuboid cuboid, Set<TblColRef> dimensions, //
            Set<TblColRef> groups, List<TblColRef> dynGroups, List<TupleExpression> dynGroupExprs, //
            Set<TblColRef> filterCols, Set<FunctionDesc> metrics, List<DynamicFunctionDesc> dynFuncs, //
            TupleFilter filter, TupleFilter havingFilter, StorageContext context) {
        this.cuboid = cuboid;
        this.dimensions = dimensions;
        this.groups = groups;
        this.dynGroups = dynGroups;
        this.dynGroupExprs = dynGroupExprs;
        this.filterCols = filterCols;
        this.metrics = metrics;
        this.dynFuncs = dynFuncs;
        this.filter = filter;
        this.havingFilter = havingFilter;
        this.context = context;
    }

    public Cuboid getCuboid() {
        return cuboid;
    }

    public void setCuboid(Cuboid cuboid) {
        this.cuboid = cuboid;
    }

    public Set<TblColRef> getDimensions() {
        return dimensions;
    }

    public void setDimensions(Set<TblColRef> dimensions) {
        this.dimensions = dimensions;
    }

    public Set<TblColRef> getGroups() {
        return groups;
    }

    public void setGroups(Set<TblColRef> groups) {
        this.groups = groups;
    }

    public List<TblColRef> getDynGroups() {
        return dynGroups;
    }

    public List<TupleExpression> getDynGroupExprs() {
        return dynGroupExprs;
    }

    public Set<FunctionDesc> getMetrics() {
        return metrics;
    }

    public void setMetrics(Set<FunctionDesc> metrics) {
        this.metrics = metrics;
    }

    public List<DynamicFunctionDesc> getDynFuncs() {
        return dynFuncs;
    }

    public TupleFilter getFilter() {
        return filter;
    }

    public void setFilter(TupleFilter filter) {
        this.filter = filter;
    }

    public TupleFilter getHavingFilter() {
        return havingFilter;
    }

    public void setHavingFilter(TupleFilter havingFilter) {
        this.havingFilter = havingFilter;
    }

    public StorageContext getContext() {
        return context;
    }

    public void setContext(StorageContext context) {
        this.context = context;
    }

    public Set<TblColRef> getFilterCols() {
        return filterCols;
    }

    public void setFilterCols(Set<TblColRef> filterCols) {
        this.filterCols = filterCols;
    }
}
