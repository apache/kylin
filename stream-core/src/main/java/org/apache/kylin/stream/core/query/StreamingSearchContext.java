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

package org.apache.kylin.stream.core.query;

import java.util.Set;

import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class StreamingSearchContext {
    private CubeDesc cubeDesc;
    private TupleFilter filter;
    private Set<TblColRef> dimensions;
    private Set<TblColRef> groups;
    private TupleFilter havingFilter;
    private Set<FunctionDesc> metrics;

    private Set<TblColRef> addedGroups;

    private ResponseResultSchema respResultSchema;

    private StreamingDataQueryPlanner queryPlanner;

    private long hitCuboid;
    private long basicCuboid;

    public StreamingSearchContext(CubeDesc cubeDesc, Set<TblColRef> dimensions, Set<TblColRef> groups,
                                  Set<FunctionDesc> metrics, TupleFilter filter, TupleFilter havingFilter) {
        this.cubeDesc = cubeDesc;
        this.dimensions = dimensions;
        this.groups = groups;
        this.metrics = metrics;
        this.filter = filter;
        this.havingFilter = havingFilter;
        this.respResultSchema = new ResponseResultSchema(cubeDesc, dimensions, metrics);
        this.queryPlanner = new StreamingDataQueryPlanner(cubeDesc, filter);
        this.addedGroups = Sets.newHashSet();
        calculateHitCuboid();
    }

    public TupleFilter getFilter() {
        return filter;
    }

    public Set<TblColRef> getGroups() {
        return groups;
    }

    public void addNewGroups(Set<TblColRef> newGroups) {
        addedGroups.addAll(newGroups);
    }

    public Set<TblColRef> getAllGroups() {
        if (addedGroups.isEmpty()) {
            return groups;
        }
        return Sets.union(groups, addedGroups);
    }

    public Set<FunctionDesc> getMetrics() {
        return metrics;
    }

    public Set<TblColRef> getDimensions() {
        return dimensions;
    }

    public ResponseResultSchema getRespResultSchema() {
        return respResultSchema;
    }

    public TupleFilter getHavingFilter() {
        return havingFilter;
    }

    public void setRespResultSchema(ResponseResultSchema respResultSchema) {
        this.respResultSchema = respResultSchema;
    }

    public CubeDesc getCubeDesc() {
        return cubeDesc;
    }

    public long getHitCuboid() {
        return hitCuboid;
    }

    public void setHitCuboid(long hitCuboid) {
        this.hitCuboid = hitCuboid;
    }

    public void setBasicCuboid(long basicCuboid) {
        this.basicCuboid = basicCuboid;
    }

    public boolean hitBasicCuboid() {
        return hitCuboid == basicCuboid;
    }

    public StreamingDataQueryPlanner getQueryPlanner() {
        return queryPlanner;
    }

    public void setQueryPlanner(StreamingDataQueryPlanner queryPlanner) {
        this.queryPlanner = queryPlanner;
    }

    private void calculateHitCuboid() {
        long basicCuboid = Cuboid.getBaseCuboidId(cubeDesc);
        this.setBasicCuboid(basicCuboid);
        if (!cubeDesc.getConfig().isStreamingBuildAdditionalCuboids()) {
            this.setHitCuboid(basicCuboid);
            return;
        }
        long targetCuboidID = identifyCuboid(dimensions);
        Set<Long> mandatoryCuboids = getMandatoryCuboids();
        for (long cuboidID : mandatoryCuboids) {
            if ((targetCuboidID & ~cuboidID) == 0) {
                this.setHitCuboid(cuboidID);
                return;
            }
        }
        this.setHitCuboid(basicCuboid);
    }

    private long identifyCuboid(Set<TblColRef> dimensions) {
        long cuboidID = 0;
        for (TblColRef column : dimensions) {
            int index = cubeDesc.getRowkey().getColumnBitIndex(column);
            cuboidID |= 1L << index;
        }
        return cuboidID;
    }

    private Set<Long> getMandatoryCuboids() {
        Set<Long> sortedSet = Sets.newTreeSet(Cuboid.cuboidSelectComparator);
        sortedSet.addAll(cubeDesc.getMandatoryCuboids());
        return sortedSet;
    }
}
