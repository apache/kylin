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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealizationCandidate;

import lombok.Getter;
import lombok.Setter;

public class NLayoutCandidate implements IRealizationCandidate {
    private @Nonnull LayoutEntity layoutEntity;
    @Setter
    private double cost;

    @Setter
    @Getter
    private CapabilityResult capabilityResult;

    public static final NLayoutCandidate EMPTY = new NLayoutCandidate(new LayoutEntity(), Double.MAX_VALUE,
            new CapabilityResult());

    // derived
    private @Nonnull Map<Integer, DeriveInfo> derivedToHostMap = Maps.newHashMap();

    @Getter
    @Setter
    Set<String> derivedTableSnapshots = new HashSet<>();

    public NLayoutCandidate(@Nonnull LayoutEntity layoutEntity) {
        this.layoutEntity = layoutEntity;
    }

    public NLayoutCandidate(@Nonnull LayoutEntity layoutEntity, double cost, CapabilityResult result) {
        this.layoutEntity = layoutEntity;
        this.cost = cost;
        this.capabilityResult = result;
    }

    public boolean isEmptyCandidate() {
        return this.getLayoutEntity().getIndex() == null;
    }

    @Nonnull
    public LayoutEntity getLayoutEntity() {
        return layoutEntity;
    }

    public void setLayoutEntity(@Nonnull LayoutEntity cuboidLayout) {
        this.layoutEntity = cuboidLayout;
    }

    @Nonnull
    public Map<Integer, DeriveInfo> getDerivedToHostMap() {
        return derivedToHostMap;
    }

    public void setDerivedToHostMap(@Nonnull Map<Integer, DeriveInfo> derivedToHostMap) {
        this.derivedToHostMap = derivedToHostMap;
    }

    public Map<List<Integer>, List<DeriveInfo>> makeHostToDerivedMap() {
        Map<List<Integer>, List<DeriveInfo>> hostToDerivedMap = Maps.newHashMap();

        for (Map.Entry<Integer, DeriveInfo> entry : derivedToHostMap.entrySet()) {

            Integer derCol = entry.getKey();
            List<Integer> hostCols = entry.getValue().columns;
            DeriveInfo.DeriveType type = entry.getValue().type;
            JoinDesc join = entry.getValue().join;

            List<DeriveInfo> infoList = hostToDerivedMap.computeIfAbsent(hostCols, k -> new ArrayList<>());

            // Merged duplicated derived column
            boolean merged = false;
            for (DeriveInfo existing : infoList) {
                if (existing.type == type && existing.join.getPKSide().equals(join.getPKSide())) {
                    if (existing.columns.contains(derCol)) {
                        merged = true;
                        break;
                    }
                    if (type == DeriveInfo.DeriveType.LOOKUP || type == DeriveInfo.DeriveType.LOOKUP_NON_EQUI) {
                        existing.columns.add(derCol);
                        merged = true;
                        break;
                    }
                }
            }
            if (!merged)
                infoList.add(new DeriveInfo(type, join, Lists.newArrayList(derCol), false));
        }

        return hostToDerivedMap;
    }

    @Override
    public double getCost() {
        return this.cost;
    }

    @Override
    public String toString() {
        return "LayoutCandidate{" + "cuboidLayout=" + layoutEntity + ", indexEntity=" + layoutEntity.getIndex()
                + ", cost=" + cost + '}';
    }
}
