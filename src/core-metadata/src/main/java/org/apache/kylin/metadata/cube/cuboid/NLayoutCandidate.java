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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealizationCandidate;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class NLayoutCandidate implements IRealizationCandidate {

    public static final NLayoutCandidate EMPTY = new NLayoutCandidate(new LayoutEntity(), Double.MAX_VALUE,
            new CapabilityResult());

    private LayoutEntity layoutEntity;
    private double cost;
    private CapabilityResult capabilityResult;
    private long range;
    private long maxSegEnd;
    private Map<Integer, DeriveInfo> derivedToHostMap = Maps.newHashMap();
    Set<String> derivedTableSnapshots = Sets.newHashSet();

    public NLayoutCandidate(LayoutEntity layoutEntity) {
        Preconditions.checkNotNull(layoutEntity);
        this.layoutEntity = layoutEntity;
    }

    public NLayoutCandidate(LayoutEntity layoutEntity, double cost, CapabilityResult result) {
        this(layoutEntity);
        this.cost = cost;
        this.capabilityResult = result;
    }

    public boolean isEmptyCandidate() {
        return this.getLayoutEntity().getIndex() == null;
    }

    public Map<List<Integer>, List<DeriveInfo>> makeHostToDerivedMap() {
        Map<List<Integer>, List<DeriveInfo>> hostToDerivedMap = Maps.newHashMap();
        derivedToHostMap.forEach((derivedColId, deriveInfo) -> {
            DeriveInfo.DeriveType type = deriveInfo.type;
            List<Integer> columns = deriveInfo.columns;
            List<DeriveInfo> infoList = hostToDerivedMap.computeIfAbsent(columns, k -> Lists.newArrayList());
            if (!isMerged(derivedColId, deriveInfo, infoList)) {
                infoList.add(new DeriveInfo(type, deriveInfo.join, Lists.newArrayList(derivedColId), false));
            }
        });
        return hostToDerivedMap;
    }

    // Merged duplicated derived column
    private static boolean isMerged(Integer derCol, DeriveInfo deriveInfo, List<DeriveInfo> infoList) {
        DeriveInfo.DeriveType type = deriveInfo.type;
        boolean merged = false;
        for (DeriveInfo existing : infoList) {
            if (existing.type == type && existing.join.getPKSide().equals(deriveInfo.join.getPKSide())) {
                if (existing.columns.contains(derCol)) {
                    merged = true;
                }
                if (type == DeriveInfo.DeriveType.LOOKUP || type == DeriveInfo.DeriveType.LOOKUP_NON_EQUI) {
                    existing.columns.add(derCol);
                    merged = true;
                }
            }
            if (merged) {
                break;
            }
        }
        return merged;
    }

    @Override
    public String toString() {
        String type = "";
        if (layoutEntity.isManual()) {
            type += "manual";
        } else if (layoutEntity.isAuto()) {
            type += "auto";
        }
        if (layoutEntity.isBase()) {
            type += type.isEmpty() ? "base" : ",base";
        }
        if (type.isEmpty()) {
            type = "unknown";
        }
        return "LayoutCandidate{" + "layout=" + layoutEntity //
                + ", type=" + type //
                + ", cost=" + cost //
                + "}";
    }
}
