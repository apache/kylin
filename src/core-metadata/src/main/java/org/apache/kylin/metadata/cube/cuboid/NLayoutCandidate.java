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
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayoutDetails;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealizationCandidate;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class NLayoutCandidate implements IRealizationCandidate {

    private LayoutEntity layoutEntity;
    private NDataLayoutDetails dataLayoutDetails;
    private long layoutId;
    private double cost;
    private List<NDataSegment> prunedSegments;
    private CapabilityResult capabilityResult;
    private long range;
    private long maxSegEnd;
    private Map<Integer, DeriveInfo> derivedToHostMap = Maps.newHashMap();
    Set<String> derivedLookups = Sets.newHashSet();
    private boolean isStreaming;

    public NLayoutCandidate(LayoutEntity layoutEntity) {
        Preconditions.checkNotNull(layoutEntity);
        this.layoutEntity = layoutEntity;
        this.layoutId = layoutEntity.getId();
    }

    public NLayoutCandidate(LayoutEntity layoutEntity, NDataLayoutDetails dataLayoutDetails) {
        Preconditions.checkNotNull(layoutEntity);
        Preconditions.checkNotNull(dataLayoutDetails);
        this.layoutEntity = layoutEntity;
        this.layoutId = layoutEntity.getId();
        this.dataLayoutDetails = dataLayoutDetails;
    }

    public NLayoutCandidate(LayoutEntity layoutEntity, double cost, CapabilityResult result) {
        this(layoutEntity);
        this.cost = cost;
        this.capabilityResult = result;
    }

    public static NLayoutCandidate ofEmptyCandidate() {
        LayoutEntity layout = new LayoutEntity();
        layout.setId(-1L);
        return new NLayoutCandidate(layout, Integer.MAX_VALUE, new CapabilityResult());
    }

    public boolean isEmpty() {
        return layoutEntity == null || this.layoutEntity.getId() == -1L;
    }

    public boolean isTableIndex() {
        return !isEmpty() && IndexEntity.isTableIndex(layoutId);
    }

    public boolean isAggIndex() {
        return !isEmpty() && IndexEntity.isAggIndex(layoutId);
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
