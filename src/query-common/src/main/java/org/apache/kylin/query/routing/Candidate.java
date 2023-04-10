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

package org.apache.kylin.query.routing;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.QueryableSeg;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPContextProp;

import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.Getter;
import lombok.Setter;

@Getter
public class Candidate {

    // ============================================================================

    IRealization realization;
    OLAPContext ctx;

    @Setter
    CapabilityResult capability;
    @Setter
    OLAPContextProp rewrittenCtx;
    @Setter
    Map<String, String> matchedJoinsGraphAliasMap;

    @Setter
    private Map<String, List<Long>> prunedPartitions;

    private final QueryableSeg queryableSeg = new QueryableSeg();

    public List<NDataSegment> getPrunedSegments(NDataflow df) {
        if (df.isStreaming()) {
            return queryableSeg.getStreamingSegments();
        } else {
            return queryableSeg.getBatchSegments();
        }
    }

    public Map<String, Set<Long>> getChSegToLayoutsMap(NDataflow df) {
        return df.isStreaming() ? Maps.newHashMap() : queryableSeg.getChSegToLayoutsMap();
    }

    public void setPrunedSegments(Segments<NDataSegment> prunedSegments, NDataflow df) {
        if (df.isStreaming()) {
            queryableSeg.setStreamingSegments(prunedSegments);
        } else {
            queryableSeg.setBatchSegments(prunedSegments);
            fillSecondStorageLayouts(df);
        }
    }

    private void fillSecondStorageLayouts(NDataflow df) {
        Map<String, Set<Long>> secondStorageSegmentLayoutMap = Maps.newHashMap();
        if (SecondStorageUtil.isModelEnable(df.getProject(), df.getId())) {
            for (NDataSegment segment : queryableSeg.getBatchSegments()) {
                Set<Long> chEnableLayoutIds = SecondStorageUtil.listEnableLayoutBySegment(df.getProject(), df.getId(),
                        segment.getId());
                if (CollectionUtils.isNotEmpty(chEnableLayoutIds)) {
                    secondStorageSegmentLayoutMap.put(segment.getId(), chEnableLayoutIds);
                }
            }
        }
        queryableSeg.setChSegToLayoutsMap(secondStorageSegmentLayoutMap);
    }

    public Candidate(IRealization realization, OLAPContext ctx, Map<String, String> matchedJoinsGraphAliasMap) {
        this.realization = realization;
        this.ctx = ctx;
        this.matchedJoinsGraphAliasMap = matchedJoinsGraphAliasMap;
    }

    @Override
    public String toString() {
        return realization.toString();
    }

    public static Comparator<Candidate> tableIndexUnmatchedColSizeSorter() {
        return Comparator.comparingInt(c -> c.getCapability().getLayoutUnmatchedColsSize());
    }

    public static Comparator<Candidate> modelPrioritySorter() {
        return (c1, c2) -> {
            if (QueryContext.current().getModelPriorities().length == 0) {
                return 0;
            }
            Map<String, Integer> priorities = new HashMap<>();
            for (int i = 0; i < QueryContext.current().getModelPriorities().length; i++) {
                priorities.put(QueryContext.current().getModelPriorities()[i], i);
            }

            String modelAlias1 = StringUtils.upperCase(c1.getRealization().getModel().getAlias());
            String modelAlias2 = StringUtils.upperCase(c2.getRealization().getModel().getAlias());
            return priorities.getOrDefault(modelAlias1, Integer.MAX_VALUE)
                    - priorities.getOrDefault(modelAlias2, Integer.MAX_VALUE);
        };
    }

    public static Comparator<Candidate> realizationCostSorter() {
        return Comparator.comparingInt(c -> c.getRealization().getCost());
    }

    public static Comparator<Candidate> realizationCapabilityCostSorter() {
        return Comparator.comparingDouble(c -> c.getCapability().getCost());
    }

    public static Comparator<Candidate> modelUuidSorter() {
        return Comparator.comparing(c -> c.getRealization().getModel().getId());
    }
}
