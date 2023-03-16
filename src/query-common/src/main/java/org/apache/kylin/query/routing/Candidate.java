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

    public static final CandidateComparator COMPARATOR = new CandidateComparator();
    public static final CandidateTableIndexComparator COMPARATOR_TABLE_INDEX = new CandidateTableIndexComparator();

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

    // for testing only
    Candidate() {
    }

    @Override
    public String toString() {
        return realization.toString();
    }

    public static class CandidateComparator implements Comparator<Candidate> {

        @Override
        public int compare(Candidate c1, Candidate c2) {
            return compareCandidate(c1, c2);
        }
    }

    public static class CandidateTableIndexComparator implements Comparator<Candidate> {

        @Override
        public int compare(Candidate c1, Candidate c2) {
            CapabilityResult capabilityResult1 = c1.getCapability();
            CapabilityResult capabilityResult2 = c2.getCapability();
            if (capabilityResult1.getLayoutUnmatchedColsSize() != capabilityResult2.getLayoutUnmatchedColsSize()) {
                return capabilityResult1.getLayoutUnmatchedColsSize() - capabilityResult2.getLayoutUnmatchedColsSize();
            }
            return compareCandidate(c1, c2);
        }
    }

    private static int compareCandidate(Candidate c1, Candidate c2) {
        IRealization real1 = c1.getRealization();
        IRealization real2 = c2.getRealization();

        if (QueryContext.current().getModelPriorities().length > 0) {

            Map<String, Integer> priorities = new HashMap<>();
            for (int i = 0; i < QueryContext.current().getModelPriorities().length; i++) {
                priorities.put(QueryContext.current().getModelPriorities()[i], i);
            }

            int comp = priorities.getOrDefault(StringUtils.upperCase(real1.getModel().getAlias()), Integer.MAX_VALUE)
                    - priorities.getOrDefault(StringUtils.upperCase(real2.getModel().getAlias()), Integer.MAX_VALUE);
            if (comp != 0) {
                return comp;
            }
        }

        int comp = real1.getCost() - real2.getCost();
        if (comp != 0) {
            return comp;
        }

        comp = Double.compare(c1.capability.cost, c2.capability.cost);
        if (comp != 0) {
            return comp;
        }

        return real1.getModel().getId().compareTo(real2.getModel().getId());
    }
}
