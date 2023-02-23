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
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPContextProp;

import lombok.Getter;
import lombok.Setter;

public class Candidate {

    public static final CandidateComparator COMPARATOR = new CandidateComparator();
    public static final CandidateTableIndexComparator COMPARATOR_TABLE_INDEX = new CandidateTableIndexComparator();

    // ============================================================================

    IRealization realization;
    @Getter
    OLAPContext ctx;
    SQLDigest sqlDigest;
    CapabilityResult capability;
    @Getter
    @Setter
    OLAPContextProp rewrittenCtx;
    @Getter
    @Setter
    Map<String, String> aliasMap;

    @Getter
    @Setter
    private List<NDataSegment> prunedSegments;

    @Getter
    @Setter
    private List<NDataSegment> prunedStreamingSegments;

    @Getter
    @Setter
    private Map<String, Set<Long>> secondStorageSegmentLayoutMap;

    public void setPrunedSegments(List<NDataSegment> prunedSegments, boolean isStreaming) {
        if (isStreaming) {
            this.prunedStreamingSegments = prunedSegments;
        } else {
            this.prunedSegments = prunedSegments;
        }
    }

    @Getter
    @Setter
    private Map<String, List<Long>> prunedPartitions;

    public Candidate(IRealization realization, OLAPContext ctx, Map<String, String> aliasMap) {
        this.realization = realization;
        this.ctx = ctx;
        this.aliasMap = aliasMap;
    }

    // for testing only
    Candidate() {
    }

    public IRealization getRealization() {
        return realization;
    }

    public CapabilityResult getCapability() {
        return capability;
    }

    public void setCapability(CapabilityResult capability) {
        this.capability = capability;
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

            int comp = priorities.getOrDefault(real1.getModel().getAlias().toUpperCase(Locale.ROOT),
                    Integer.MAX_VALUE)
                    - priorities.getOrDefault(real2.getModel().getAlias().toUpperCase(Locale.ROOT),
                    Integer.MAX_VALUE);
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
