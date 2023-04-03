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
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Ordering;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.relnode.OLAPContext;

import lombok.Getter;

public class QueryRouter {

    private QueryRouter() {
    }

    public static void applyRules(Candidate candidate) {
        Strategy pruningStrategy = getStrategy(candidate.getCtx());
        for (PruningRule r : pruningStrategy.getRules()) {
            r.apply(candidate);
        }
    }

    public static void sortCandidates(OLAPContext context, List<Candidate> candidates) {
        Strategy strategy = getStrategy(context);
        candidates.sort(strategy.getSorter());
    }

    private static Strategy getStrategy(OLAPContext context) {
        String project = context.olapSchema.getProjectName();
        KylinConfig projectConfig = NProjectManager.getProjectConfig(project);
        return new Strategy(projectConfig);
    }

    public static class Strategy {
        private static final PruningRule SEGMENT_PRUNING = new SegmentPruningRule();
        private static final PruningRule PARTITION_PRUNING = new PartitionPruningRule();
        private static final PruningRule REMOVE_INCAPABLE_REALIZATIONS = new RemoveIncapableRealizationsRule();

        @Getter
        List<PruningRule> rules = Lists.newArrayList();

        private final List<Comparator<Candidate>> sorters = Lists.newArrayList();

        public Comparator<Candidate> getSorter() {
            return Ordering.compound(sorters);
        }

        public Strategy(KylinConfig config) {

            // add all rules
            rules.add(SEGMENT_PRUNING);
            rules.add(PARTITION_PRUNING);
            rules.add(REMOVE_INCAPABLE_REALIZATIONS);

            // add all sorters
            if (config.useTableIndexAnswerSelectStarEnabled()) {
                sorters.add(Candidate.tableIndexUnmatchedColSizeSorter());
            }
            sorters.add(Candidate.modelPrioritySorter());
            sorters.add(Candidate.realizationCostSorter());
            sorters.add(Candidate.realizationCapabilityCostSorter());
            sorters.add(Candidate.modelUuidSorter());
        }
    }
}
