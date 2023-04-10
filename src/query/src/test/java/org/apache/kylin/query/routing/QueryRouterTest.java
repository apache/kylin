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

import java.util.List;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.util.MetadataTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.val;

@MetadataInfo
class QueryRouterTest {
    @Test
    void testSort() {
        {
            Candidate c1 = CandidateTestUtils.mockCandidate("model0001", "modelA", 1, 1);
            Candidate c2 = CandidateTestUtils.mockCandidate("model0002", "modelB", 2, 2);
            assertSortedResults(c1, Lists.newArrayList(c1, c2));
        }

        {
            Candidate c1 = CandidateTestUtils.mockCandidate("model0001", "modelA", 2, 1);
            Candidate c2 = CandidateTestUtils.mockCandidate("model0002", "modelB", 2, 2);
            List<Candidate> candidates = Lists.newArrayList(c1, c2);
            QueryRouter.sortCandidates("default", candidates);
            Assertions.assertEquals(c1, candidates.get(0));
        }

        {
            Candidate c1 = CandidateTestUtils.mockCandidate("model0001", "modelA", 2, 2);
            Candidate c2 = CandidateTestUtils.mockCandidate("model0002", "modelB", 2, 2);
            List<Candidate> candidates = Lists.newArrayList(c1, c2);
            QueryRouter.sortCandidates("default", candidates);
            Assertions.assertEquals(c1, candidates.get(0));
        }

        {
            Candidate c1 = CandidateTestUtils.mockCandidate("model0001", "modelA", 1, 1);
            Candidate c2 = CandidateTestUtils.mockCandidate("model0002", "modelB", 2, 2);
            Candidate c3 = CandidateTestUtils.mockCandidate("model0003", "modelC", 4, 4);
            List<Candidate> candidates = Lists.newArrayList(c1, c2, c3);
            QueryRouter.sortCandidates("default", candidates);
            Assertions.assertEquals(c1, candidates.get(0));
        }

        {
            Candidate c1 = CandidateTestUtils.mockCandidate("model0001", "modelA", 1, 1);
            Candidate c2 = mockEmptyCandidate("model0003", "modelC", 1);
            List<Candidate> candidates = Lists.newArrayList(c1, c2);
            QueryRouter.sortCandidates("default", candidates);
            Assertions.assertEquals(c1, candidates.get(0));
        }

        {
            Candidate c1 = mockStreamingCandidate("model0001", "modelA", 2, 1);
            Candidate c2 = mockEmptyCandidate("model0002", "modelB", 2);
            List<Candidate> candidates = Lists.newArrayList(c1, c2);
            QueryRouter.sortCandidates("default", candidates);
            Assertions.assertEquals(c1, candidates.get(0));
        }

        {
            Candidate c1 = mockHybridCandidate("model0001", "modelA", 3, 1, 2);
            Candidate c2 = mockEmptyCandidate("model0002", "modelB", 3);

            assertSortedResults(c1, Lists.newArrayList(c1, c2));
        }

        {
            Candidate c1 = CandidateTestUtils.mockCandidate("model0001", "modelA", 1, 3);
            Candidate c2 = mockStreamingCandidate("model0002", "modelB", 1, 2);
            Candidate c3 = mockHybridCandidate("model0003", "modelC", 1, 4, 2.5);
            List<Candidate> candidates = Lists.newArrayList(c1, c2, c3);
            QueryRouter.sortCandidates("default", candidates);
            Assertions.assertEquals(c2, candidates.get(0));
        }
    }

    @Test
    void testSortWithVacantPruningRule() {
        // This property does affect the sorting of candidates in different models.
        MetadataTestUtils.updateProjectConfig("default", "kylin.query.index-match-rules",
                QueryRouter.USE_VACANT_INDEXES);
        testSort();
    }

    @Test
    void testTableIndexAnswerSelectStar() {
        String useTableIndexAnswerSelectStar = "kylin.query.use-tableindex-answer-select-star.enabled";
        MetadataTestUtils.updateProjectConfig("default", useTableIndexAnswerSelectStar, "true");
        Candidate c1 = CandidateTestUtils.mockCandidate("model0001", "modelA", 2, 1, 1);
        Candidate c2 = CandidateTestUtils.mockCandidate("model0002", "modelB", 1, 1, 2);
        assertSortedResults(c1, Lists.newArrayList(c1, c2));

        MetadataTestUtils.updateProjectConfig("default", useTableIndexAnswerSelectStar, "false");
        assertSortedResults(c2, Lists.newArrayList(c1, c2));
    }

    private void assertSortedResults(Candidate expectCandidate, List<Candidate> candidates) {
        QueryRouter.sortCandidates("default", candidates);
        Assertions.assertEquals(expectCandidate, candidates.get(0));
    }

    private Candidate mockStreamingCandidate(String modelId, String modelName, int realizationCost,
            double candidateCost) {
        IRealization realization = CandidateTestUtils.mockRealization(modelId, modelName, realizationCost);
        OLAPContext olapContext = CandidateTestUtils.mockOlapContext();
        val candidate = new Candidate(realization, olapContext, Maps.newHashMap());
        val cap = new CapabilityResult();
        cap.setSelectedStreamingCandidate(() -> candidateCost);
        cap.setCost(cap.getSelectedStreamingCandidate().getCost());
        candidate.setCapability(cap);
        return candidate;
    }

    private Candidate mockHybridCandidate(String modelId, String modelName, int realizationCost, double candidateCost,
            double streamingCandidateCost) {
        IRealization realization = CandidateTestUtils.mockRealization(modelId, modelName, realizationCost);
        OLAPContext olapContext = CandidateTestUtils.mockOlapContext();
        val candidate = new Candidate(realization, olapContext, Maps.newHashMap());
        val cap = new CapabilityResult();
        cap.setSelectedCandidate(() -> candidateCost);
        cap.setSelectedStreamingCandidate(() -> streamingCandidateCost);
        cap.setCost(
                (int) Math.min(cap.getSelectedCandidate().getCost(), cap.getSelectedStreamingCandidate().getCost()));
        candidate.setCapability(cap);
        return candidate;
    }

    private Candidate mockEmptyCandidate(String modelId, String modelName, int realizationCost) {
        IRealization realization = CandidateTestUtils.mockRealization(modelId, modelName, realizationCost);
        OLAPContext olapContext = CandidateTestUtils.mockOlapContext();
        val candidate = new Candidate(realization, olapContext, Maps.newHashMap());
        candidate.realization = CandidateTestUtils.mockRealization(modelId, modelName, realizationCost);
        val cap = new CapabilityResult();
        cap.setSelectedCandidate(NLayoutCandidate.EMPTY);
        cap.setSelectedStreamingCandidate(NLayoutCandidate.EMPTY);
        candidate.setCapability(cap);
        return candidate;
    }
}
