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

import org.apache.kylin.common.QueryContext;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.query.relnode.OLAPContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@MetadataInfo
class CandidateSortTest {

    @Test
    void testModelPrioritySorter() {
        try (QueryContext queryContext = QueryContext.current()) {
            Comparator<Candidate> sorter = Candidate.modelPrioritySorter();

            // assert that c1 is more prioritary than c2
            {
                String[] modelPriorities = CandidateTestUtils
                        .mockModelPriorityValues(new String[] { "modelA", "modelB" });
                queryContext.setModelPriorities(modelPriorities);
                Candidate c1 = CandidateTestUtils.mockCandidate("model0001", "modelA", 1, 1);
                Candidate c2 = CandidateTestUtils.mockCandidate("model0001", "modelB", 1, 1);

                Assertions.assertEquals(-1, sorter.compare(c1, c2));
                Assertions.assertEquals(1, sorter.compare(c2, c1));
                assertSortResult(c1, sorter, Lists.newArrayList(c1, c2));
            }

            // with empty model priorities, c1 and c2 has the same priority
            {
                queryContext.setModelPriorities(new String[] {});
                Candidate c1 = CandidateTestUtils.mockCandidate("model0001", "modelA", 1, 1);
                Candidate c2 = CandidateTestUtils.mockCandidate("model0002", "modelB", 2, 2);
                assertSortResult(c1, sorter, Lists.newArrayList(c1, c2));
            }

            {
                queryContext.setModelPriorities(CandidateTestUtils.mockModelPriorityValues(new String[] { "modelB" }));
                Candidate c1 = CandidateTestUtils.mockCandidate("model0001", "modelA", 1, 1);
                Candidate c2 = CandidateTestUtils.mockCandidate("model0002", "modelB", 2, 2);
                Assertions.assertEquals(Integer.MAX_VALUE, sorter.compare(c1, c2));
                assertSortResult(c2, sorter, Lists.newArrayList(c1, c2));
            }

            {
                queryContext.setModelPriorities(new String[] { "MODELB", "MODELA" });
                Candidate c1 = CandidateTestUtils.mockCandidate("model0001", "modelA", 1, 1);
                Candidate c2 = CandidateTestUtils.mockCandidate("model0002", "modelB", 2, 2);
                Assertions.assertEquals(1, sorter.compare(c1, c2));
                assertSortResult(c2, sorter, Lists.newArrayList(c1, c2));
            }
        }
    }

    @Test
    void realizationCostSorterTest() {
        Comparator<Candidate> comparator = Candidate.realizationCostSorter();
        NDataflow df1 = Mockito.mock(NDataflow.class);
        NDataflow df2 = Mockito.mock(NDataflow.class);
        OLAPContext olapContext = new OLAPContext(0);
        olapContext.allColumns = Sets.newHashSet();
        Candidate c1 = new Candidate(df1, olapContext, Maps.newHashMap());
        Candidate c2 = new Candidate(df2, olapContext, Maps.newHashMap());
        Mockito.when(c1.getRealization().getCost()).thenReturn(1);
        Mockito.when(c2.getRealization().getCost()).thenReturn(2);

        // Assert that the Comparator sorts the Candidates correctly
        assertSortResult(c1, comparator, Lists.newArrayList(c1, c2));
    }

    @Test
    void partialResultSorter() {
        Candidate c1 = CandidateTestUtils.mockCandidate("model0001", "modelA", 2, false);
        Candidate c2 = CandidateTestUtils.mockCandidate("model0002", "modelB", 2, true);
        Candidate c3 = CandidateTestUtils.mockCandidate("model0003", "modelC", 1, false);
        Comparator<Candidate> comparator = Candidate.partialResultSorter();
        assertSortResult(c1, comparator, Lists.newArrayList(c1, c2));
        assertSortResult(c3, comparator, Lists.newArrayList(c2, c3));
        assertSortResult(c1, comparator, Lists.newArrayList(c1, c3));
    }

    @Test
    void realizationCapabilityCostSorter() {
        {
            Candidate c1 = CandidateTestUtils.mockCandidate("model0001", "modelA", 1, 1);
            Candidate c2 = CandidateTestUtils.mockCandidate("model0001", "modelA", 1, 2);
            Candidate c3 = CandidateTestUtils.mockCandidate("model0001", "modelA", 1, 2);
            Comparator<Candidate> comparator = Candidate.realizationCapabilityCostSorter();
            assertSortResult(c1, comparator, Lists.newArrayList(c1, c2));
            assertSortResult(c2, comparator, Lists.newArrayList(c2, c3));
        }

        {
            Candidate c1 = CandidateTestUtils.mockCandidate("model0001", "modelA", 2, false);
            Candidate c2 = CandidateTestUtils.mockCandidate("model0002", "modelB", 2, true);
            Candidate c3 = CandidateTestUtils.mockCandidate("model0003", "modelC", 1, false);
            Candidate c4 = CandidateTestUtils.mockCandidate("model0004", "modelD", 1, true);
            Comparator<Candidate> comparator = Candidate.partialResultSorter()
                    .thenComparing(Candidate.realizationCapabilityCostSorter());
            assertSortResult(c1, comparator, Lists.newArrayList(c1, c2));
            assertSortResult(c2, comparator, Lists.newArrayList(c2, c4));
            assertSortResult(c3, comparator, Lists.newArrayList(c1, c3));
            assertSortResult(c3, comparator, Lists.newArrayList(c1, c2, c3, c4));
        }
    }

    @Test
    void testModelUuidSorter() {
        Candidate c1 = CandidateTestUtils.mockCandidate("model0001", "modelA", 1, 1);
        Candidate c2 = CandidateTestUtils.mockCandidate("model0002", "modelB", 1, 1);
        Comparator<Candidate> comparator = Candidate.modelUuidSorter();
        assertSortResult(c1, comparator, Lists.newArrayList(c1, c2));
    }

    @Test
    void testTableIndexUnmatchedColSizeComparator() {
        Comparator<Candidate> comparator = Candidate.tableIndexUnmatchedColSizeSorter();
        Candidate c1 = CandidateTestUtils.mockCandidate("model0001", "modelA", 1, 1, 1);
        Candidate c2 = CandidateTestUtils.mockCandidate("model0002", "modelB", 1, 2, 2);
        assertSortResult(c1, comparator, Lists.newArrayList(c2, c1));
    }

    private void assertSortResult(Candidate expected, Comparator<Candidate> comparator, List<Candidate> candidates) {
        candidates.sort(comparator);
        Assertions.assertEquals(expected, candidates.get(0));
    }
}
