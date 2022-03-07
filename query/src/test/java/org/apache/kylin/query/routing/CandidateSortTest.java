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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class CandidateSortTest {
    @Test
    public void testCubeHintCandidateSort() {
        QueryContext queryContext = QueryContextFacade.current();
        {
            queryContext.setCubePriorities(new String[]{});
            Candidate cube1 = mockCandidate("cube0001", "cubeA", 1, 1);
            Candidate cube2 = mockCandidate("cube0002", "cubeB", 2, 2);
            sort(cube1, cube2).assertFirst(cube1);
        }

        {
            queryContext.setCubePriorities(new String[]{"cubeb"});
            Candidate cube1 = mockCandidate("cube0001", "cubeA", 1, 1);
            Candidate cube2 = mockCandidate("cube0002", "cubeB", 2, 2);
            sort(cube1, cube2).assertFirst(cube2);
        }

        {
            queryContext.setCubePriorities(new String[]{"cubeb", "cubea"});
            Candidate cube1 = mockCandidate("cube0001", "cubeA", 1, 1);
            Candidate cube2 = mockCandidate("cube0002", "cubeB", 2, 2);
            sort(cube1, cube2).assertFirst(cube2);
        }

        {
            queryContext.setCubePriorities(new String[]{"cubec", "cubea"});
            Candidate cube1 = mockCandidate("cube0001", "cubeA", 1, 1);
            Candidate cube2 = mockCandidate("cube0002", "cubeB", 2, 2);
            Candidate cube3 = mockCandidate("cube0003", "cubeC", 4, 4);
            sort(cube1, cube2, cube3).assertFirst(cube3);
        }
    }

    @Test
    public void testSort() {
        {
            Candidate cube1 = mockCandidate("cube0001", "cubeA", 1, 1);
            Candidate cube2 = mockCandidate("cube0002", "cubeB", 2, 2);
            sort(cube1, cube2).assertFirst(cube1);
        }

        {
            Candidate cube1 = mockCandidate("cube0001", "cubeA", 2, 1);
            Candidate cube2 = mockCandidate("cube0002", "cubeB", 2, 2);
            sort(cube1, cube2).assertFirst(cube1);
        }

        {
            Candidate cube1 = mockCandidate("cube0001", "cubeA", 2, 2);
            Candidate cube2 = mockCandidate("cube0002", "cubeB", 2, 2);
            sort(cube1, cube2).assertFirst(cube1);
        }

        {
            Candidate cube1 = mockCandidate("cube0001", "cubeA", 1, 1);
            Candidate cube2 = mockCandidate("cube0002", "cubeB", 2, 2);
            Candidate cube3 = mockCandidate("cube0003", "cubeC", 4, 4);
            sort(cube1, cube2, cube3).assertFirst(cube1);
        }
    }

    private interface SortedCandidate {

        void assertFirst(Candidate candidate);
    }

    private SortedCandidate sort(Candidate... candidates) {
        return candidate -> {
            Arrays.sort(candidates, Candidate.COMPARATOR);
        };
    }

    private Candidate mockCandidate(String cubeId, String cubeName, int cubeCost, int candidateCost) {
        Candidate candidate = new Candidate();
        candidate.realization = mockRealization(cubeId, cubeName, cubeCost);
        CapabilityResult cap = new CapabilityResult();
        cap.setCost(candidateCost);
        candidate.setCapability(cap);
        return candidate;
    }

    private IRealization mockRealization(String cubeId, String cubeName, int cost) {
        return new IRealization() {
            @Override
            public CapabilityResult isCapable(SQLDigest digest) {
                return null;
            }

            @Override
            public RealizationType getType() {
                return null;
            }

            @Override
            public KylinConfig getConfig() {
                return null;
            }

            @Override
            public DataModelDesc getModel() {
                DataModelDesc model = new DataModelDesc();
                model.setName("modelName");
                model.setUuid("model001");
                return model;
            }

            @Override
            public Set<TblColRef> getAllColumns() {
                return null;
            }

            @Override
            public Set<ColumnDesc> getAllColumnDescs() {
                return null;
            }

            @Override
            public List<TblColRef> getAllDimensions() {
                return null;
            }

            @Override
            public List<MeasureDesc> getMeasures() {
                return null;
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public String getName() {
                return cubeName;
            }

            @Override
            public String getCanonicalName() {
                return null;
            }

            @Override
            public long getDateRangeStart() {
                return 0;
            }

            @Override
            public long getDateRangeEnd() {
                return 0;
            }

            @Override
            public boolean supportsLimitPushDown() {
                return false;
            }

            @Override
            public int getCost() {
                return cost;
            }

            @Override
            public int getStorageType() {
                return 0;
            }
        };
    }
}
