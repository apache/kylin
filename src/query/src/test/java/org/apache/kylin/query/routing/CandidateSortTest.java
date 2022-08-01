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

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.model.NDataModel;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class CandidateSortTest {

    @Test
    public void testModelHintCandidateSort() {
        try (QueryContext queryContext = QueryContext.current()) {
            {
                queryContext.setModelPriorities(new String[] {});
                val model1 = mockCandidate("model0001", "modelA", 1, 1);
                val model2 = mockCandidate("model0002", "modelB", 2, 2);
                sort(model1, model2).assertFirst(model1);
            }

            {
                queryContext.setModelPriorities(new String[] { "MODELB" });
                val model1 = mockCandidate("model0001", "modelA", 1, 1);
                val model2 = mockCandidate("model0002", "modelB", 2, 2);
                sort(model1, model2).assertFirst(model2);
            }

            {
                queryContext.setModelPriorities(new String[] { "MODELB", "MODELA" });
                val model1 = mockCandidate("model0001", "modelA", 1, 1);
                val model2 = mockCandidate("model0002", "modelB", 2, 2);
                sort(model1, model2).assertFirst(model2);
            }

            {
                queryContext.setModelPriorities(new String[] { "MODELC", "MODELA" });
                val model1 = mockCandidate("model0001", "modelA", 1, 1);
                val model2 = mockCandidate("model0002", "modelB", 2, 2);
                val model3 = mockCandidate("model0003", "modelC", 4, 4);
                sort(model1, model2, model3).assertFirst(model3);
            }
        }
    }

    @Test
    public void testSort() {
        {
            val model1 = mockCandidate("model0001", "modelA", 1, 1);
            val model2 = mockCandidate("model0002", "modelB", 2, 2);
            sort(model1, model2).assertFirst(model1);
        }

        {
            val model1 = mockCandidate("model0001", "modelA", 2, 1);
            val model2 = mockCandidate("model0002", "modelB", 2, 2);
            sort(model1, model2).assertFirst(model1);
        }

        {
            val model1 = mockCandidate("model0001", "modelA", 2, 2);
            val model2 = mockCandidate("model0002", "modelB", 2, 2);
            sort(model1, model2).assertFirst(model1);
        }

        {
            val model1 = mockCandidate("model0001", "modelA", 1, 1);
            val model2 = mockCandidate("model0002", "modelB", 2, 2);
            val model3 = mockCandidate("model0003", "modelC", 4, 4);
            sort(model1, model2, model3).assertFirst(model1);
        }
    }

    private interface SortedCandidate {

        void assertFirst(Candidate candidate);
    }

    private SortedCandidate sort(Candidate... candidates) {
        return candidate -> {
            Arrays.sort(candidates, Candidate.COMPARATOR);
            Assert.assertEquals(candidate.getRealization().getModel().getAlias(),
                    candidates[0].getRealization().getModel().getAlias());
        };
    }

    private Candidate mockCandidate(String modelId, String modelName, int modelCost, double candidateCost) {
        val candidate = new Candidate();
        candidate.realization = mockRealization(modelId, modelName, modelCost);
        val cap = new CapabilityResult();
        cap.setSelectedCandidate(() -> candidateCost);
        candidate.setCapability(cap);
        return candidate;
    }

    private IRealization mockRealization(String modelId, String modelName, int cost) {
        return new IRealization() {
            @Override
            public CapabilityResult isCapable(SQLDigest digest, List<NDataSegment> prunedSegments) {
                return null;
            }

            @Override
            public CapabilityResult isCapable(SQLDigest digest, List<NDataSegment> prunedSegments,
                    List<NDataSegment> prunedStreamingSegments) {
                return null;
            }

            @Override
            public String getType() {
                return null;
            }

            @Override
            public KylinConfig getConfig() {
                return null;
            }

            @Override
            public NDataModel getModel() {
                val model = new NDataModel();
                model.setAlias(modelName);
                model.setUuid(modelId);
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
            public List<IRealization> getRealizations() {
                return null;
            }

            @Override
            public FunctionDesc findAggrFunc(FunctionDesc aggrFunc) {
                return null;
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public String getUuid() {
                return null;
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
            public int getCost() {
                return cost;
            }

            @Override
            public boolean hasPrecalculatedFields() {
                return false;
            }

            @Override
            public int getStorageType() {
                return 0;
            }

            @Override
            public boolean isStreaming() {
                return false;
            }

            @Override
            public String getProject() {
                return null;
            }
        };
    }

}
