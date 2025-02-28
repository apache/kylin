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

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.relnode.OlapContext;

import lombok.val;

public class CandidateTestUtils {

    static Candidate mockCandidate(String modelId, String modelName, int realizationCost, double candidateCost) {
        return mockCandidate(modelId, modelName, realizationCost, candidateCost, 0);
    }

    static Candidate mockCandidate(String modelId, String modelName, int realizationCost, double candidateCost,
            int unmatchedColSize) {
        IRealization realization = mockRealization(modelId, modelName, realizationCost);
        OlapContext olapContext = mockOlapContext();
        val candidate = new Candidate(realization, olapContext, Maps.newHashMap());
        val cap = new CapabilityResult();
        cap.setSelectedCandidate(() -> candidateCost);
        cap.setCost(cap.getSelectedCandidate().getCost());
        cap.setLayoutUnmatchedColsSize(unmatchedColSize);
        candidate.setCapability(cap);
        return candidate;
    }

    static Candidate mockCandidate(String modelId, String modelName, double candidateCost, boolean partialResult) {
        IRealization realization = mockRealization(modelId, modelName, 1);
        OlapContext olapContext = mockOlapContext();
        val candidate = new Candidate(realization, olapContext, Maps.newHashMap());
        val cap = new CapabilityResult();
        cap.setSelectedCandidate(() -> candidateCost);
        cap.setCost(cap.getSelectedCandidate().getCost());
        cap.setLayoutUnmatchedColsSize(0);
        cap.setPartialResult(partialResult);
        candidate.setCapability(cap);
        return candidate;
    }

    static OlapContext mockOlapContext() {
        return new OlapContext(-1);
    }

    static IRealization mockRealization(String modelId, String modelName, int cost) {
        return new NDataflow() {
            @Override
            public NDataModel getModel() {
                val model = new NDataModel();
                model.setAlias(modelName);
                model.setUuid(modelId);
                return model;
            }

            @Override
            public boolean isOnline() {
                return true;
            }

            @Override
            public int getCost() {
                return cost;
            }
        };
    }

    static String[] mockModelPriorityValues(String[] arr) {
        return Arrays.stream(arr).map(StringUtils::upperCase).toArray(String[]::new);
    }
}
