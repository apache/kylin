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

package org.apache.kylin.metadata.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;

/**
 *  Updating cc expression will cause change on both cc and measure.
 *  This UpdateImpact records changed cc's and measure's id during cc modification in
 *  io.kyligence.kap.rest.service.ModelSemanticHelper#updateModelColumns.
 */

@Getter
@Setter
public class UpdateImpact implements Serializable {
    private Set<Integer> removedOrUpdatedCCs = new HashSet<>();

    private Set<Integer> invalidMeasures = new HashSet<>(); // removed measure due to cc update, clear related layouts

    private Set<Integer> invalidRequestMeasures = new HashSet<>(); // removed measure in request not in modelDesc

    private Set<Integer> updatedCCs = new HashSet<>();

    private Set<Integer> updatedMeasures = new HashSet<>();

    private Map<Integer, Integer> replacedMeasures = new HashMap<>(); // need to swap measure id in Agg/TableIndex, layouts

    public UpdateImpact() { // do nothing
    }

    public Set<Integer> getAffectedIds() {
        Set<Integer> affectedId = new HashSet<>();
        affectedId.addAll(updatedCCs);
        affectedId.addAll(updatedMeasures);
        affectedId.addAll(replacedMeasures.values());
        return affectedId;
    }

}
