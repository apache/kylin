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
import java.util.Map;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.model.NDataModel;

public class RealizationCheck {
    private final Map<NDataModel, List<IncapableReason>> modelIncapableReasons = Maps.newHashMap();
    private final Map<NDataModel, Map<String, String>> capableModels = Maps.newHashMap();

    public Map<NDataModel, List<IncapableReason>> getModelIncapableReasons() {
        return modelIncapableReasons;
    }

    public void addModelIncapableReason(NDataModel modelDesc, IncapableReason reason) {
        if (!modelIncapableReasons.containsKey(modelDesc)) {
            modelIncapableReasons.put(modelDesc, Lists.newArrayList(reason));
        } else {
            List<IncapableReason> incapableReasons = modelIncapableReasons.get(modelDesc);
            if (!incapableReasons.contains(reason))
                incapableReasons.add(reason);
        }
    }

    public void addCapableModel(NDataModel modelDesc, Map<String, String> aliasMap) {
        if (!this.capableModels.containsKey(modelDesc))
            this.capableModels.put(modelDesc, aliasMap);
    }

    public enum IncapableReason {
        MODEL_UNMATCHED_JOIN, //
        MODEL_BAD_JOIN_SEQUENCE
    }
}
