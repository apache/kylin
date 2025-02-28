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

package org.apache.kylin.rec;

import java.util.List;

import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.model.NDataModel;

public class IndexPlanSelectProposer extends AbstractProposer {

    public IndexPlanSelectProposer(AbstractContext smartContext) {
        super(smartContext);
    }

    @Override
    public void execute() {
        List<AbstractContext.ModelContext> modelContexts = proposeContext.getModelContexts();
        if (modelContexts == null || modelContexts.isEmpty()) {
            return;
        }

        for (AbstractContext.ModelContext modelContext : modelContexts) {
            IndexPlan indexPlan = findExisting(modelContext.getTargetModel());
            if (indexPlan != null) {
                modelContext.setOriginIndexPlan(indexPlan);
                modelContext.setTargetIndexPlan(indexPlan.copy());
            }
        }
    }

    private IndexPlan findExisting(NDataModel model) {
        return model == null ? null : proposeContext.getOriginIndexPlan(model.getUuid());
    }

    @Override
    public String getIdentifierName() {
        return "IndexPlanSelectProposer";
    }
}
