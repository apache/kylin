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
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.recommendation.candidate.RawRecManager;

public abstract class AbstractSemiContext extends AbstractContext {

    protected AbstractSemiContext(KylinConfig kylinConfig, String project, String[] sqlArray) {
        this(kylinConfig, project, sqlArray, "");
    }

    protected AbstractSemiContext(KylinConfig kylinConfig, String project, String[] sqlArray, String modelName) {
        super(kylinConfig, project, sqlArray, modelName);
        getExistingNonLayoutRecItemMap().putAll(RawRecManager.getInstance(project).queryNonLayoutRecItems(null));
    }

    @Override
    public List<NDataModel> getOriginModels() {
        return getRelatedModels().stream() //
                .filter(model -> getExtraMeta().getOnlineModelIds().contains(model.getUuid()))
                .collect(Collectors.toList());
    }

    @Override
    public IndexPlan getOriginIndexPlan(String modelId) {
        return NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).getIndexPlan(modelId);
    }

    @Override
    public void changeModelMainType(NDataModel model) {
        model.setManagementType(ManagementType.MODEL_BASED);
    }

    @Override
    public void saveMetadata() {
        // do nothing
    }

    @Override
    public String getIdentifier() {
        return "Auto-Recommendation-Version2";
    }
}
