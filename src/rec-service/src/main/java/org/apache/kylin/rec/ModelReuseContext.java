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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.rec.common.AccelerateInfo;

public class ModelReuseContext extends AbstractSemiContext {

    public ModelReuseContext(KylinConfig kylinConfig, String project, String[] sqlArray) {
        this(kylinConfig, project, sqlArray, "");
    }

    public ModelReuseContext(KylinConfig kylinConfig, String project, String[] sqlArray, boolean canCreateNewModel) {
        this(kylinConfig, project, sqlArray);
        this.canCreateNewModel = canCreateNewModel;
    }

    public ModelReuseContext(KylinConfig kylinConfig, String project, String[] sqlArray, String modelName) {
        super(kylinConfig, project, sqlArray, modelName);
        this.partialMatch = getSmartConfig().getKylinConfig().isQueryMatchPartialInnerJoinModel();
        this.partialMatchNonEqui = getSmartConfig().getKylinConfig().partialMatchNonEquiJoins();
    }

    @Override
    public ChainedProposer createProposers() {
        return new ChainedProposer(this, ImmutableList.of(//
                new SQLAnalysisProposer(this), //
                new ModelSelectProposer(this), //
                new ViewModelSelectProposer(this), //
                new ModelOptProposer(this), //
                new ModelInfoAdjustProposer(this), //
                new ModelRenameProposer(this), //
                new IndexPlanSelectProposer(this), //
                new IndexPlanOptProposer(this), //
                new IndexPlanShrinkProposer(this) //
        ));
    }

    @Override
    public void handleExceptionAfterModelSelect() {
        if (isCanCreateNewModel()) {
            return;
        }

        getModelContexts().forEach(modelCtx -> {
            if (modelCtx.isTargetModelMissing()) {
                modelCtx.getModelTree().getOlapContexts().forEach(olapContext -> {
                    AccelerateInfo accelerateInfo = getAccelerateInfoMap().get(olapContext.getSql());
                    accelerateInfo.setPendingMsg(ModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG);
                });
            }
        });
    }
}
