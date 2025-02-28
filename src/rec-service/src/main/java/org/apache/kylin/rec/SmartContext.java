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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.engine.spark.utils.ComputedColumnEvalUtil;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.util.ExpandableMeasureUtil;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.rec.model.ModelTree;

import lombok.Getter;

@Getter
public class SmartContext extends AbstractContext {

    private final ExpandableMeasureUtil expandableMeasureUtil = new ExpandableMeasureUtil((model, ccDesc) -> {
        String ccExpression = PushDownUtil.massageComputedColumn(model, model.getProject(), ccDesc, null);
        ccDesc.setInnerExpression(ccExpression);
        ComputedColumnEvalUtil.evaluateExprAndType(model, ccDesc);
    });

    public SmartContext(KylinConfig kylinConfig, String project, String[] sqls) {
        super(kylinConfig, project, sqls);
        this.canCreateNewModel = true;
    }

    @Override
    public ModelContext createModelContext(ModelTree modelTree) {
        return new ModelContext(this, modelTree);
    }

    @Override
    public IndexPlan getOriginIndexPlan(String modelId) {
        return NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).getIndexPlan(modelId);
    }

    @Override
    public List<NDataModel> getOriginModels() {
        return NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).listAllModels().stream()
                .filter(model -> !model.isBroken()).collect(Collectors.toList());

    }

    @Override
    public void changeModelMainType(NDataModel model) {
        model.setManagementType(ManagementType.MODEL_BASED);
    }

    @Override
    public String getIdentifier() {
        return "Auto-Modeling";
    }

    @Override
    public void saveMetadata() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            saveModel();
            saveIndexPlan();
            return true;
        }, getProject());
    }

    void saveModel() {
        NDataModelManager dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                getProject());
        for (ModelContext modelCtx : getModelContexts()) {
            if (modelCtx.skipSavingMetadata()) {
                continue;
            }
            NDataModel model = modelCtx.getTargetModel();
            NDataModel updated;
            if (dataModelManager.getDataModelDesc(model.getUuid()) != null) {
                updated = dataModelManager.updateDataModelDesc(model);
            } else {
                updated = dataModelManager.createDataModelDesc(model, model.getOwner());
            }

            // expand measures
            expandableMeasureUtil.deleteExpandableMeasureInternalMeasures(updated);
            expandableMeasureUtil.expandExpandableMeasure(updated);
            updated = dataModelManager.updateDataModelDesc(updated);

            // update and expand index plan as well
            IndexPlan indexPlan = modelCtx.getTargetIndexPlan();
            ExpandableMeasureUtil.expandRuleBasedIndex(indexPlan.getRuleBasedIndex(), updated);
            ExpandableMeasureUtil.expandIndexPlanIndexes(indexPlan, updated);
        }
    }

    private void saveIndexPlan() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(),
                getProject());
        for (ModelContext modelContext : getModelContexts()) {
            if (modelContext.skipSavingMetadata()) {
                continue;
            }
            IndexPlan indexPlan = modelContext.getTargetIndexPlan();
            if (indexPlanManager.getIndexPlan(indexPlan.getUuid()) == null) {
                indexPlanManager.createIndexPlan(indexPlan);
                NDataflow df = dataflowManager.createDataflow(indexPlan, indexPlan.getModel().getOwner());
                dataflowManager.fillDfManually(df,
                        Collections.singletonList(SegmentRange.TimePartitionedSegmentRange.createInfinite()));
            } else {
                indexPlanManager.updateIndexPlan(indexPlan);
            }
        }
    }

    @Override
    public ChainedProposer createProposers() {
        ImmutableList<AbstractProposer> proposers = ImmutableList.of(//
                new SQLAnalysisProposer(this), //
                new ModelSelectProposer(this), //
                new ModelOptProposer(this), //
                new ModelInfoAdjustProposer(this), //
                new ModelRenameProposer(this), //
                new IndexPlanSelectProposer(this), //
                new IndexPlanOptProposer(this), //
                new IndexPlanShrinkProposer(this) //
        );
        return new ChainedProposer(this, proposers);
    }
}
