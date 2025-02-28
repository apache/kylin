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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.model.AbstractJoinRule;
import org.apache.kylin.rec.model.ModelTree;

public class ModelSelectContext extends AbstractSemiContext {

    public ModelSelectContext(KylinConfig kylinConfig, String project, String[] sqls) {
        super(kylinConfig, project, sqls);
        this.canCreateNewModel = getSmartConfig().getModelOptRule().equalsIgnoreCase(AbstractJoinRule.APPEND);
        this.partialMatch = getSmartConfig().getKylinConfig().isQueryMatchPartialInnerJoinModel();
        this.partialMatchNonEqui = getSmartConfig().getKylinConfig().partialMatchNonEquiJoins();
    }

    @Override
    public ChainedProposer createProposers() {
        ImmutableList<AbstractProposer> proposers = ImmutableList.of(//
                new SQLAnalysisProposer(this), //
                new ModelSelectProposer(this), //
                new ViewModelSelectProposer(this));
        return new ChainedProposer(this, proposers);
    }

    @Override
    public IndexPlan getOriginIndexPlan(String modelId) {
        throw new NotImplementedException("Fetch origin indexes is forbidden in ModelSelectAIAugmentedContext!");
    }

    @Override
    public List<NDataModel> getOriginModels() {
        return getRelatedModels().stream() //
                .filter(model -> getExtraMeta().getOnlineModelIds().contains(model.getUuid()))
                .collect(Collectors.toList());

    }

    /**
     * For ModelSelectContextOfSemiV2 this method was used for record the relation of sql and model.
     */
    @Override
    public void handleExceptionAfterModelSelect() {
        for (ModelContext modelContext : getModelContexts()) {
            ModelTree modelTree = modelContext.getModelTree();
            if (modelTree == null || CollectionUtils.isEmpty(modelTree.getOlapContexts())) {
                continue;
            }
            NDataModel originModel = modelContext.getOriginModel();
            modelTree.getOlapContexts().forEach(ctx -> {
                AccelerateInfo accelerateInfo = getAccelerateInfoMap().get(ctx.getSql());
                if (originModel != null) {
                    AccelerateInfo.QueryLayoutRelation relation = new AccelerateInfo.QueryLayoutRelation(ctx.getSql(),
                            originModel.getUuid(), -1, originModel.getSemanticVersion());
                    relation.setModelId(originModel.getId());
                    accelerateInfo.getRelatedLayouts().add(relation);
                }
            });
        }
    }

    @Override
    public void changeModelMainType(NDataModel model) {
        throw new NotImplementedException("Modifying ModelMaintainType is forbidden in ModelSelectAIAugmentedContext");
    }
}
