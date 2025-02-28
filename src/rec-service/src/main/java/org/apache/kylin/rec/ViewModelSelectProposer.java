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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MapUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.rec.model.GreedyModelTreesBuilder;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ViewModelSelectProposer extends AbstractProposer {

    private final NDataModelManager dataModelManager;

    protected ViewModelSelectProposer(AbstractContext proposeContext) {
        super(proposeContext);
        dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
    }

    @Override
    public void execute() {
        Map<String, Collection<OlapContext>> modelViewOlapContextMap = proposeContext.getModelViewOlapContextMap();
        List<AbstractContext.ModelContext> modelContexts = proposeContext.getModelContexts();
        if (MapUtils.isEmpty(modelViewOlapContextMap) || null == modelContexts) {
            return;
        }
        Map<String, AbstractContext.ModelContext> existedModelContextsMap = modelContexts.stream()
                .filter(e -> null != e.getOriginModel())
                .collect(Collectors.toMap(e -> e.getOriginModel().getAlias(), v -> v, (v1, v2) -> v1));
        Map<String, NDataModel> aliasModelMap = proposeContext.getOriginModels().stream()
                .collect(Collectors.toMap(NDataModel::getAlias, Function.identity()));
        modelViewOlapContextMap.forEach((modelAlias, olapContexts) -> {
            if (existedModelContextsMap.containsKey(modelAlias)) {
                addToExistedModelContext(existedModelContextsMap.get(modelAlias), olapContexts);
            } else {
                NDataModel dataModel = aliasModelMap.get(modelAlias);
                if (null != dataModel) {
                    createNewModelContext(dataModel, olapContexts);
                }
            }
        });
    }

    private void addToExistedModelContext(AbstractContext.ModelContext existedContext,
            Collection<OlapContext> olapContexts) {
        existedContext.getModelTree().getOlapContexts().addAll(olapContexts);
    }

    private void createNewModelContext(NDataModel dataModel, Collection<OlapContext> olapContexts) {
        AbstractContext.ModelContext modelContext = proposeContext.createModelContext(
                new GreedyModelTreesBuilder(KylinConfig.getInstanceFromEnv(), project, proposeContext)
                        .build(olapContexts, dataModel.getRootFactTable().getTableDesc()));
        setModelContextModel(modelContext, dataModel);
        proposeContext.getModelContexts().add(modelContext);
    }

    @Override
    public String getIdentifierName() {
        return "ViewModelSelectProposer";
    }

    private void setModelContextModel(AbstractContext.ModelContext modelContext, NDataModel dataModel) {
        modelContext.setOriginModel(dataModel);
        NDataModel targetModel = dataModelManager.copyBySerialization(dataModel);
        targetModel.init(KylinConfig.getInstanceFromEnv(), project, Lists.newArrayList());
        modelContext.setTargetModel(targetModel);
        targetModel.getComputedColumnDescs().forEach(cc -> modelContext.getUsedCC().put(cc.getExpression(), cc));
    }
}
