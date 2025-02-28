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

package org.apache.kylin.rest.service;

import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.request.OpenSqlAccelerateRequest;
import org.apache.kylin.rest.response.OpenAccSqlResponse;
import org.apache.kylin.rest.response.OpenSuggestionResponse;
import org.apache.kylin.rest.response.SuggestAndOptimizedResponse;
import org.apache.kylin.rest.response.SuggestionResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("modelRecService")
public class ModelRecService extends AbstractModelService {
    @Autowired
    private RawRecService rawRecService;

    @Autowired
    private OptRecService optRecService;

    @Autowired
    private ModelSmartService modelSmartService;

    @Autowired
    private ModelService modelService;

    @Autowired
    private IndexPlanService indexPlanService;

    @Autowired
    @Qualifier("modelBuildService")
    private ModelBuildService modelBuildService;

    @Autowired
    public AclEvaluate aclEvaluate;

    @VisibleForTesting
    public void saveRecResult(SuggestionResponse modelSuggestionResponse, String project, String saveIndexesStrategy) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            for (SuggestionResponse.ModelRecResponse response : modelSuggestionResponse.getReusedModels()) {

                NDataModelManager modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                NDataModel model = modelMgr.getDataModelDesc(response.getId());
                BaseIndexUpdateHelper baseIndexUpdater = new BaseIndexUpdateHelper(model,
                        SAVE_INDEXES_STRATEGY.equalsIgnoreCase(saveIndexesStrategy));
                modelMgr.updateDataModel(response.getId(), copyForWrite -> {
                    copyForWrite.setJoinTables(response.getJoinTables());
                    copyForWrite.setComputedColumnDescs(response.getComputedColumnDescs());
                    copyForWrite.setAllNamedColumns(response.getAllNamedColumns());
                    copyForWrite.setAllMeasures(response.getAllMeasures());
                });
                NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                val targetIndexPlan = response.getIndexPlan();
                indexMgr.updateIndexPlan(response.getId(), copyForWrite -> {
                    if (SAVE_INDEXES_STRATEGY.equalsIgnoreCase(saveIndexesStrategy)) {
                        splitIndexesIntoSingleDimIndexes(model, targetIndexPlan);
                    }
                    copyForWrite.setIndexes(targetIndexPlan.getIndexes());
                });
                baseIndexUpdater.update(indexPlanService);
            }
            return null;
        }, project);
    }

    void saveProposedJoinRelations(List<SuggestionResponse.ModelRecResponse> reusedModels, boolean canCreateNewModel,
            String project) {
        if (!canCreateNewModel) {
            return;
        }

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            for (SuggestionResponse.ModelRecResponse response : reusedModels) {
                NDataModelManager modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                modelMgr.updateDataModel(response.getId(), copyForWrite -> {
                    List<JoinTableDesc> newJoinTables = response.getJoinTables();
                    if (newJoinTables.size() != copyForWrite.getJoinTables().size()) {
                        copyForWrite.setJoinTables(newJoinTables);
                        copyForWrite.setAllNamedColumns(response.getAllNamedColumns());
                        copyForWrite.setAllMeasures(response.getAllMeasures());
                        copyForWrite.setComputedColumnDescs(response.getComputedColumnDescs());
                    }
                });
            }
            return null;
        }, project);
    }

    public OpenSuggestionResponse suggestOrOptimizeModels(OpenSqlAccelerateRequest request) {
        SuggestionResponse innerResponse = suggestOptimizeModels(request, false);
        return OpenSuggestionResponse.from(innerResponse, request.getSqls());
    }

    public OpenAccSqlResponse suggestAndOptimizeModels(OpenSqlAccelerateRequest request) {
        SuggestionResponse innerResponse = suggestOptimizeModels(request, true);

        // Since the metric only comes from one table, KE will only recommend a unique model
        String modelName = null;
        String modelId = null;
        if (!CollectionUtils.isEmpty(innerResponse.getNewModels())) {
            modelName = innerResponse.getNewModels().get(0).getAlias();
            modelId = innerResponse.getNewModels().get(0).getUuid();
        } else if (!CollectionUtils.isEmpty(innerResponse.getReusedModels())) {
            modelName = innerResponse.getReusedModels().get(0).getAlias();
            modelId = innerResponse.getReusedModels().get(0).getUuid();
        } else if (!CollectionUtils.isEmpty(innerResponse.getOptimalModels())) {
            modelName = innerResponse.getOptimalModels().get(0).getAlias();
            modelId = innerResponse.getOptimalModels().get(0).getUuid();
        }

        OpenAccSqlResponse openAccSqlResponse = OpenAccSqlResponse.from(innerResponse, request.getSqls());
        openAccSqlResponse.setModel(modelName);

        // Complete index to all exists segments
        if (request.isNeedBuild()) {
            String project = request.getProject();
            NDataflow df = getManager(NDataflowManager.class, project).getDataflow(modelId);

            /*
             * The empty segment that KE exists by default.
             * see org.apache.kylin.rest.request.OpenSqlAccelerateRequest.withEmptySegment
             */
            if (df.getSegments().size() == 1) {
                return openAccSqlResponse;
            }
            modelBuildService.buildIndicesManually(modelId, project, ExecutablePO.DEFAULT_PRIORITY, null, null);
        }

        return openAccSqlResponse;
    }

    public SuggestionResponse suggestOptimizeModels(OpenSqlAccelerateRequest request, boolean createNewModel) {
        SuggestAndOptimizedResponse response = modelSmartService.generateSuggestion(request, createNewModel);
        SuggestionResponse suggestionResponse;
        if (request.isAcceptRecommendation()) {
            suggestionResponse = saveModelAndApproveRecommendations(response, request);
        } else {
            suggestionResponse = saveModelAndRecommendations(response, request);
        }
        return suggestionResponse;
    }

    private SuggestionResponse saveModelAndRecommendations(SuggestAndOptimizedResponse response,
            OpenSqlAccelerateRequest request) {
        SuggestionResponse innerResponse = response.getSuggestionResponse();
        List<ModelRequest> modelRequests = response.getModelRequests();
        boolean canCreateNewModel = response.isCanCreateNewModel();
        String project = response.getProject();
        Set<String> modelIds = response.getModelIds();

        modelService.checkNewModels(request.getProject(), modelRequests);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            if (request.isSaveNewModel()) {
                modelService.saveNewModelsAndIndexes(request.getProject(), modelRequests);
            }
            saveProposedJoinRelations(innerResponse.getReusedModels(), canCreateNewModel, project);
            optRecService.updateRecommendationCount(project, modelIds);
            return null;
        }, request.getProject());
        return innerResponse;
    }

    private SuggestionResponse saveModelAndApproveRecommendations(SuggestAndOptimizedResponse response,
            OpenSqlAccelerateRequest request) {
        SuggestionResponse innerResponse = response.getSuggestionResponse();
        List<ModelRequest> modelRequests = response.getModelRequests();
        modelService.checkNewModels(request.getProject(), modelRequests);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            modelService.saveNewModelsAndIndexes(request.getProject(), request.getAcceptRecommendationStrategy(),
                    modelRequests);
            saveRecResult(innerResponse, request.getProject(), request.getAcceptRecommendationStrategy());
            return null;
        }, request.getProject());

        return innerResponse;
    }

}
