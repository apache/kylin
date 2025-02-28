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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rest.request.CreateBaseIndexRequest;
import org.apache.kylin.rest.response.BuildBaseIndexResponse;

/**
 * due to complex model semantic update and table reload,
 * base index update work by record current info about baseindex, and after change(table reload..)
 * update base index according diff between previous info and current info.
 */
public class BaseIndexUpdateHelper {

    private long preBaseAggLayout;
    private long preBaseTableLayout;
    private static final long NON_EXIST_LAYOUT = -1;

    private String project;
    private String modelId;
    private List<IndexEntity.Source> updateBaseIndexTypes;
    private boolean needUpdate;

    public BaseIndexUpdateHelper(NDataModel model, boolean createIfNotExist) {
        this(model, updateTypesByFlag(createIfNotExist));
    }

    private static List<IndexEntity.Source> updateTypesByFlag(boolean createIfNotExist) {
        if (createIfNotExist) {
            return Lists.newArrayList(IndexEntity.Source.BASE_AGG_INDEX, IndexEntity.Source.BASE_TABLE_INDEX);
        } else {
            return Lists.newArrayList();
        }
    }

    public BaseIndexUpdateHelper(NDataModel model, List<IndexEntity.Source> updateBaseIndexTypes) {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(),
                model.getProject());
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(model.getId());
        if (!indexPlan.isBroken()) {
            needUpdate = indexPlan.getConfig().isBaseIndexAutoUpdate();
        }

        if (needUpdate) {
            project = model.getProject();
            modelId = model.getId();
            this.updateBaseIndexTypes = updateBaseIndexTypes;
            preBaseAggLayout = getBaseAggLayout();
            preBaseTableLayout = getBaseTableLayout();
        }
    }

    public BuildBaseIndexResponse update(IndexPlanService service) {
        return update(service, true);
    }

    public BuildBaseIndexResponse update(IndexPlanService service, boolean checkProjectOperation) {
        if (!needUpdate) {
            return BuildBaseIndexResponse.EMPTY;
        }
        if (notExist(preBaseAggLayout) && notExist(preBaseTableLayout)
                && !updateBaseIndexTypes.contains(IndexEntity.Source.BASE_TABLE_INDEX)
                && !updateBaseIndexTypes.contains(IndexEntity.Source.BASE_AGG_INDEX)) {
            return BuildBaseIndexResponse.EMPTY;
        }

        long curBaseTableLayout = getBaseTableLayout();
        boolean needCreateBaseTable = updateBaseIndexTypes.contains(IndexEntity.Source.BASE_TABLE_INDEX);
        if (exist(preBaseTableLayout) && notExist(curBaseTableLayout)) {
            needCreateBaseTable = true;
        }

        Long curExistBaseAggLayout = getBaseAggLayout();
        boolean needCreateBaseAgg = updateBaseIndexTypes.contains(IndexEntity.Source.BASE_AGG_INDEX);
        if (exist(preBaseAggLayout) && notExist(curExistBaseAggLayout)) {
            needCreateBaseAgg = true;
        }

        CreateBaseIndexRequest indexRequest = new CreateBaseIndexRequest();
        indexRequest.setModelId(modelId);
        indexRequest.setProject(project);
        BuildBaseIndexResponse response;
        if (checkProjectOperation) {
            response = service.updateBaseIndex(project, indexRequest, needCreateBaseTable, needCreateBaseAgg, true);
        } else {
            response = service.updateBaseIndexInternal(project, indexRequest, needCreateBaseTable, needCreateBaseAgg,
                    true);
        }

        response.judgeIndexOperateType(exist(preBaseAggLayout), true);
        response.judgeIndexOperateType(exist(preBaseTableLayout), false);

        return response;
    }

    private boolean notExist(long layout) {
        return layout == NON_EXIST_LAYOUT;
    }

    private boolean exist(long layout) {
        return layout != NON_EXIST_LAYOUT;
    }

    private long getBaseTableLayout() {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        LayoutEntity layout = indexPlanManager.getIndexPlan(modelId).getBaseTableLayout();
        return layout != null ? layout.getId() : NON_EXIST_LAYOUT;
    }

    private long getBaseAggLayout() {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        LayoutEntity layout = indexPlanManager.getIndexPlan(modelId).getBaseAggLayout();
        return layout != null ? layout.getId() : NON_EXIST_LAYOUT;
    }

}
