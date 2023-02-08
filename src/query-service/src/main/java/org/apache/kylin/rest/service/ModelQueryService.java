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

import static org.apache.kylin.rest.util.ModelTriple.SORT_KEY_CALC_OBJECT;
import static org.apache.kylin.rest.util.ModelTriple.SORT_KEY_DATAFLOW;
import static org.apache.kylin.rest.util.ModelTriple.SORT_KEY_DATA_MODEL;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.FusionModel;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.constant.ModelAttributeEnum;
import org.apache.kylin.rest.service.params.ModelQueryParams;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.ModelTriple;
import org.apache.kylin.rest.util.ModelTripleComparator;
import org.apache.kylin.rest.util.ModelUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;

import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class ModelQueryService extends BasicService implements ModelQuerySupporter {
    public static final String LAST_MODIFIED = "lastModified";
    public static final String USAGE = "usage";
    public static final String STORAGE = "storage";
    public static final String QUERY_HIT_COUNT = "queryHitCount";
    public static final String EXPANSION_RATE = "expansionrate";

    @Autowired
    public AclEvaluate aclEvaluate;

    public List<ModelTriple> getModels(ModelQueryParams queryParam) {
        val projectName = queryParam.getProjectName();
        List<ModelTriple> modelTripleList = matchFirstModels(queryParam);
        modelTripleList = filterModels(modelTripleList, queryParam);
        modelTripleList = sortModels(modelTripleList, projectName, queryParam.getSortBy(), queryParam.isReverse());
        return modelTripleList;
    }

    public List<ModelTriple> matchFirstModels(ModelQueryParams queryParam) {
        val projectName = queryParam.getProjectName();
        val exactMatch = queryParam.isExactMatch();
        val lastModifyFrom = queryParam.getLastModifyFrom();
        val lastModifyTo = queryParam.getLastModifyTo();
        return getManager(NDataflowManager.class, projectName).listAllDataflows(true).parallelStream()
                .map(df -> new ModelTriple(df,
                        df.checkBrokenWithRelatedInfo() ? getBrokenModel(projectName, df.getId()) : df.getModel()))
                .filter(p -> !(Objects.nonNull(lastModifyFrom) && lastModifyFrom > p.getDataModel().getLastModified())
                        && !(Objects.nonNull(lastModifyTo) && lastModifyTo <= p.getDataModel().getLastModified())
                        && (ModelUtils.isArgMatch(queryParam.getModelAliasOrOwner(), exactMatch,
                                p.getDataModel().getAlias())
                                || ModelUtils.isArgMatch(queryParam.getModelAliasOrOwner(), exactMatch,
                                        p.getDataModel().getOwner()))
                        && ModelUtils.isArgMatch(queryParam.getModelAlias(), exactMatch, p.getDataModel().getAlias())
                        && ModelUtils.isArgMatch(queryParam.getOwner(), exactMatch, p.getDataModel().getOwner())
                        && !p.getDataModel().fusionModelBatchPart())
                .collect(Collectors.toList());
    }

    public List<ModelTriple> filterModels(List<ModelTriple> modelTripleList, ModelQueryParams elem) {
        Set<ModelAttributeEnum> modelAttributeSet = Sets
                .newHashSet(elem.getModelAttributes() == null ? Collections.emptyList() : elem.getModelAttributes());

        if (StringUtils.isNotEmpty(elem.getModelId())) {
            modelTripleList.removeIf(t -> !t.getDataModel().getUuid().equals(elem.getModelId()));
        }

        boolean streamingEnabled = KylinConfig.getInstanceFromEnv().streamingEnabled();
        modelTripleList = modelTripleList.parallelStream()
                .filter(triple -> triple.getDataModel().isAccessible(streamingEnabled)) //
                .collect(Collectors.toList());

        if (!modelAttributeSet.isEmpty()) {
            val isProjectEnable = SecondStorageUtil.isProjectEnable(elem.getProjectName());
            modelTripleList = modelTripleList.parallelStream()
                    .filter(t -> filterModelAttribute(t, modelAttributeSet, isProjectEnable))
                    .collect(Collectors.toList());
        }

        return modelTripleList;
    }

    public boolean filterModelAttribute(ModelTriple modelTriple, Set<ModelAttributeEnum> modelAttributeSet,
            boolean isProjectEnable) {
        val modelType = modelTriple.getDataModel().getModelType();
        switch (modelType) {
        case BATCH:
            return modelAttributeSet.contains(ModelAttributeEnum.BATCH)
                    || isMatchSecondStorage(modelTriple, isProjectEnable, modelAttributeSet);
        case HYBRID:
            return modelAttributeSet.contains(ModelAttributeEnum.HYBRID)
                    || isMatchSecondStorage(modelTriple, isProjectEnable, modelAttributeSet);
        case STREAMING:
            return modelAttributeSet.contains(ModelAttributeEnum.STREAMING)
                    || isMatchSecondStorage(modelTriple, isProjectEnable, modelAttributeSet);
        default:
            return false;
        }
    }

    private boolean isMatchSecondStorage(ModelTriple modelTriple, boolean isProjectEnable,
            Set<ModelAttributeEnum> modelAttributeSet) {
        boolean secondStorageMatched = false;
        if (isProjectEnable && modelAttributeSet.contains(ModelAttributeEnum.SECOND_STORAGE)) {
            secondStorageMatched = SecondStorageUtil.isModelEnable(modelTriple.getDataModel().getProject(),
                    modelTriple.getDataModel().getId());
        }
        return secondStorageMatched;
    }

    public List<ModelTriple> sortModels(List<ModelTriple> modelTripleList, String projectName, String sortBy,
            boolean reverse) {
        if (StringUtils.isEmpty(sortBy)) {
            if (getManager(NProjectManager.class).getProject(projectName).isSemiAutoMode()) {
                return modelTripleList.parallelStream()
                        .sorted(new ModelTripleComparator(ModelService.REC_COUNT, !reverse, SORT_KEY_DATA_MODEL))
                        .collect(Collectors.toList());
            } else {
                return modelTripleList.parallelStream()
                        .sorted(new ModelTripleComparator(LAST_MODIFIED, !reverse, SORT_KEY_DATA_MODEL))
                        .collect(Collectors.toList());
            }
        }
        switch (sortBy) {
        case USAGE:
            return modelTripleList.parallelStream()
                    .sorted(new ModelTripleComparator(QUERY_HIT_COUNT, !reverse, SORT_KEY_DATAFLOW))
                    .collect(Collectors.toList());
        case STORAGE:
            return sortByStorage(modelTripleList, projectName, reverse);
        case EXPANSION_RATE:
            return sortByExpansionRate(modelTripleList, projectName, reverse);
        default:
            return modelTripleList.parallelStream()
                    .sorted(new ModelTripleComparator(LAST_MODIFIED, !reverse, SORT_KEY_DATA_MODEL))
                    .collect(Collectors.toList());
        }
    }

    private List<ModelTriple> sortByStorage(List<ModelTriple> tripleList, String projectName, boolean reverse) {
        val dfMgr = getManager(NDataflowManager.class, projectName);
        tripleList.parallelStream().filter(t -> t.getDataModel().isFusionModel()).forEach(t -> {
            BiConsumer<NDataflow, NDataflow> expansionRateFunc = (streamingDataflow, batchDataflow) -> {
                val totalStorageSize = streamingDataflow.getStorageBytesSize() + batchDataflow.getStorageBytesSize();
                t.setCalcObject(totalStorageSize);
            };
            calcOfFusionModel(projectName, t, dfMgr, expansionRateFunc);
        });
        tripleList.parallelStream().filter(t -> !t.getDataModel().isFusionModel())
                .forEach(t -> t.setCalcObject(t.getDataflow().getStorageBytesSize()));

        return tripleList.parallelStream()
                .sorted(new ModelTripleComparator("calcObject", !reverse, SORT_KEY_CALC_OBJECT))
                .collect(Collectors.toList());
    }

    private List<ModelTriple> sortByExpansionRate(List<ModelTriple> tripleList, String projectName, boolean reverse) {
        val dfMgr = getManager(NDataflowManager.class, projectName);
        tripleList.parallelStream().filter(t -> t.getDataModel().isFusionModel()).forEach(t -> {
            BiConsumer<NDataflow, NDataflow> expansionRateFunc = (streamingDataflow, batchDataflow) -> {
                val totalStorageSize = batchDataflow.getStorageBytesSize() + streamingDataflow.getStorageBytesSize();
                val totalSourceSize = batchDataflow.getSourceBytesSize() + streamingDataflow.getSourceBytesSize();
                t.setCalcObject(ModelUtils.computeExpansionRate(totalStorageSize, totalSourceSize));
            };
            calcOfFusionModel(projectName, t, dfMgr, expansionRateFunc);
        });
        tripleList.parallelStream().filter(t -> !t.getDataModel().isFusionModel()).forEach(t -> {
            val dataflow = t.getDataflow();
            t.setCalcObject(
                    ModelUtils.computeExpansionRate(dataflow.getStorageBytesSize(), dataflow.getSourceBytesSize()));
        });
        List<ModelTriple> sorted;
        if (!reverse) {
            sorted = tripleList.stream().sorted(Comparator.comparing(a -> new BigDecimal((String) a.getCalcObject())))
                    .collect(Collectors.toList());
        } else {
            sorted = tripleList.stream().sorted((a, b) -> new BigDecimal((String) b.getCalcObject())
                    .compareTo(new BigDecimal((String) a.getCalcObject()))).collect(Collectors.toList());
        }
        List<ModelTriple> unknownModels = sorted.stream()
                .filter(model -> "-1".equalsIgnoreCase((String) model.getCalcObject())).collect(Collectors.toList());

        List<ModelTriple> models = sorted.stream()
                .filter(model -> !"-1".equalsIgnoreCase((String) model.getCalcObject())).collect(Collectors.toList());
        models.addAll(unknownModels);
        return models;
    }

    private void calcOfFusionModel(String projectName, ModelTriple t, NDataflowManager dfMgr,
            BiConsumer<NDataflow, NDataflow> func) {
        val modelDesc = t.getDataModel();
        FusionModel fusionModel = getManager(FusionModelManager.class, projectName)
                .getFusionModel(modelDesc.getFusionId());

        val batchModel = fusionModel.getBatchModel();
        val streamingDataflow = t.getDataflow();
        val batchDataflow = dfMgr.getDataflow(batchModel.getId());
        func.accept(streamingDataflow, batchDataflow);
    }

    public NDataModel getBrokenModel(String project, String modelId) {
        val model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataModelDescWithoutInit(modelId);
        model.setBroken(true);
        return model;
    }
}
