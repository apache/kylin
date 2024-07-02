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

import static org.apache.commons.collections.ListUtils.intersection;
import static org.apache.kylin.common.exception.ServerErrorCode.REC_LIST_OUT_OF_DATE;
import static org.apache.kylin.common.exception.ServerErrorCode.UNSUPPORTED_REC_OPERATION_TYPE;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.candidate.RawRecManager;
import org.apache.kylin.metadata.recommendation.ref.CCRef;
import org.apache.kylin.metadata.recommendation.ref.DimensionRef;
import org.apache.kylin.metadata.recommendation.ref.LayoutRef;
import org.apache.kylin.metadata.recommendation.ref.MeasureRef;
import org.apache.kylin.metadata.recommendation.ref.ModelColumnRef;
import org.apache.kylin.metadata.recommendation.ref.OptRecManagerV2;
import org.apache.kylin.metadata.recommendation.ref.OptRecV2;
import org.apache.kylin.metadata.recommendation.ref.RecommendationRef;
import org.apache.kylin.metadata.recommendation.util.RawRecUtil;
import org.apache.kylin.rest.model.FuzzyKeySearcher;
import org.apache.kylin.rest.request.OptRecRequest;
import org.apache.kylin.rest.response.OptRecDepResponse;
import org.apache.kylin.rest.response.OptRecDetailResponse;
import org.apache.kylin.rest.response.OptRecLayoutResponse;
import org.apache.kylin.rest.response.OptRecLayoutsResponse;
import org.apache.kylin.rest.util.PagingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.stereotype.Component;

@EnableDiscoveryClient
@Component("optRecService")
public class OptRecService extends AbstractModelService {
    private static final Logger log = LoggerFactory.getLogger(LogConstant.SMART_CATEGORY);
    public static final int V2 = 2;
    public static final String RECOMMENDATION_SOURCE = "recommendation_source";
    public static final String OPERATION_ERROR_MSG = "The operation types of recommendation includes: add_index, removal_index and all(by default)";
    public static final String ALL = OptRecService.RecActionType.ALL.name();

    @Autowired
    private ModelService modelService;

    public void discard(String project, OptRecRequest request) {
        aclEvaluate.checkProjectOperationPermission(project);
        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(project).loadOptRecV2(request.getModelId());
        RawRecManager rawManager = RawRecManager.getInstance(project);
        Set<Integer> allToHandle = Sets.newHashSet();
        allToHandle.addAll(request.getRecItemsToAddLayout());
        allToHandle.addAll(request.getRecItemsToRemoveLayout());
        rawManager.discardByIds(intersection(optRecV2.getRawIds(), Lists.newArrayList(allToHandle)));
        updateRecommendationCount(project, request.getModelId());
    }

    public void clean(String project, String modelId) {
        aclEvaluate.checkProjectOperationPermission(project);
        OptRecManagerV2 managerV2 = OptRecManagerV2.getInstance(project);
        managerV2.discardAll(modelId);
        updateRecommendationCount(project, modelId);
    }

    private static OptRecDepResponse convert(RecommendationRef ref, Long recCardinality) {
        OptRecDepResponse response = convert(ref);
        response.setCardinality(recCardinality);
        return response;
    }

    private static OptRecDepResponse convert(RecommendationRef ref) {
        OptRecDepResponse response = new OptRecDepResponse();
        response.setVersion(V2);
        response.setContent(ref.getContent());
        response.setName(ref.getName());
        response.setAdd(!ref.isExisted());
        response.setCrossModel(ref.isCrossModel());
        response.setItemId(ref.getId());
        return response;
    }

    public static Long getRecCardinality(RecommendationRef ref, NDataModel model,
            Map<TableRef, TableExtDesc> columnCardinalities) {
        if (ref instanceof DimensionRef) {
            Object entity = ref.getEntity();
            if (entity instanceof NDataModel.NamedColumn) {
                return getCardinality(ref, model, columnCardinalities);
            } else if (entity instanceof ModelColumnRef) {
                ModelColumnRef columnRef = (ModelColumnRef) ref.getEntity();
                return getCardinality(columnRef, model, columnCardinalities);
            }
        }
        return null;
    }

    private static Long getCardinality(RecommendationRef ref, NDataModel model,
            Map<TableRef, TableExtDesc> columnCardinalities) {
        Object entity = ref.getEntity();
        if (entity instanceof NDataModel.NamedColumn) {
            NDataModel.NamedColumn namedColumn = (NDataModel.NamedColumn) entity;
            int columnId = model.getColumnIdByColumnName(namedColumn.getAliasDotColumn());
            TblColRef tblColRef = model.getEffectiveCols().getOrDefault(columnId, null);
            TableRef table = Objects.isNull(tblColRef) ? null : tblColRef.getTableRef();
            TableExtDesc tableExt = columnCardinalities.get(table);
            TableExtDesc.ColumnStats columnStats = Objects.isNull(tableExt) ? null
                    : tableExt.getColumnStatsByName(namedColumn.getName());
            return Objects.isNull(columnStats) ? null : columnStats.getCardinality();
        }
        return null;
    }

    public OptRecDetailResponse validateSelectedRecItems(String project, String modelId,
            List<Integer> recListOfAddLayouts, List<Integer> recListOfRemoveLayouts) {
        aclEvaluate.checkProjectReadPermission(project);

        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(project).loadOptRecV2(modelId);
        OptRecDetailResponse detailResponse = new OptRecDetailResponse();
        List<Integer> layoutRecsToAdd = validate(recListOfAddLayouts, optRecV2, detailResponse, true);
        List<Integer> layoutRecsToRemove = validate(recListOfRemoveLayouts, optRecV2, detailResponse, false);
        detailResponse.setRecItemsToAddLayout(layoutRecsToAdd);
        detailResponse.setRecItemsToRemoveLayout(layoutRecsToRemove);
        return detailResponse;
    }

    public Map<TableRef, TableExtDesc> getModelTables(NDataModel model) {
        Set<TableRef> tables = model.getAllTables();
        NTableMetadataManager tableMetadataManager = getManager(NTableMetadataManager.class, model.getProject());
        Map<TableRef, TableExtDesc> modelTables = Maps.newHashMap();
        for (TableRef table : tables) {
            TableDesc tableDesc = table.getTableDesc();
            TableExtDesc tableExt = tableMetadataManager.getTableExtIfExists(tableDesc);
            modelTables.put(table, tableExt);
        }
        return modelTables;
    }

    private List<Integer> validate(List<Integer> layoutRecList, OptRecV2 optRecV2, OptRecDetailResponse detailResponse,
            boolean isAdd) {
        List<Integer> healthyList = Lists.newArrayList();
        Set<OptRecDepResponse> dimensionRefResponse = Sets.newHashSet();
        Set<OptRecDepResponse> measureRefResponse = Sets.newHashSet();
        Set<OptRecDepResponse> ccRefResponse = Sets.newHashSet();
        NDataModelManager modelManager = getManager(NDataModelManager.class, optRecV2.getProject());
        NDataModel model = modelManager.getDataModelDesc(optRecV2.getUuid());
        Map<TableRef, TableExtDesc> modelTables = getModelTables(model);
        layoutRecList.forEach(recItemId -> {
            LayoutRef layoutRef = checkRecItemIsValidAndReturn(optRecV2, recItemId, isAdd);
            layoutRef.getDependencies().forEach(ref -> {
                if (ref instanceof DimensionRef) {
                    Long recCardinality = getRecCardinality(ref, model, modelTables);
                    OptRecDepResponse optRecDepResponse = OptRecService.convert(ref, recCardinality);
                    dimensionRefResponse.add(optRecDepResponse);
                }
                if (ref instanceof MeasureRef) {
                    measureRefResponse.add(OptRecService.convert(ref));
                }

                for (RecommendationRef innerRef : ref.getDependencies()) {
                    if (innerRef instanceof CCRef) {
                        ccRefResponse.add(OptRecService.convert(innerRef));
                    }
                }
            });
            healthyList.add(recItemId);
        });

        detailResponse.getDimensionItems().addAll(dimensionRefResponse);
        detailResponse.getMeasureItems().addAll(measureRefResponse);
        detailResponse.getCcItems().addAll(ccRefResponse);
        return healthyList;
    }

    public OptRecDetailResponse getSingleOptRecDetail(String project, String modelId, int recItemId, boolean isAdd) {
        aclEvaluate.checkProjectReadPermission(project);

        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(project).loadOptRecV2(modelId);
        OptRecDetailResponse detailResponse = new OptRecDetailResponse();
        List<Integer> validList = validate(Lists.newArrayList(recItemId), optRecV2, detailResponse, isAdd);
        if (isAdd) {
            detailResponse.getRecItemsToAddLayout().addAll(validList);
        } else {
            detailResponse.getRecItemsToRemoveLayout().addAll(validList);
        }
        return detailResponse;
    }

    public static LayoutRef checkRecItemIsValidAndReturn(OptRecV2 optRecV2, int recItemId, boolean isAdd) {
        Set<Integer> allRecItemIds = Sets.newHashSet(optRecV2.getRawIds());
        Set<Integer> brokenRefIds = optRecV2.getBrokenRefIds();
        if (!allRecItemIds.contains(recItemId) || brokenRefIds.contains(recItemId)) {
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.SMART_CATEGORY)) {
                log.info("all recommendation ids {}, broken ref ids {}", allRecItemIds, brokenRefIds);
            }
            throw new KylinException(REC_LIST_OUT_OF_DATE, MsgPicker.getMsg().getRecListOutOfDate());
        }
        Map<Integer, LayoutRef> layoutRefs = isAdd //
                ? optRecV2.getAdditionalLayoutRefs() //
                : optRecV2.getRemovalLayoutRefs();
        LayoutRef layoutRef = layoutRefs.get(-recItemId);
        if (layoutRef == null) {
            throw new KylinException(REC_LIST_OUT_OF_DATE, MsgPicker.getMsg().getRecListOutOfDate());
        }
        return layoutRef;
    }

    public OptRecLayoutsResponse getOptRecLayoutsResponse(String project, String modelId, List<String> recTypeList,
            String key, boolean desc, String orderBy, int offset, int limit) {
        aclEvaluate.checkProjectReadPermission(project);
        Set<RawRecItem.IndexRecType> userDefinedTypes = Sets.newHashSet();
        recTypeList.forEach(type -> {
            if (RawRecItem.IndexRecType.ADD_TABLE_INDEX.name().equalsIgnoreCase(type)) {
                userDefinedTypes.add(RawRecItem.IndexRecType.ADD_TABLE_INDEX);
            } else if (RawRecItem.IndexRecType.ADD_AGG_INDEX.name().equalsIgnoreCase(type)) {
                userDefinedTypes.add(RawRecItem.IndexRecType.ADD_AGG_INDEX);
            } else if (RawRecItem.IndexRecType.REMOVE_AGG_INDEX.name().equalsIgnoreCase(type)) {
                userDefinedTypes.add(RawRecItem.IndexRecType.REMOVE_AGG_INDEX);
            } else {
                userDefinedTypes.add(RawRecItem.IndexRecType.REMOVE_TABLE_INDEX);
            }
        });

        Set<Integer> brokenRecs = Sets.newHashSet();
        List<OptRecLayoutResponse> recList = getRecLayoutResponses(project, modelId, key, OptRecService.ALL,
                brokenRecs);
        if (userDefinedTypes.size() != RawRecItem.IndexRecType.values().length) {
            recList.removeIf(resp -> !userDefinedTypes.isEmpty() && !userDefinedTypes.contains(resp.getType()));
        }
        if (StringUtils.isNotEmpty(orderBy)) {
            recList.sort(BasicService.propertyComparator(orderBy, !desc));
        }
        OptRecLayoutsResponse response = new OptRecLayoutsResponse();
        response.setLayouts(PagingUtil.cutPage(recList, offset, limit));
        response.setSize(recList.size());
        response.setBrokenRecs(brokenRecs);
        return response;
    }

    /**
     * get recommendations by recommendation type:
     * 1. additional, only fetch recommendations can create new layouts on IndexPlan
     * 2. removal, only fetch recommendations can remove layouts of IndexPlan
     * 3. all, fetch all recommendations
     */
    public OptRecLayoutsResponse getOptRecLayoutsResponse(String project, String modelId, String recActionType) {
        return getOptRecLayoutsResponseInner(project, modelId, recActionType);
    }

    public OptRecLayoutsResponse getOptRecLayoutsResponseInner(String project, String modelId, String recActionType) {
        OptRecLayoutsResponse layoutsResponse = new OptRecLayoutsResponse();
        List<OptRecLayoutResponse> responses = getRecLayoutResponses(project, modelId, null, recActionType,
                layoutsResponse.getBrokenRecs());
        layoutsResponse.getLayouts().addAll(responses);
        layoutsResponse.setSize(layoutsResponse.getLayouts().size());
        return layoutsResponse;
    }

    /**
     * get layout to be approved
     */
    public List<RawRecItem> getRecLayout(OptRecV2 optRecV2, RecActionType recActionType) {
        if (optRecV2 == null) {
            return Collections.emptyList();
        }

        Map<Integer, LayoutRef> layoutRefMap = new HashMap<>();
        switch (recActionType) {
        case ALL:
            layoutRefMap.putAll(optRecV2.getAdditionalLayoutRefs());
            layoutRefMap.putAll(optRecV2.getRemovalLayoutRefs());
            break;
        case ADD_INDEX:
            layoutRefMap.putAll(optRecV2.getAdditionalLayoutRefs());
            break;
        case REMOVE_INDEX:
            layoutRefMap.putAll(optRecV2.getRemovalLayoutRefs());
            break;
        default:
            // do nothing
        }

        List<RawRecItem> result = Lists.newArrayList();
        layoutRefMap.forEach((recId, layoutRef) -> {
            if (layoutRef.isBroken() || layoutRef.isExcluded() || layoutRef.isExisted() || recId >= 0) {
                return;
            }
            result.add(optRecV2.getRawRecItemMap().get(-recId));
        });

        return result;
    }

    private List<OptRecLayoutResponse> getRecLayoutResponses(String project, String modelId, String key,
            String recActionType, Set<Integer> brokenRecCollector) {
        List<RawRecItem> rawRecItems = Lists.newArrayList();
        OptRecV2 optRecV2 = OptRecManagerV2.getInstance(project).loadOptRecV2(modelId);
        if (recActionType.equalsIgnoreCase(RecActionType.ALL.name())) {
            rawRecItems.addAll(getRecLayout(optRecV2, RecActionType.ALL));
        } else if (recActionType.equalsIgnoreCase(RecActionType.ADD_INDEX.name())) {
            rawRecItems.addAll(getRecLayout(optRecV2, RecActionType.ADD_INDEX));
        } else if (recActionType.equalsIgnoreCase(RecActionType.REMOVE_INDEX.name())) {
            rawRecItems.addAll(getRecLayout(optRecV2, RecActionType.REMOVE_INDEX));
        } else {
            throw new KylinException(UNSUPPORTED_REC_OPERATION_TYPE, OptRecService.OPERATION_ERROR_MSG);
        }

        brokenRecCollector.addAll(optRecV2.getBrokenRefIds());
        List<RawRecItem> filterRecItems = Lists.newArrayList();
        if (!StringUtils.isBlank(key)) {
            List<RawRecItem> handle = filterRecItems(key, rawRecItems, optRecV2);
            filterRecItems.addAll(handle);
        } else {
            filterRecItems.addAll(rawRecItems);
        }
        return convertToV2RecResponse(project, modelId, filterRecItems, optRecV2);
    }

    private static List<RawRecItem> filterRecItems(String key, List<RawRecItem> rawRecItems, OptRecV2 optRecV2) {
        List<RawRecItem> filterRecItems = Lists.newArrayList();
        Set<String> ccFullNames = FuzzyKeySearcher.searchComputedColumns(optRecV2.getModel(), key);
        Set<Integer> columnRefs = FuzzyKeySearcher.searchColumnRefs(optRecV2, ccFullNames, key);
        Set<Integer> ccRefIds = FuzzyKeySearcher.searchCCRecRefs(optRecV2, key);
        Set<Integer> dependRefs = Sets.newHashSet(ccRefIds);
        dependRefs.addAll(columnRefs);
        Set<Integer> relatedRecIds = FuzzyKeySearcher.searchDependRefIds(optRecV2, dependRefs, key);

        rawRecItems.forEach(recItem -> {
            if (recItem.isRemoveLayoutRec()) {
                final long id = RawRecUtil.getLayout(recItem).getId();
                if (String.valueOf(id).equalsIgnoreCase(key.trim())) {
                    filterRecItems.add(recItem);
                    return;
                }
            }
            for (int dependID : recItem.getDependIDs()) {
                if (relatedRecIds.contains(dependID)) {
                    filterRecItems.add(recItem);
                    return;
                }
            }
        });
        return filterRecItems;
    }

    /**
     * convert RawRecItem response:
     * if type=Layout, layout will be added to IndexPlan when user approves this RawRecItem
     * if type=REMOVAL_LAYOUT, layout will be removed from IndexPlan when user approves this RawRecItem
     */
    private List<OptRecLayoutResponse> convertToV2RecResponse(String project, String modelId, List<RawRecItem> recItems,
            OptRecV2 optRecV2) {
        List<OptRecLayoutResponse> layoutRecResponseList = Lists.newArrayList();

        NDataflowManager dfManager = getManager(NDataflowManager.class, project);
        NDataflow dataflow = dfManager.getDataflow(modelId);
        recItems.forEach(rawRecItem -> {
            boolean isAdd = rawRecItem.getType() == RawRecItem.RawRecType.ADDITIONAL_LAYOUT;
            OptRecLayoutResponse response = new OptRecLayoutResponse();
            response.setId(rawRecItem.getId());
            response.setAdd(isAdd);
            response.setAgg(rawRecItem.isAgg());
            response.setDataSize(-1);
            response.setUsage(rawRecItem.getHitCount());
            response.setType(rawRecItem.getLayoutRecType());
            response.setCreateTime(rawRecItem.getCreateTime());
            response.setLastModified(rawRecItem.getUpdateTime());
            HashMap<String, String> memoInfo = Maps.newHashMap();
            memoInfo.put(OptRecService.RECOMMENDATION_SOURCE, rawRecItem.getRecSource());
            response.setMemoInfo(memoInfo);
            if (!isAdd) {
                long layoutId = RawRecUtil.getLayout(rawRecItem).getId();
                response.setIndexId(layoutId);
                response.setDataSize(dataflow.getByteSize(layoutId));
            }
            OptRecDetailResponse detailResponse = new OptRecDetailResponse();
            List<Integer> validList = validate(Lists.newArrayList(rawRecItem.getId()), optRecV2, detailResponse, isAdd);
            if (isAdd) {
                detailResponse.getRecItemsToAddLayout().addAll(validList);
            } else {
                detailResponse.getRecItemsToRemoveLayout().addAll(validList);
            }
            response.setRecDetailResponse(detailResponse);
            layoutRecResponseList.add(response);
        });
        return layoutRecResponseList;
    }

    public void updateRecommendationCount(String project, Set<String> modelList) {
        if (CollectionUtils.isEmpty(modelList)) {
            return;
        }
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            modelList.forEach(modelId -> updateRecommendationCount(project, modelId));
            return null;
        }, project);
    }

    public void updateRecommendationCount(String project, String modelId) {
        int size = getOptRecLayoutsResponseInner(project, modelId, OptRecService.ALL).getSize();
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            modelService.updateRecommendationsCount(project, modelId, size);
            return null;
        }, project);
    }

    public enum RecActionType {
        ALL, ADD_INDEX, REMOVE_INDEX
    }
}
