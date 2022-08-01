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

package org.apache.kylin.rest.controller;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.LAYOUT_LIST_EMPTY;

import java.util.List;
import java.util.Set;

import javax.validation.Valid;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;
import org.apache.kylin.rest.request.CreateBaseIndexRequest;
import org.apache.kylin.rest.request.CreateTableIndexRequest;
import org.apache.kylin.rest.request.UpdateRuleBasedCuboidRequest;
import org.apache.kylin.rest.response.AggIndexResponse;
import org.apache.kylin.rest.response.BuildBaseIndexResponse;
import org.apache.kylin.rest.response.BuildIndexResponse;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.DiffRuleBasedIndexResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.FusionRuleDataResult;
import org.apache.kylin.rest.response.IndexGraphResponse;
import org.apache.kylin.rest.response.IndexResponse;
import org.apache.kylin.rest.response.IndexStatResponse;
import org.apache.kylin.rest.response.TableIndexResponse;
import org.apache.kylin.rest.service.FusionIndexService;
import org.apache.kylin.rest.service.IndexPlanService;
import org.apache.kylin.rest.service.ModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.ApiOperation;
import lombok.val;

@RestController
@RequestMapping(value = "/api/index_plans", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NIndexPlanController extends NBasicController {

    private static final String MODEL_ID = "modelId";

    @Autowired
    @Qualifier("indexPlanService")
    private IndexPlanService indexPlanService;

    @Autowired
    @Qualifier("fusionIndexService")
    private FusionIndexService fusionIndexService;

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @ApiOperation(value = "updateRule", tags = { "AI" }, notes = "Update Body: model_id")
    @PutMapping(value = "/rule")
    public EnvelopeResponse<BuildIndexResponse> updateRule(@RequestBody UpdateRuleBasedCuboidRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ID, request.getModelId());
        modelService.validateCCType(request.getModelId(), request.getProject());
        indexPlanService.checkIndexCountWithinLimit(request);
        val response = fusionIndexService.updateRuleBasedCuboid(request.getProject(), request).getSecond();
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "getRule", tags = { "AI" })
    @GetMapping(value = "/rule")
    public EnvelopeResponse<RuleBasedIndex> getRule(@RequestParam("project") String project,
            @RequestParam("model") String modelId) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        val rule = fusionIndexService.getRule(project, modelId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, rule, "");
    }

    @ApiOperation(value = "diffRule", tags = { "AI" })
    @PutMapping(value = "/rule_based_index_diff")
    public EnvelopeResponse<DiffRuleBasedIndexResponse> calculateDiffRuleBasedIndex(
            @RequestBody UpdateRuleBasedCuboidRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ID, request.getModelId());

        val diffRuleBasedIndexResponse = fusionIndexService.calculateDiffRuleBasedIndex(request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, diffRuleBasedIndexResponse, "");
    }

    @ApiOperation(value = "calculateAggIndexCombination", tags = { "AI" }, notes = "Update Body: model_id")
    @PutMapping(value = "/agg_index_count")
    public EnvelopeResponse<AggIndexResponse> calculateAggIndexCombination(
            @RequestBody UpdateRuleBasedCuboidRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ID, request.getModelId());

        val aggIndexCount = fusionIndexService.calculateAggIndexCount(request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, aggIndexCount, "");
    }

    @ApiOperation(value = "createTableIndex", tags = { "AI" }, notes = "Update Body: model_id")
    @PostMapping(value = "/table_index")
    public EnvelopeResponse<BuildIndexResponse> createTableIndex(@Valid @RequestBody CreateTableIndexRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ID, request.getModelId());
        modelService.validateCCType(request.getModelId(), request.getProject());
        val response = fusionIndexService.createTableIndex(request.getProject(), request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "updateTableIndex", tags = { "AI" }, notes = "Update Body: model_id")
    @PutMapping(value = "/table_index")
    public EnvelopeResponse<BuildIndexResponse> updateTableIndex(@Valid @RequestBody CreateTableIndexRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ID, request.getModelId());
        checkRequiredArg("id", request.getId());
        modelService.validateCCType(request.getModelId(), request.getProject());
        val response = fusionIndexService.updateTableIndex(request.getProject(), request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @Deprecated
    @ApiOperation(value = "deleteTableIndex", tags = { "AI" }, notes = "Update URL: {project}, Update Param: project")
    @DeleteMapping(value = "/table_index/{id:.+}")
    public EnvelopeResponse<String> deleteTableIndex(@PathVariable("id") Long id, @RequestParam("model") String modelId,
            @RequestParam("project") String project) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        checkRequiredArg("id", id);
        indexPlanService.removeTableIndex(project, modelId, id);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @Deprecated
    @ApiOperation(value = "getTableIndex", tags = {
            "AI" }, notes = "Update Param: page_offset, page_size; Update response: total_size")
    @GetMapping(value = "/table_index")
    public EnvelopeResponse<DataResult<List<TableIndexResponse>>> getTableIndex(
            @RequestParam(value = "project") String project, //
            @RequestParam(value = "model") String modelId, //
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        val tableIndexs = indexPlanService.getTableIndexs(project, modelId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(tableIndexs, offset, limit), "");
    }

    @ApiOperation(value = "getIndex", tags = { "AI" }, notes = "Update response: total_size")
    @GetMapping(value = "/index")
    public EnvelopeResponse<FusionRuleDataResult<List<IndexResponse>>> getIndex(
            @RequestParam(value = "project") String project, @RequestParam(value = "model") String modelId, //
            @RequestParam(value = "sort_by", required = false, defaultValue = "") String order,
            @RequestParam(value = "reverse", required = false, defaultValue = "false") Boolean desc,
            @RequestParam(value = "sources", required = false, defaultValue = "") List<IndexEntity.Source> sources,
            @RequestParam(value = "key", required = false, defaultValue = "") String key,
            @RequestParam(value = "status", required = false, defaultValue = "") List<IndexEntity.Status> status,
            @RequestParam(value = "ids", required = false, defaultValue = "") List<Long> ids,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "range", required = false, defaultValue = "") List<IndexEntity.Range> range) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        val indexes = fusionIndexService.getIndexes(project, modelId, key, status, order, desc, sources, ids, range);
        val indexUpdateEnabled = FusionIndexService.checkUpdateIndexEnabled(project, modelId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                FusionRuleDataResult.get(indexes, offset, limit, indexUpdateEnabled), "");
    }

    @ApiOperation(value = "indexGraph", tags = { "AI" })
    @GetMapping(value = "/index_graph")
    public EnvelopeResponse<IndexGraphResponse> getIndexGraph(@RequestParam(value = "project") String project,
            @RequestParam(value = "model") String modelId, //
            @RequestParam(value = "order", required = false, defaultValue = "100") Integer size) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        val indexes = indexPlanService.getIndexGraph(project, modelId, size);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, indexes, "");
    }

    @ApiOperation(value = "deleteIndex", tags = { "AI" }, notes = "Update response: need to update total_size")
    @DeleteMapping(value = "/index/{layout_id:.+}")
    public EnvelopeResponse<String> deleteIndex(@PathVariable(value = "layout_id") long layoutId,
            @RequestParam(value = "project") String project, //
            @RequestParam(value = "model") String modelId, @RequestParam("index_range") IndexEntity.Range indexRange) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        fusionIndexService.removeIndex(project, modelId, layoutId, indexRange);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "batch deleteIndex", tags = { "AI" })
    @DeleteMapping(value = "/index")
    public EnvelopeResponse<String> batchDeleteIndex(@RequestParam(value = "layout_ids") Set<Long> layoutIds,
            @RequestParam(value = "project") String project, @RequestParam(value = "model") String modelId) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        if (CollectionUtils.isEmpty(layoutIds)) {
            throw new KylinException(LAYOUT_LIST_EMPTY);
        }
        fusionIndexService.removeIndexes(project, modelId, layoutIds);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "create base index", tags = { "AI" })
    @PostMapping(value = "/base_index")
    @ResponseBody
    public EnvelopeResponse<BuildBaseIndexResponse> createBaseIndex(@RequestBody CreateBaseIndexRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ID, request.getModelId());
        val response = indexPlanService.createBaseIndex(request.getProject(), request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "update base index", tags = { "AI" })
    @PutMapping(value = "/base_index")
    @ResponseBody
    public EnvelopeResponse<BuildBaseIndexResponse> updateBaseIndex(@RequestBody CreateBaseIndexRequest request) {
        checkProjectName(request.getProject());
        checkRequiredArg(MODEL_ID, request.getModelId());
        val response = indexPlanService.updateBaseIndex(request.getProject(), request, false);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "getIndex", tags = { "AI" })
    @GetMapping(value = "/index_stat")
    public EnvelopeResponse<IndexStatResponse> getIndexStat(@RequestParam(value = "project") String project,
            @RequestParam(value = "model_id") String modelId) {
        checkProjectName(project);
        checkRequiredArg(MODEL_ID, modelId);
        val response = indexPlanService.getStat(project, modelId);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }
}
