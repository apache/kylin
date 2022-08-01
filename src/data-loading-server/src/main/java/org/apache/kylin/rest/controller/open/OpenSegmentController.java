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

package org.apache.kylin.rest.controller.open;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NAME_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_MULTI_PARTITION_DISABLE;

import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.controller.BaseController;
import org.apache.kylin.rest.controller.SegmentController;
import org.apache.kylin.rest.request.BuildIndexRequest;
import org.apache.kylin.rest.request.BuildSegmentsRequest;
import org.apache.kylin.rest.request.CheckSegmentRequest;
import org.apache.kylin.rest.request.IndexesToSegmentsRequest;
import org.apache.kylin.rest.request.PartitionsBuildRequest;
import org.apache.kylin.rest.request.PartitionsRefreshRequest;
import org.apache.kylin.rest.request.SegmentsRequest;
import org.apache.kylin.rest.response.BuildIndexResponse;
import org.apache.kylin.rest.response.CheckSegmentResponse;
import org.apache.kylin.rest.response.JobInfoResponse;
import org.apache.kylin.rest.response.JobInfoResponseWithFailure;
import org.apache.kylin.rest.response.NDataSegmentResponse;
import org.apache.kylin.rest.response.SegmentPartitionResponse;
import org.apache.kylin.rest.service.FusionModelService;
import org.apache.kylin.rest.service.ModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/models", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenSegmentController extends BaseController {

    @Autowired
    private SegmentController segmentController;

    @Autowired
    private ModelService modelService;

    @Autowired
    private FusionModelService fusionModelService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @VisibleForTesting
    public NDataModel getModel(String modelAlias, String project) {
        NDataModel model = modelService.getManager(NDataModelManager.class, project).listAllModels().stream() //
                .filter(dataModel -> dataModel.getUuid().equals(modelAlias) //
                        || dataModel.getAlias().equalsIgnoreCase(modelAlias))
                .findFirst().orElse(null);

        if (model == null) {
            throw new KylinException(MODEL_NAME_NOT_EXIST, modelAlias);
        }
        if (model.isBroken()) {
            throw new KylinException(ServerErrorCode.MODEL_BROKEN,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getBrokenModelOperationDenied(), modelAlias));
        }
        return model;
    }

    @ApiOperation(value = "getSegments", tags = { "AI" })
    @GetMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<NDataSegmentResponse>>> getSegments(
            @PathVariable(value = "model_name") String modelAlias, //
            @RequestParam(value = "project") String project, //
            @RequestParam(value = "status", required = false) String status, //
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "start", required = false, defaultValue = "1") String start,
            @RequestParam(value = "end", required = false, defaultValue = "" + (Long.MAX_VALUE - 1)) String end,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modified_time") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "false") Boolean reverse) {
        String projectName = checkProjectName(project);
        String modelId = getModel(modelAlias, projectName).getUuid();
        return segmentController.getSegments(modelId, projectName, status, offset, limit, start, end, null, null, false,
                sortBy, reverse);
    }

    @ApiOperation(value = "getMultiPartitions", tags = { "DW" })
    @GetMapping(value = "/{model_name:.+}/segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<SegmentPartitionResponse>>> getMultiPartitions(
            @PathVariable(value = "model_name") String modelAlias, //
            @RequestParam(value = "project") String project, //
            @RequestParam("segment_id") String segId,
            @RequestParam(value = "status", required = false) List<String> status,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset, //
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "sort_by", required = false, defaultValue = "last_modify_time") String sortBy,
            @RequestParam(value = "reverse", required = false, defaultValue = "true") Boolean reverse) {
        String projectName = checkProjectName(project);
        String modelId = getModel(modelAlias, project).getId();
        return segmentController.getMultiPartition(modelId, projectName, segId, status, pageOffset, pageSize, sortBy,
                reverse);
    }

    @ApiOperation(value = "buildSegmentsManually", tags = { "DW" })
    @PostMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> buildSegmentsManually(@PathVariable("model_name") String modelAlias,
            @RequestBody BuildSegmentsRequest buildSegmentsRequest) throws Exception {
        String projectName = checkProjectName(buildSegmentsRequest.getProject());
        buildSegmentsRequest.setProject(projectName);
        validatePriority(buildSegmentsRequest.getPriority());
        String modelId = getModel(modelAlias, buildSegmentsRequest.getProject()).getUuid();
        return segmentController.buildSegmentsManually(modelId, buildSegmentsRequest);
    }

    @ApiOperation(value = "refreshOrMergeSegments", tags = { "DW" })
    @PutMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> refreshOrMergeSegments(@PathVariable("model_name") String modelAlias,
            @RequestBody SegmentsRequest request) {
        String projectName = checkProjectName(request.getProject());
        request.setProject(projectName);
        validatePriority(request.getPriority());
        String modelId = getModel(modelAlias, request.getProject()).getUuid();
        return segmentController.refreshOrMergeSegments(modelId, request);
    }

    @ApiOperation(value = "deleteSegments", tags = { "DW" })
    @DeleteMapping(value = "/{model_name:.+}/segments")
    @ResponseBody
    public EnvelopeResponse<String> deleteSegments(@PathVariable("model_name") String modelAlias,
            @RequestParam("project") String project, //
            @RequestParam("purge") Boolean purge, //
            @RequestParam(value = "force", required = false, defaultValue = "false") boolean force, //
            @RequestParam(value = "ids", required = false) String[] ids, //
            @RequestParam(value = "names", required = false) String[] names) {
        String projectName = checkProjectName(project);
        if (purge) {
            ids = new String[0];
        }
        String modelId = getModel(modelAlias, projectName).getUuid();
        return segmentController.deleteSegments(modelId, projectName, purge, force, ids, names);
    }

    @ApiOperation(value = "completeSegments", tags = { "DW" })
    @PostMapping(value = "/{model_name}/segments/completion")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponseWithFailure> completeSegments(@PathVariable("model_name") String modelAlias,
            @RequestParam(value = "project") String project,
            @RequestParam(value = "parallel", required = false, defaultValue = "false") boolean parallel,
            @RequestParam(value = "ids", required = false) String[] ids,
            @RequestParam(value = "names", required = false) String[] names,
            @RequestParam(value = "batch_index_ids", required = false) List<Long> batchIndexIds,
            @RequestParam(value = "partial_build", required = false, defaultValue = "false") boolean partialBuild,
            @RequestParam(value = "priority", required = false, defaultValue = "3") Integer priority,
            @RequestParam(value = "yarn_queue", required = false) String yarnQueue,
            @RequestParam(value = "tag", required = false) Object tag) {
        String projectName = checkProjectName(project);
        checkSegmentParams(ids, names);
        String modelId = getModel(modelAlias, projectName).getUuid();
        Pair<String, String[]> pair = fusionModelService.convertSegmentIdWithName(modelId, projectName, ids, names);
        IndexesToSegmentsRequest req = new IndexesToSegmentsRequest();
        req.setProject(projectName);
        req.setParallelBuildBySegment(parallel);
        req.setSegmentIds(Lists.newArrayList(pair.getSecond()));
        req.setPartialBuild(partialBuild);
        req.setIndexIds(batchIndexIds);
        req.setPriority(priority);
        req.setYarnQueue(yarnQueue);
        req.setTag(tag);
        return segmentController.addIndexesToSegments(pair.getFirst(), req);
    }

    @ApiOperation(value = "buildIndicesManually", tags = { "AI" })
    @PostMapping(value = "/{model_name:.+}/indexes")
    @ResponseBody
    public EnvelopeResponse<BuildIndexResponse> buildIndicesManually(@PathVariable("model_name") String modelAlias,
            @RequestBody BuildIndexRequest request) {
        String projectName = checkProjectName(request.getProject());
        request.setProject(projectName);
        validatePriority(request.getPriority());
        String modelId = getModel(modelAlias, request.getProject()).getId();
        return segmentController.buildIndicesManually(modelId, request);
    }

    @ApiOperation(value = "checkSegments", tags = { "DW" })
    @PostMapping(value = "/{model:.+}/segments/check")
    @ResponseBody
    public EnvelopeResponse<CheckSegmentResponse> checkSegments(@PathVariable("model") String modelAlias,
            @RequestBody CheckSegmentRequest request) {
        String projectName = checkProjectName(request.getProject());
        request.setProject(projectName);
        aclEvaluate.checkProjectOperationPermission(request.getProject());
        checkRequiredArg("start", request.getStart());
        checkRequiredArg("end", request.getEnd());
        validateDataRange(request.getStart(), request.getEnd());
        NDataModel model = getModel(modelAlias, projectName);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelService.checkSegments(request.getProject(),
                model.getAlias(), request.getStart(), request.getEnd()), "");
    }

    @ApiOperation(value = "build multi_partition", tags = { "DW" })
    @PostMapping(value = "/{model_name:.+}/segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> buildMultiPartition(@PathVariable("model_name") String modelAlias,
            @RequestBody PartitionsBuildRequest param) {
        String projectName = checkProjectName(param.getProject());
        checkProjectMLP(projectName);
        param.setProject(projectName);
        val modelId = getModel(modelAlias, param.getProject()).getId();
        return segmentController.buildMultiPartition(modelId, param);
    }

    @ApiOperation(value = "refresh multi_partition", tags = { "DW" })
    @PutMapping(value = "/{model_name:.+}/segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<JobInfoResponse> refreshMultiPartition(@PathVariable("model_name") String modelAlias,
            @RequestBody PartitionsRefreshRequest param) {
        String projectName = checkProjectName(param.getProject());
        checkProjectMLP(projectName);
        param.setProject(projectName);
        val modelId = getModel(modelAlias, param.getProject()).getId();
        return segmentController.refreshMultiPartition(modelId, param);
    }

    @ApiOperation(value = "delete multi_partition", tags = { "DW" })
    @DeleteMapping(value = "/segments/multi_partition")
    @ResponseBody
    public EnvelopeResponse<String> deleteMultiPartition(@RequestParam("model") String modelAlias,
            @RequestParam("project") String project, @RequestParam("segment_id") String segmentId,
            @RequestParam("sub_partition_values") List<String[]> subPartitionValues) {
        String projectName = checkProjectName(project);
        checkProjectMLP(projectName);
        checkRequiredArg("sub_partition_values", subPartitionValues);
        checkMLP("sub_partition_values", subPartitionValues);
        NDataModel model = getModel(modelAlias, projectName);
        modelService.deletePartitionsByValues(project, segmentId, model.getId(), subPartitionValues);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    private void checkProjectMLP(String project) {
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(project);
        if (!projectInstance.getConfig().isMultiPartitionEnabled()) {
            throw new KylinException(PROJECT_MULTI_PARTITION_DISABLE, projectInstance.getName());
        }
    }

    static void checkMLP(String fieldName, List<String[]> subPartitionValues) {
        if (subPartitionValues.isEmpty()) {
            throw new KylinException(INVALID_PARAMETER, String.format(Locale.ROOT, "'%s' cannot be empty.", fieldName));
        }
    }
}
