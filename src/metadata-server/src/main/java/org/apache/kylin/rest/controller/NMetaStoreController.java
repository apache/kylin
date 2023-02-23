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
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_MODEL_ID;
import static org.apache.kylin.common.exception.ServerErrorCode.FILE_FORMAT_ERROR;
import static org.apache.kylin.common.exception.ServerErrorCode.FILE_NOT_EXIST;
import static org.apache.kylin.rest.request.ModelImportRequest.ImportType.NEW;
import static org.apache.kylin.rest.request.ModelImportRequest.ImportType.OVERWRITE;
import static org.springframework.http.MediaType.MULTIPART_FORM_DATA_VALUE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.DatatypeConverter;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.ZipFileUtils;
import org.apache.kylin.metadata.model.schema.SchemaChangeCheckResult;
import org.apache.kylin.rest.request.MetadataCleanupRequest;
import org.apache.kylin.rest.request.ModelImportRequest;
import org.apache.kylin.rest.request.ModelPreviewRequest;
import org.apache.kylin.rest.request.StorageCleanupRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ModelPreviewResponse;
import org.apache.kylin.rest.service.MetaStoreService;
import org.apache.kylin.tool.util.HashFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.collect.Lists;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/metastore", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class NMetaStoreController extends NBasicController {

    @Autowired
    @Qualifier("metaStoreService")
    private MetaStoreService metaStoreService;

    private static final List<ModelImportRequest.ImportType> IMPORT_TYPE = Lists.newArrayList(NEW, OVERWRITE);

    @ApiOperation(value = "previewModels", tags = { "MID" })
    @GetMapping(value = "/previews/models")
    @ResponseBody
    public EnvelopeResponse<List<ModelPreviewResponse>> previewModels(@RequestParam(value = "project") String project,
            @RequestParam(value = "model_ids", required = false, defaultValue = "") List<String> modeIds) {
        checkProjectName(project);
        List<ModelPreviewResponse> simplifiedModels = metaStoreService.getPreviewModels(project, modeIds);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, simplifiedModels, "");
    }

    @ApiOperation(value = "backupModels", tags = { "MID" })
    @PostMapping(value = "/backup/models", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    @ResponseBody
    public void exportModelMetadata(@RequestParam(value = "project") String project, ModelPreviewRequest request,
            HttpServletResponse response) throws Exception {
        String projectName = checkProjectName(project);
        if (CollectionUtils.isEmpty(request.getIds())) {
            throw new KylinException(EMPTY_MODEL_ID, "At least one model should be selected to export!");
        }
        ByteArrayOutputStream byteArrayOutputStream = metaStoreService.getCompressedModelMetadata(projectName,
                request.getIds(), request.isExportRecommendations(), request.isExportOverProps(),
                request.isExportMultiplePartitionValues());
        String filename;

        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
                byteArrayOutputStream.toByteArray())) {
            byte[] md5 = HashFunction.MD5.checksum(byteArrayInputStream);
            filename = String.format(Locale.ROOT, "%s_model_metadata_%s_%s.zip", projectName.toLowerCase(Locale.ROOT),
                    new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss", Locale.getDefault(Locale.Category.FORMAT))
                            .format(new Date()),
                    DatatypeConverter.printHexBinary(md5));
        }

        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
                byteArrayOutputStream.toByteArray())) {
            setDownloadResponse(byteArrayInputStream, filename, MediaType.APPLICATION_FORM_URLENCODED_VALUE, response);
        }
    }

    @ApiOperation(value = "validationModels", tags = { "MID" })
    @PostMapping(value = "/validation/models")
    @ResponseBody
    public EnvelopeResponse<SchemaChangeCheckResult> uploadAndCheckModelMetadata(
            @RequestParam(value = "project") String project, @RequestPart("file") MultipartFile uploadFile,
            @RequestPart(value = "request", required = false) ModelImportRequest request) throws Exception {
        checkProjectName(project);
        checkUploadFile(uploadFile);

        SchemaChangeCheckResult modelMetadataCheckResponse = metaStoreService.checkModelMetadata(project, uploadFile,
                request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, modelMetadataCheckResponse, "");
    }

    @ApiOperation(value = "uploadModels", tags = { "MID" })
    @PostMapping(value = "/models", consumes = { MULTIPART_FORM_DATA_VALUE })
    @ResponseBody
    public EnvelopeResponse<String> importModelMetadata(@RequestParam(value = "project") String project,
            @RequestPart(value = "file") MultipartFile metadataFile, @RequestPart("request") ModelImportRequest request)
            throws Exception {
        checkProjectName(project);
        checkUploadFile(metadataFile);
        if (request.getModels().stream()
                .noneMatch(modelImport -> IMPORT_TYPE.contains(modelImport.getImportType()))) {
            throw new KylinException(EMPTY_MODEL_ID, "At least one model should be selected to import!");
        }
        metaStoreService.importModelMetadata(project, metadataFile, request);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "cleanupModels", tags = { "SM" })
    @PostMapping(value = "/cleanup")
    @ResponseBody
    public EnvelopeResponse<String> cleanUpMetaStore(@RequestBody MetadataCleanupRequest request) throws Exception {
        String project = request.getProject();
        if (!UnitOfWork.GLOBAL_UNIT.equals(project)) {
            checkProjectName(project);
        }
        metaStoreService.cleanupMeta(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "cleanupStorage", tags = { "SM" })
    @PostMapping(value = "/cleanup_storage")
    @ResponseBody
    public EnvelopeResponse<String> cleanupStorage(@RequestBody StorageCleanupRequest request) throws Exception {

        metaStoreService.cleanupStorage(request.getProjectsToClean(), request.isCleanupStorage());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    private void checkUploadFile(MultipartFile uploadFile) {
        if (Objects.isNull(uploadFile) || uploadFile.isEmpty()) {
            throw new KylinException(FILE_NOT_EXIST, "please select a file");
        }
        if (!ZipFileUtils.validateZipFilename(uploadFile.getOriginalFilename())) {
            throw new KylinException(FILE_FORMAT_ERROR, "upload file must end with .zip");
        }
    }
}
