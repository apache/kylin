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
import static org.apache.kylin.common.exception.KylinException.CODE_SUCCESS;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.TIME_INVALID_RANGE_NOT_CONSISTENT;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.GET_METADATA_BACKUP_LIST_FAILED;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.helper.MetadataToolHelper;
import org.apache.kylin.metadata.asynctask.MetadataRestoreTask;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.request.DiagPackageRequest;
import org.apache.kylin.rest.request.DiagProgressRequest;
import org.apache.kylin.rest.request.MaintenanceModeRequest;
import org.apache.kylin.rest.request.QueryDiagPackageRequest;
import org.apache.kylin.rest.response.DiagStatusResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.MaintenanceModeResponse;
import org.apache.kylin.rest.response.ServerExtInfoResponse;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.apache.kylin.rest.response.ServersResponse;
import org.apache.kylin.rest.service.MetadataBackupService;
import org.apache.kylin.rest.service.OpsService;
import org.apache.kylin.rest.service.ScheduleService;
import org.apache.kylin.rest.service.SystemService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.tool.HDFSMetadataTool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/system", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpsController extends NBasicController {
    
    private static String DEPRECATED_MAINTENANCE_MODE = "Maintenance mode has been deprecated.";

    @Autowired
    @Qualifier("systemService")
    private SystemService systemService;

    @Autowired
    private ClusterManager clusterManager;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private MetadataBackupService metadataBackupService;

    @Autowired
    private ScheduleService scheduleService;

    @Autowired
    @Qualifier("opsService")
    private OpsService opsService;

    @VisibleForTesting
    public void setAclEvaluate(AclEvaluate aclEvaluate) {
        this.aclEvaluate = aclEvaluate;
    }

    @VisibleForTesting
    public AclEvaluate getAclEvaluate() {
        return this.aclEvaluate;
    }

    private MetadataToolHelper metadataToolHelper = new MetadataToolHelper();

    @ApiOperation(value = "dump ke inner metadata responding to system kylinconfig")
    @GetMapping(value = "/metadata/dump")
    @ResponseBody
    public EnvelopeResponse<String> dumpMetadata(@RequestParam(value = "dump_path") String dumpPath) throws Exception {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        KylinConfig backupConfig = kylinConfig.getMetadataBackupFromSystem() ? kylinConfig
                : KylinConfig.createKylinConfig(kylinConfig);
        metadataToolHelper.backup(backupConfig, null, dumpPath, null, true, false);
        return new EnvelopeResponse<>(CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "/diag", tags = { "SM" })
    @PostMapping(value = "/diag")
    @ResponseBody
    public EnvelopeResponse<String> getRemoteDumpDiagPackage(
            @RequestParam(value = "host", required = false) String host,
            @RequestBody DiagPackageRequest diagPackageRequest, @RequestHeader HttpHeaders headers,
            final HttpServletRequest request) throws Exception {
        host = decodeHost(host);
        AddressUtil.validateHost(host);
        if (StringUtils.isNotBlank(diagPackageRequest.getJobId())) {
            diagPackageRequest.setStart("");
            diagPackageRequest.setEnd("");
        } else {
            if (StringUtils.isBlank(diagPackageRequest.getStart())
                    || StringUtils.isBlank(diagPackageRequest.getEnd())) {
                throw new KylinException(TIME_INVALID_RANGE_NOT_CONSISTENT);
            }
        }
        validateDataRange(diagPackageRequest.getStart(), diagPackageRequest.getEnd());
        if (StringUtils.isEmpty(host) || KylinConfig.getInstanceFromEnv().getMicroServiceMode() != null) {
            String uuid = systemService.dumpLocalDiagPackage(diagPackageRequest.getStart(), diagPackageRequest.getEnd(),
                    diagPackageRequest.getJobId(), diagPackageRequest.getProject(), headers);
            return new EnvelopeResponse<>(CODE_SUCCESS, uuid, "");
        } else {
            String url = host + "/kylin/api/system/diag";
            return generateTaskForRemoteHost(request, url);
        }
    }

    @ApiOperation(value = "queryDiag", tags = { "QE" })
    @PostMapping(value = "/diag/query")
    @ResponseBody
    public EnvelopeResponse<String> getRemoteDumpQueryDiagPackage(
            @RequestParam(value = "host", required = false) String host,
            @RequestBody QueryDiagPackageRequest queryDiagPackageRequest, @RequestHeader HttpHeaders headers,
            final HttpServletRequest request) throws Exception {
        host = decodeHost(host);
        AddressUtil.validateHost(host);
        if (StringUtils.isEmpty(host) || KylinConfig.getInstanceFromEnv().getMicroServiceMode() != null) {
            String uuid = systemService.dumpLocalQueryDiagPackage(queryDiagPackageRequest.getQueryId(),
                    queryDiagPackageRequest.getProject(), headers);
            return new EnvelopeResponse<>(CODE_SUCCESS, uuid, "");
        } else {
            String url = host + "/kylin/api/system/diag/query";
            return generateTaskForRemoteHost(request, url);
        }
    }

    @ApiOperation(value = "diagProgress", tags = { "SM" })
    @PutMapping(value = "/diag/progress")
    @ResponseBody
    public EnvelopeResponse<String> updateDiagProgress(@RequestBody DiagProgressRequest diagProgressRequest) {
        systemService.updateDiagProgress(diagProgressRequest);
        return new EnvelopeResponse<>(CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "diagStatus", tags = { "SM" })
    @GetMapping(value = "/diag/status")
    @ResponseBody
    public EnvelopeResponse<DiagStatusResponse> getRemotePackageStatus(
            @RequestParam(value = "host", required = false) String host, @RequestParam(value = "id") String id,
            @RequestParam(value = "project", required = false) String project, final HttpServletRequest request)
            throws Exception {
        host = decodeHost(host);
        AddressUtil.validateHost(host);
        if (StringUtils.isEmpty(host) || KylinConfig.getInstanceFromEnv().getMicroServiceMode() != null) {
            return systemService.getExtractorStatus(id, project);
        } else {
            String url = host + "/kylin/api/system/diag/status?id=" + id;
            if (StringUtils.isNotEmpty(project)) {
                url = url + "&project=" + project;
            }
            return generateTaskForRemoteHost(request, url);
        }
    }

    @ApiOperation(value = "diagDownload", tags = { "SM" })
    @GetMapping(value = "/diag")
    @ResponseBody
    public void remoteDownloadPackage(@RequestParam(value = "host", required = false) String host,
            @RequestParam(value = "id") String id, @RequestParam(value = "project", required = false) String project,
            final HttpServletRequest request, final HttpServletResponse response) throws IOException {
        host = decodeHost(host);
        AddressUtil.validateHost(host);
        if (StringUtils.isEmpty(host) || KylinConfig.getInstanceFromEnv().getMicroServiceMode() != null) {
            setDownloadResponse(systemService.getDiagPackagePath(id, project), MediaType.APPLICATION_OCTET_STREAM_VALUE,
                    response);
        } else {
            String url = host + "/kylin/api/system/diag?id=" + id;
            if (StringUtils.isNotEmpty(project)) {
                url = url + "&project=" + project;
            }
            downloadFromRemoteHost(request, url, response);
        }
    }

    @ApiOperation(value = "cancelDiag", tags = { "SM" })
    @DeleteMapping(value = "/diag")
    @ResponseBody
    public EnvelopeResponse<String> remoteStopPackage(@RequestParam(value = "host", required = false) String host,
            @RequestParam(value = "id") String id, final HttpServletRequest request) throws Exception {
        host = decodeHost(host);
        AddressUtil.validateHost(host);
        if (StringUtils.isEmpty(host) || KylinConfig.getInstanceFromEnv().getMicroServiceMode() != null) {
            systemService.stopDiagTask(id);
            return new EnvelopeResponse<>(CODE_SUCCESS, "", "");
        } else {
            String url = host + "/kylin/api/system/diag?id=" + id;
            return generateTaskForRemoteHost(request, url);
        }
    }

    @ApiOperation(value = "enterMaintenance", tags = { "DW" })
    @PostMapping(value = "/maintenance_mode", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> setMaintenanceMode(@RequestBody MaintenanceModeRequest maintenanceModeRequest) {
        return new EnvelopeResponse<>(CODE_SUCCESS, "", DEPRECATED_MAINTENANCE_MODE);
    }

    @ApiOperation(value = "exitMaintenance", tags = { "DW" })
    @DeleteMapping(value = "/maintenance_mode", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> unsetReadMode(@RequestParam(value = "reason") String reason) {
        return new EnvelopeResponse<>(CODE_SUCCESS, "", DEPRECATED_MAINTENANCE_MODE);
    }

    @ApiOperation(value = "getMaintenance", tags = { "DW" })
    @GetMapping(value = "/maintenance_mode", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<MaintenanceModeResponse> getMaintenanceMode() {
        return new EnvelopeResponse<>(CODE_SUCCESS, new MaintenanceModeResponse(false, DEPRECATED_MAINTENANCE_MODE),
                DEPRECATED_MAINTENANCE_MODE);
    }

    @ApiOperation(value = "servers", tags = { "DW" })
    @GetMapping(value = "/servers")
    @ResponseBody
    public EnvelopeResponse<ServersResponse> getServers(
            @RequestParam(value = "ext", required = false, defaultValue = "false") boolean ext) {
        ServersResponse response = new ServersResponse();
        List<ServerInfoResponse> servers = clusterManager.getServers();
        response.setStatus(new MaintenanceModeResponse(false, DEPRECATED_MAINTENANCE_MODE));
        if (ext) {
            response.setServers(servers.stream().map(
                    server -> new ServerExtInfoResponse().setServer(server).setSecretName(encodeHost(server.getHost())))
                    .collect(Collectors.toList()));
        } else {
            response.setServers(servers.stream().map(ServerInfoResponse::getHost).collect(Collectors.toList()));
        }
        return new EnvelopeResponse<>(CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "cleanup garbage", tags = { "MID" })
    @PostMapping(value = "/cleanup_garbage")
    @ResponseBody
    public EnvelopeResponse<String> cleanupGarbage(final HttpServletRequest request) throws Exception {
        Pair<String, String> result = scheduleService.triggerAllCleanupGarbage(request);
        return new EnvelopeResponse<>(result.getFirst(), result.getSecond(), "");
    }

    @ApiOperation(value = "backup metadata", tags = { "SM" })
    @PostMapping(value = "/do_metadata_backup")
    @ResponseBody
    public EnvelopeResponse<String> backupMetadata(@RequestBody HashMap request) {
        String project = getProjectStrAndCheckPermission(request);
        String resPath = opsService.backupMetadata(project);
        return new EnvelopeResponse<>(CODE_SUCCESS, resPath, "");
    }

    @ApiOperation(value = "get metadata backup", tags = { "SM" })
    @GetMapping(value = "/get_metadata_backup_list")
    @ResponseBody
    public EnvelopeResponse<String> getMetadataBackupList(
            @RequestParam(value = "project", required = false) String project) {
        project = getProjectStrAndCheckPermission(project);
        Map res = Maps.newHashMap();
        try {
            res.put("metadata_backup_list", opsService.getMetadataBackupList(project));
        } catch (IOException e) {
            throw new KylinException(GET_METADATA_BACKUP_LIST_FAILED, e);
        }
        return new EnvelopeResponse(CODE_SUCCESS, res, "");
    }

    @ApiOperation(value = "cancel backup metadata", tags = { "SM" })
    @PostMapping(value = "/cancel_metadata_backup")
    @ResponseBody
    public EnvelopeResponse<String> cancelBackupMetadata(@RequestBody HashMap request) throws IOException {
        String path = (String) request.get("path");
        String project = getProjectStrAndCheckPermission(request);
        opsService.cancelAndDeleteMetadataBackup(path, project);
        return new EnvelopeResponse<>(CODE_SUCCESS, null, "");
    }

    @ApiOperation(value = "delete metadata backup", tags = { "SM" })
    @PostMapping(value = "/delete_metadata_backup")
    @ResponseBody
    public EnvelopeResponse<String> deleteMetadataBackup(@RequestBody HashMap request) throws Exception {
        List<String> pathList = (List<String>) request.get("path");
        String project = getProjectStrAndCheckPermission(request);
        String msg = opsService.deleteMetadataBackup(pathList, project);
        return new EnvelopeResponse<>(CODE_SUCCESS, null, msg);
    }

    @ApiOperation(value = "restore metadata", tags = { "SM" })
    @PostMapping(value = "/do_restore_metadata")
    @ResponseBody
    public EnvelopeResponse<Map> restoreMetadata(@RequestBody HashMap request) {
        String path = (String) request.get("path");
        String project = getProjectStrAndCheckPermission(request);
        boolean is_truncate = (boolean) request.get("is_truncate");
        String uuid = opsService.restoreMetadata(path, project, is_truncate);
        Map<String, String> res = Maps.newHashMap();
        res.put("restore_task_id", uuid);
        return new EnvelopeResponse(CODE_SUCCESS, res, "");
    }

    @ApiOperation(value = "get metadata backup store dir", tags = { "SM" })
    @GetMapping(value = "/metadata_backup_store_dir")
    @ResponseBody
    public EnvelopeResponse<String> getMetadataBackupStoreDir(
            @RequestParam(value = "project", required = false) String project) {
        project = getProjectStrAndCheckPermission(project);
        String path = opsService.getMetaBackupStoreDir(project);
        Map<String, String> res = Maps.newHashMap();
        res.put("path", path);
        return new EnvelopeResponse(CODE_SUCCESS, res, "");
    }

    @ApiOperation(value = "get metadata restore task status", tags = { "SM" })
    @GetMapping(value = "/metadata_restore_task_status")
    @ResponseBody
    public EnvelopeResponse<String> getMetadataRestoreTaskStatus(
            @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "restore_task_id", required = true) String uuid) {
        project = getProjectStrAndCheckPermission(project);
        MetadataRestoreTask.MetadataRestoreStatus status = opsService.getMetadataRestoreStatus(uuid, project);
        Map<String, Object> res = Maps.newHashMap();
        res.put("status", status);
        return new EnvelopeResponse(CODE_SUCCESS, res, "");
    }

    @ApiOperation(value = "get has metadata restore task in progress", tags = { "SM" })
    @GetMapping(value = "/has_metadata_restore_in_progress")
    @ResponseBody
    public EnvelopeResponse<String> getHasMetadataRestoreTaskInProgress() {
        boolean running = OpsService.MetadataRestore.hasMetadataRestoreRunning();
        Map<String, Object> res = Maps.newHashMap();
        res.put("has_metadata_restore_in_progress", running);
        return new EnvelopeResponse(CODE_SUCCESS, res, "");
    }

    public String getProjectStrAndCheckPermission(HashMap map) {
        String project = (String) map.get("project");
        return getProjectStrAndCheckPermission(project);
    }

    public String getProjectStrAndCheckPermission(String project) {
        if (project == null || project.equals(UnitOfWork.GLOBAL_UNIT)) {
            project = UnitOfWork.GLOBAL_UNIT;
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            ProjectInstance projectInstance = getProject(project);
            project = projectInstance.getName();
            aclEvaluate.checkProjectAdminPermission(project);
        }
        return project;
    }
}
