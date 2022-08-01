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

import java.io.IOException;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.EpochCheckBroadcastNotifier;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.request.DiagPackageRequest;
import org.apache.kylin.rest.request.DiagProgressRequest;
import org.apache.kylin.rest.request.MaintenanceModeRequest;
import org.apache.kylin.rest.request.QueryDiagPackageRequest;
import org.apache.kylin.rest.response.DiagStatusResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.MaintenanceModeResponse;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.apache.kylin.rest.response.ServersResponse;
import org.apache.kylin.rest.service.MaintenanceModeService;
import org.apache.kylin.rest.service.MetadataBackupService;
import org.apache.kylin.rest.service.SystemService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.tool.util.ToolUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.annotations.VisibleForTesting;

import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/system", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NSystemController extends NBasicController {

    @Autowired
    @Qualifier("systemService")
    private SystemService systemService;

    @Autowired
    @Qualifier("maintenanceModeService")
    private MaintenanceModeService maintenanceModeService;

    @Autowired
    private ClusterManager clusterManager;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private MetadataBackupService metadataBackupService;

    @VisibleForTesting
    public void setAclEvaluate(AclEvaluate aclEvaluate) {
        this.aclEvaluate = aclEvaluate;
    }

    @VisibleForTesting
    public AclEvaluate getAclEvaluate() {
        return this.aclEvaluate;
    }

    @ApiOperation(value = "dump ke inner metadata responding to system kylinconfig")
    @GetMapping(value = "/metadata/dump")
    @ResponseBody
    public EnvelopeResponse<String> dumpMetadata(@RequestParam(value = "dump_path") String dumpPath) throws Exception {
        String[] args = new String[] { "-backup", "-compress", "-dir", dumpPath };
        metadataBackupService.backup(args);
        return new EnvelopeResponse<>(CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "diag", tags = { "SM" })
    @PostMapping(value = "/diag")
    @ResponseBody
    public EnvelopeResponse<String> getRemoteDumpDiagPackage(
            @RequestParam(value = "host", required = false) String host,
            @RequestBody DiagPackageRequest diagPackageRequest, final HttpServletRequest request) throws Exception {
        validateDataRange(diagPackageRequest.getStart(), diagPackageRequest.getEnd());
        if (StringUtils.isEmpty(host)) {
            String uuid = systemService.dumpLocalDiagPackage(diagPackageRequest.getStart(), diagPackageRequest.getEnd(),
                    diagPackageRequest.getJobId());
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
            @RequestBody QueryDiagPackageRequest queryDiagPackageRequest, final HttpServletRequest request)
            throws Exception {
        if (StringUtils.isEmpty(host)) {
            String uuid = systemService.dumpLocalQueryDiagPackage(queryDiagPackageRequest.getQueryId(),
                    queryDiagPackageRequest.getProject());
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

    @PutMapping(value = "/roll_event_log")
    @ResponseBody
    public EnvelopeResponse<String> rollEventLog() {
        if (ToolUtil.waitForSparderRollUp()) {
            return new EnvelopeResponse<>(CODE_SUCCESS, "", "");
        }
        return new EnvelopeResponse<>(KylinException.CODE_UNDEFINED, "", "Rollup sparder eventLog failed.");
    }

    @ApiOperation(value = "diagStatus", tags = { "SM" })
    @GetMapping(value = "/diag/status")
    @ResponseBody
    public EnvelopeResponse<DiagStatusResponse> getRemotePackageStatus(
            @RequestParam(value = "host", required = false) String host, @RequestParam(value = "id") String id,
            final HttpServletRequest request) throws Exception {
        if (StringUtils.isEmpty(host)) {
            return systemService.getExtractorStatus(id);
        } else {
            String url = host + "/kylin/api/system/diag/status?id=" + id;
            return generateTaskForRemoteHost(request, url);
        }
    }

    @ApiOperation(value = "diagDownload", tags = { "SM" })
    @GetMapping(value = "/diag")
    @ResponseBody
    public void remoteDownloadPackage(@RequestParam(value = "host", required = false) String host,
            @RequestParam(value = "id") String id, final HttpServletRequest request, final HttpServletResponse response)
            throws Exception {
        if (StringUtils.isEmpty(host)) {
            setDownloadResponse(systemService.getDiagPackagePath(id), MediaType.APPLICATION_OCTET_STREAM_VALUE,
                    response);
        } else {
            String url = host + "/kylin/api/system/diag?id=" + id;
            downloadFromRemoteHost(request, url, response);
        }
    }

    @ApiOperation(value = "cancelDiag", tags = { "SM" })
    @DeleteMapping(value = "/diag")
    @ResponseBody
    public EnvelopeResponse<String> remoteStopPackage(@RequestParam(value = "host", required = false) String host,
            @RequestParam(value = "id") String id, final HttpServletRequest request) throws Exception {
        if (StringUtils.isEmpty(host)) {
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
        maintenanceModeService.setMaintenanceMode(maintenanceModeRequest.getReason());
        return new EnvelopeResponse<>(CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "exitMaintenance", tags = { "DW" })
    @DeleteMapping(value = "/maintenance_mode", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<String> unsetReadMode(@RequestParam(value = "reason") String reason) {
        maintenanceModeService.unsetMaintenanceMode(reason);
        EventBusFactory.getInstance().postAsync(new EpochCheckBroadcastNotifier());
        return new EnvelopeResponse<>(CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getMaintenance", tags = { "DW" })
    @GetMapping(value = "/maintenance_mode", produces = { HTTP_VND_APACHE_KYLIN_JSON })
    @ResponseBody
    public EnvelopeResponse<MaintenanceModeResponse> getMaintenanceMode() throws Exception {
        return new EnvelopeResponse<>(CODE_SUCCESS, maintenanceModeService.getMaintenanceMode(), "");
    }

    @ApiOperation(value = "servers", tags = { "DW" })
    @GetMapping(value = "/servers")
    @ResponseBody
    public EnvelopeResponse<ServersResponse> getServers(
            @RequestParam(value = "ext", required = false, defaultValue = "false") boolean ext) {
        val response = new ServersResponse();
        val servers = clusterManager.getServers();
        response.setStatus(maintenanceModeService.getMaintenanceMode());
        if (ext) {
            response.setServers(servers);
        } else {
            response.setServers(servers.stream().map(ServerInfoResponse::getHost).collect(Collectors.toList()));
        }
        return new EnvelopeResponse<>(CODE_SUCCESS, response, "");
    }

    @ApiOperation(value = "host", tags = { "DW" })
    @GetMapping(value = "/host")
    @ResponseBody
    public EnvelopeResponse<String> getHostname() {
        return new EnvelopeResponse<>(CODE_SUCCESS, AddressUtil.getLocalInstance(), "");
    }

    @ApiOperation(value = "reload metadata", tags = { "MID" })
    @PostMapping(value = "/metadata/reload")
    @ResponseBody
    public EnvelopeResponse<String> reloadMetadata() throws IOException {
        systemService.reloadMetadata();
        return new EnvelopeResponse<>(CODE_SUCCESS, "", "");
    }

    //add UnitOfWork simulator
    @PostMapping(value = "/transaction/simulation")
    @ResponseBody
    public EnvelopeResponse<String> simulateUnitOfWork(String project, int seconds) {
        aclEvaluate.checkProjectAdminPermission(project);
        if (KylinConfig.getInstanceFromEnv().isUnitOfWorkSimulationEnabled()) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                long index = 0;
                while (index < seconds) {
                    index++;
                    Thread.sleep(1000L);
                }
                return index;
            }, project);
        }
        return new EnvelopeResponse<>(CODE_SUCCESS, "", "");
    }
}
