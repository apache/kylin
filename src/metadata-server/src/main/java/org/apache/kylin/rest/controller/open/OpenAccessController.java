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
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.UNAUTHORIZED_ENTITY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PARAMETER_INVALID_SUPPORT_LIST;
import static org.apache.kylin.rest.aspect.InsensitiveNameAspect.getCaseInsentiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.request.AccessRequest;
import org.apache.kylin.rest.request.BatchProjectPermissionRequest;
import org.apache.kylin.rest.request.ProjectPermissionRequest;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ProjectPermissionResponse;
import org.apache.kylin.rest.response.SidPermissionWithAclResponse;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.security.AclPermissionEnum;
import org.apache.kylin.rest.security.ExternalAclProvider;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.AclTCRService;
import org.apache.kylin.rest.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/access", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenAccessController extends NBasicController {
    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("aclTCRService")
    private AclTCRService aclTCRService;

    @Autowired
    @Qualifier("userService")
    protected UserService userService;

    @ApiOperation(value = "getProjectAccessPermissions", tags = { "MID" })
    @GetMapping(value = "/project")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<ProjectPermissionResponse>>> getProjectAccessPermissions(
            @RequestParam("project") String project, @RequestParam(value = "name", required = false) String name,
            @RequestParam(value = "is_case_sensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        String projectName = checkProjectName(project);
        String projectUuid = getProjectUuid(projectName);
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, projectUuid);
        List<AccessEntryResponse> aeResponses = accessService.generateAceResponsesByFuzzMatching(ae, name,
                isCaseSensitive);
        List<ProjectPermissionResponse> permissionResponses = convertAceResponseToProjectPermissionResponse(
                aeResponses);

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                DataResult.get(permissionResponses, pageOffset, pageSize), "");
    }

    @ApiOperation(value = "grantProjectPermission", tags = { "MID" })
    @PostMapping(value = "/project")
    @ResponseBody
    public EnvelopeResponse<String> grantProjectPermission(@RequestBody BatchProjectPermissionRequest permissionRequest)
            throws IOException {
        String projectName = checkProjectName(permissionRequest.getProject());
        permissionRequest.setProject(projectName);

        checkType(permissionRequest.getType());
        checkNames(permissionRequest.getNames());
        updateRequestCaseInsentive(permissionRequest);
        ExternalAclProvider.checkExternalPermission(permissionRequest.getPermission());

        String projectUuid = getProjectUuid(permissionRequest.getProject());
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, projectUuid);
        List<AccessRequest> accessRequests = convertBatchPermissionRequestToAccessRequests(ae, permissionRequest);
        accessService.checkAccessRequestList(accessRequests);

        accessService.batchGrantAccess(accessRequests, ae);
        aclTCRService.updateAclTCR(projectUuid, accessRequests);

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateProjectPermission", tags = { "MID" })
    @PutMapping(value = "/project")
    @ResponseBody
    public EnvelopeResponse<String> updateProjectPermission(@RequestBody ProjectPermissionRequest permissionRequest)
            throws IOException {
        String projectName = checkProjectName(permissionRequest.getProject());
        permissionRequest.setProject(projectName);

        checkType(permissionRequest.getType());
        checkName(permissionRequest.getName());
        updateRequestCaseInsentive(permissionRequest);
        ExternalAclProvider.checkExternalPermission(permissionRequest.getPermission());

        AccessRequest accessRequest = convertPermissionRequestToAccessRequest(permissionRequest);
        accessService.checkAccessRequestList(Lists.newArrayList(accessRequest));

        String projectUuid = getProjectUuid(permissionRequest.getProject());
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, projectUuid);
        accessService.grantAccess(ae, accessRequest.getSid(), accessRequest.isPrincipal(),
                accessRequest.getPermission());

        aclTCRService.updateAclTCR(projectUuid, Lists.newArrayList(accessRequest));

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "revokeProjectPermission", tags = { "MID" })
    @DeleteMapping(value = "/project")
    @ResponseBody
    public EnvelopeResponse<String> revokeProjectPermission(@RequestParam("project") String project,
            @RequestParam("type") String type, @RequestParam("name") String name) throws IOException {
        String projectName = checkProjectName(project);
        String projectUuid = getProjectUuid(projectName);
        checkType(type);
        checkSidExists(type, name);
        checkSidGranted(projectUuid, name);

        boolean principal = MetadataConstants.TYPE_USER.equalsIgnoreCase(type);
        if (principal) {
            accessService.checkGlobalAdmin(name);
        }
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, projectUuid);
        accessService.revokeAccess(ae, name, principal);
        aclTCRService.revokeAclTCR(projectUuid, name, principal);

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "getUserOrGroupAclPermissions", tags = { "MID" })
    @GetMapping(value = "/acls")
    @ResponseBody
    public EnvelopeResponse<List<SidPermissionWithAclResponse>> getUserOrGroupAclPermissions(
            @RequestParam("type") String type, @RequestParam(value = "name") String name,
            @RequestParam(value = "project", required = false) String project) throws IOException {
        String projectName = "";
        if (StringUtils.isNotBlank(project)) {
            projectName = checkProjectName(project);
        }
        checkType(type);
        checkSidExists(type, name);

        boolean principal = MetadataConstants.TYPE_USER.equalsIgnoreCase(type);

        if (principal) {
            UserDetails userDetails = userService.loadUserByUsername(name);
            if (userDetails != null) {
                name = userDetails.getUsername();
            }
        }

        List<String> projects = new ArrayList<>();
        if (StringUtils.isBlank(project)) {
            projects = accessService.getGrantedProjectsOfUserOrGroup(name, principal);
        } else {
            projects.add(projectName);
        }

        List<SidPermissionWithAclResponse> response = accessService.getUserOrGroupAclPermissions(projects, name,
                principal);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    private void updateRequestCaseInsentive(BatchProjectPermissionRequest permissionRequest) {
        permissionRequest.setType(getCaseInsentiveType(permissionRequest.getType()));
        boolean principal = MetadataConstants.TYPE_USER.equalsIgnoreCase(permissionRequest.getType());
        if (principal) {
            permissionRequest.setNames(makeUserNameCaseInSentive(permissionRequest.getNames()));
        }
    }

    private void updateRequestCaseInsentive(ProjectPermissionRequest permissionRequest) {
        permissionRequest.setType(getCaseInsentiveType(permissionRequest.getType()));
        boolean principal = MetadataConstants.TYPE_USER.equalsIgnoreCase(permissionRequest.getType());
        if (principal) {
            permissionRequest.setName(makeUserNameCaseInSentive(permissionRequest.getName()));
        }
    }

    private void checkSidExists(String type, String name) throws IOException {
        boolean principal = MetadataConstants.TYPE_USER.equalsIgnoreCase(type);
        accessService.checkSid(name, principal);
    }

    private void checkSidGranted(String projectUuid, String name) throws IOException {
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, projectUuid);
        List<AccessEntryResponse> aeResponses = accessService.generateAceResponsesByFuzzMatching(ae, name, false);
        if (CollectionUtils.isEmpty(aeResponses)) {
            throw new KylinException(UNAUTHORIZED_ENTITY, MsgPicker.getMsg().getUnauthorizedSid());
        }
    }

    private void checkType(String type) {
        if (MetadataConstants.TYPE_USER.equalsIgnoreCase(type) || MetadataConstants.TYPE_GROUP.equalsIgnoreCase(type)) {
            return;
        }
        throw new KylinException(PARAMETER_INVALID_SUPPORT_LIST, "type", "user, group");
    }

    private void checkNames(List<String> names) {
        if (CollectionUtils.isEmpty(names) || names.stream().anyMatch(StringUtils::isBlank)) {
            throw new KylinException(EMPTY_PARAMETER, MsgPicker.getMsg().getEmptySid());
        }
    }

    private void checkName(String name) {
        if (StringUtils.isBlank(name)) {
            throw new KylinException(EMPTY_PARAMETER, MsgPicker.getMsg().getEmptySid());
        }
    }

    /**
     * This method will not check project, user must prepare illegal project name
     * which the same as ProjectInstance#getName
     */
    private String getProjectUuid(String project) {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance prjInstance = projectManager.getProject(project);
        return prjInstance.getUuid();
    }

    private AccessRequest convertPermissionRequestToAccessRequest(ProjectPermissionRequest permissionRequest) {
        AccessRequest accessRequest = new AccessRequest();

        accessRequest.setPrincipal(MetadataConstants.TYPE_USER.equalsIgnoreCase(permissionRequest.getType()));
        accessRequest.setSid(permissionRequest.getName());
        String permission = AclPermissionEnum
                .convertToAclPermission(permissionRequest.getPermission().toUpperCase(Locale.ROOT));
        accessRequest.setPermission(permission);

        return accessRequest;
    }

    @VisibleForTesting
    public List<AccessRequest> convertBatchPermissionRequestToAccessRequests(AclEntity ae,
            BatchProjectPermissionRequest permissionRequest) {
        List<AccessRequest> accessRequests = new ArrayList<>();
        String type = permissionRequest.getType();
        String externalPermission = permissionRequest.getPermission().toUpperCase(Locale.ROOT);
        String aclPermission = AclPermissionEnum.convertToAclPermission(externalPermission);

        List<String> names = permissionRequest.getNames();
        for (String name : names) {
            AccessRequest accessRequest = new AccessRequest();
            accessRequest.setPermission(aclPermission);
            accessRequest.setPrincipal(MetadataConstants.TYPE_USER.equalsIgnoreCase(type));
            accessRequest.setSid(name);
            accessRequests.add(accessRequest);
        }
        if (MetadataConstants.TYPE_USER.equalsIgnoreCase(type)) {
            List<String> allAclSids = accessService.getAllAclSids(ae, type);
            accessRequests.forEach(request -> {
                for (String aclSid : allAclSids) {
                    if (request.getSid().equalsIgnoreCase(aclSid)) {
                        request.setSid(aclSid);
                    }
                }
            });
        }
        return accessRequests;
    }

    private List<ProjectPermissionResponse> convertAceResponseToProjectPermissionResponse(
            List<AccessEntryResponse> aclResponseList) {
        List<ProjectPermissionResponse> responseList = new ArrayList<>();
        for (AccessEntryResponse aclResponse : aclResponseList) {
            String type = "";
            String userOrGroupName = "";
            Sid sid = aclResponse.getSid();
            List<String> extPermissionList;
            if (sid instanceof PrincipalSid) {
                type = MetadataConstants.TYPE_USER;
                userOrGroupName = ((PrincipalSid) sid).getPrincipal();
            } else if (sid instanceof GrantedAuthoritySid) {
                type = MetadataConstants.TYPE_GROUP;
                userOrGroupName = ((GrantedAuthoritySid) sid).getGrantedAuthority();
            }
            extPermissionList = CollectionUtils.isEmpty(aclResponse.getExtPermissions()) ? Collections.EMPTY_LIST
                    : aclResponse.getExtPermissions().stream().map(ExternalAclProvider::convertToExternalPermission)
                            .collect(Collectors.toList());
            String externalPermission = ExternalAclProvider.convertToExternalPermission(aclResponse.getPermission());
            responseList
                    .add(new ProjectPermissionResponse(type, userOrGroupName, externalPermission, extPermissionList));
        }
        return responseList;
    }
}
