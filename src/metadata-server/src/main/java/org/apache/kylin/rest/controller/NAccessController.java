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
import static org.apache.kylin.common.exception.ServerErrorCode.DUPLICATE_USER_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_ID_NOT_EXIST;
import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rest.request.AccessRequest;
import org.apache.kylin.rest.request.BatchAccessRequest;
import org.apache.kylin.rest.request.GlobalAccessRequest;
import org.apache.kylin.rest.request.GlobalBatchAccessRequest;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.response.CompositePermissionResponse;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.UserAccessEntryResponse;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.security.AclPermissionEnum;
import org.apache.kylin.rest.security.AclPermissionFactory;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.AclTCRService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.UserAclService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.PagingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.acls.model.Permission;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/access", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class NAccessController extends NBasicController {

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("userService")
    protected UserService userService;

    @Autowired
    @Qualifier("userGroupService")
    private IUserGroupService userGroupService;

    @Autowired
    @Qualifier("aclTCRService")
    private AclTCRService aclTCRService;

    @Autowired(required = false)
    @Qualifier("userAclService")
    private UserAclService userAclService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    /**
     * Get current user's permission in the project
     */
    @ApiOperation(value = "access control APIs", tags = { "MID" })
    @GetMapping(value = "/permission/project_permission", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> getUserPermissionInPrj(@RequestParam(value = "project") String project) {
        checkProjectName(project);
        List<String> groups = accessService.getGroupsOfCurrentUser();
        String permission = groups.contains(ROLE_ADMIN) ? "GLOBAL_ADMIN"
                : AclPermissionEnum
                        .convertToAclPermission(accessService.getCurrentNormalUserPermissionInProject(project));

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, permission, "");
    }

    /**
     * Get current user's permission in the project
     */
    @ApiOperation(value = "access control APIs", tags = { "MID" })
    @GetMapping(value = "/permission/project_ext_permission", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<CompositePermissionResponse> getUserExtPermissionInPrj(
            @RequestParam(value = "project") String project) {
        checkProjectName(project);
        List<String> groups = accessService.getGroupsOfCurrentUser();
        CompositePermissionResponse permissionResponse = new CompositePermissionResponse();
        String permission = groups.contains(ROLE_ADMIN) ? "GLOBAL_ADMIN"
                : AclPermissionEnum
                        .convertToAclPermission(accessService.getCurrentNormalUserPermissionInProject(project));
        permissionResponse.setPermission(permission);
        permissionResponse.setExtPermissions(accessService.getUserNormalExtPermissions(project));

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, permissionResponse, "");
    }

    @ApiOperation(value = "getAvailableSids", tags = { "MID" }, //
            notes = "Update Param: sid_type, is_case_sensitive, page_offset, page_size; Update Response: total_size")
    @GetMapping(value = "/available/{sid_type:.+}/{uuid:.+}")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<String>>> getAvailableSids(@PathVariable("uuid") String uuid,
            @PathVariable("sid_type") String sidType, @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "is_case_sensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, uuid);
        List<String> whole = Lists.newArrayList();
        if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            whole.addAll(userService.listNormalUsers());
            List<String> users = accessService.getAllAclSids(ae, MetadataConstants.TYPE_USER);
            whole.removeAll(users);
        } else {
            whole.addAll(getAllUserGroups());
            List<String> groups = accessService.getAllAclSids(ae, MetadataConstants.TYPE_GROUP);
            whole.removeAll(groups);
            whole.remove(ROLE_ADMIN);
        }

        List<String> matchedSids = PagingUtil.getIdentifierAfterFuzzyMatching(nameSeg, isCaseSensitive, whole);
        List<String> subList = PagingUtil.cutPage(matchedSids, pageOffset, pageSize);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(subList, matchedSids), "");
    }

    @ApiOperation(value = "access control APIs", tags = { "MID" })
    @GetMapping(value = "/available/{entity_type:.+}")
    @ResponseBody
    public EnvelopeResponse<DataResult<List<String>>> getAvailableUsers(@PathVariable("entity_type") String entityType,
            @RequestParam(value = "project") String project,
            @RequestParam(value = "model", required = false) String modelId,
            @RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "is_case_sensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        checkProjectName(project);

        List<String> whole = Lists.newArrayList();
        if (AclEntityType.PROJECT_INSTANCE.equals(entityType)) {
            whole.addAll(accessService.getProjectAdminUsers(project));
            whole.remove(getProject(project).getOwner());
        } else {
            checkRequiredArg("model", modelId);
            whole.addAll(accessService.getProjectManagementUsers(project));

            NDataModel model = projectService.getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
            if (Objects.isNull(model) || model.isBroken()) {
                throw new KylinException(MODEL_ID_NOT_EXIST, modelId);
            }
            whole.remove(model.getOwner());
        }

        List<String> matchedUsers = PagingUtil.getIdentifierAfterFuzzyMatching(nameSeg, isCaseSensitive, whole);
        List<String> subList = PagingUtil.cutPage(matchedUsers, pageOffset, pageSize);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(subList, matchedUsers), "");
    }

    @ApiOperation(value = "getGlobalUserAccessEntities", tags = { "MID" }, //
            notes = "Update Param: sid")
    @GetMapping(value = "/global/permission/{permissionType:.+}/{sid:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<Map<String, Boolean>> getGlobalUserAccessEntities(
            @PathVariable(value = "permissionType") String permissionType, @PathVariable(value = "sid") String sid) {
        Permission permission = AclPermissionFactory.getPermission(permissionType.toUpperCase(Locale.ROOT));
        boolean hasAclPermission = userAclService.isSuperAdmin(sid)
                || userAclService.hasUserAclPermission(sid, permission);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                Collections.singletonMap("enabled", hasAclPermission), "");
    }

    @ApiOperation(value = "addGlobalUserAccessEntities", tags = { "MID" }, //
            notes = "Update Body: sid")
    @PutMapping(value = "/global/permission/{permissionType:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> addGlobalUserAccessEntities(
            @PathVariable(value = "permissionType") String permissionType,
            @RequestBody GlobalAccessRequest accessRequest) {
        checkRequiredArg("username", accessRequest.getUsername());
        if (accessRequest.isEnabled()) {
            userAclService.grantUserAclPermission(accessRequest, permissionType);
        } else {
            userAclService.revokeUserAclPermission(accessRequest, permissionType);
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @PostMapping(value = "/global/permission/project/{permissionType:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> addProjectToUserAcl(
            @PathVariable(value = "permissionType") String permissionType,
            @RequestBody GlobalAccessRequest accessRequest) {
        checkRequiredArg("username", accessRequest.getUsername());
        checkProjectName(accessRequest.getProject());
        userAclService.addProjectToUserAcl(accessRequest, permissionType);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @DeleteMapping(value = "/global/permission/project/{permissionType:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> deleteProjectFromUserAcl(
            @PathVariable(value = "permissionType") String permissionType,
            @RequestBody GlobalAccessRequest accessRequest) {
        checkRequiredArg("username", accessRequest.getUsername());
        checkProjectName(accessRequest.getProject());
        userAclService.deleteProjectFromUserAcl(accessRequest, permissionType);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "batchAddGlobalUserAccessEntities", tags = { "MID" }, //
            notes = "Update Body: sid")
    @PutMapping(value = "/global/batch/permission/{permissionType:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> batchAddGlobalUserAccessEntities(
            @PathVariable(value = "permissionType") String permissionType,
            @RequestBody GlobalBatchAccessRequest accessRequest) {
        checkRequiredArg("usernames", accessRequest.getUsernameList());
        if (accessRequest.isEnabled()) {
            userAclService.grantUserAclPermission(accessRequest, permissionType);
        } else {
            userAclService.revokeUserAclPermission(accessRequest, permissionType);
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }


    @ApiOperation(value = "getAllGlobalUsersAccessEntities", tags = { "MID" }, //
            notes = "Update Param: sid")
    @GetMapping(value = "/global/permission/user_acls", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<List<UserAccessEntryResponse>> getAllGlobalUsersAccessEntities() {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, userAclService.listUserAcl(), "");
    }

    @ApiOperation(value = "getAccessEntities", tags = { "MID" }, //
            notes = "Update Param: is_case_sensitive, page_offset, page_size; Update Response: total_size")
    @GetMapping(value = "/{type:.+}/{uuid:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<DataResult<List<AccessEntryResponse>>> getAccessEntities(@PathVariable("type") String type,
            @PathVariable("uuid") String uuid, @RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "is_case_sensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {

        AclEntity ae = accessService.getAclEntity(type, uuid);
        List<AccessEntryResponse> resultsAfterFuzzyMatching = this.accessService.generateAceResponsesByFuzzMatching(ae,
                nameSeg, isCaseSensitive);
        List<AccessEntryResponse> sublist = PagingUtil.cutPage(resultsAfterFuzzyMatching, pageOffset, pageSize);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(sublist, resultsAfterFuzzyMatching),
                "");
    }

    @ApiOperation(value = "get users and groups for project", tags = { "MID" })
    @GetMapping(value = "/{uuid:.+}/all", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<Map<String, List<String>>> getProjectUsersAndGroups(@PathVariable("uuid") String uuid)
            throws IOException {
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, uuid);
        Map<String, List<String>> result = accessService.getProjectUsersAndGroups(ae);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    /**
     * Grant a new access on a domain object to a user/role
     */
    @ApiOperation(value = "grant", tags = { "MID" }, notes = "Update URL: {entity_type}; Update Body: access_entry_id")
    @PostMapping(value = "/{entity_type:.+}/{uuid:.+}")
    @ResponseBody
    public EnvelopeResponse<String> grant(@PathVariable("entity_type") String entityType,
            @PathVariable("uuid") String uuid, @RequestBody AccessRequest accessRequest) throws IOException {
        checkSid(accessRequest);
        if (accessRequest.isPrincipal()) {
            accessService.checkGlobalAdmin(accessRequest.getSid());
        }
        AclEntity ae = accessService.getAclEntity(entityType, uuid);
        accessService.grant(ae, accessRequest.getSid(), accessRequest.isPrincipal(), accessRequest.getPermission());
        if (AclEntityType.PROJECT_INSTANCE.equals(entityType)) {
            aclTCRService.remoteGrantACL(uuid, Lists.newArrayList(accessRequest));
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "batchGrant", tags = {
            "MID" }, notes = "Update URL: {entity_type}; Update Body: access_entry_id")
    @PostMapping(value = "/batch/{entity_type:.+}/{uuid:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> batchGrant(@PathVariable("entity_type") String entityType,
            @PathVariable("uuid") String uuid,
            @RequestParam(value = "init_acl", required = false, defaultValue = "true") boolean initAcl,
            @RequestBody List<BatchAccessRequest> batchAccessRequests) throws IOException {
        List<AccessRequest> requests = transform(batchAccessRequests);
        accessService.checkAccessRequestList(requests);

        AclEntity ae = accessService.getAclEntity(entityType, uuid);
        accessService.batchGrant(requests, ae);
        if (AclEntityType.PROJECT_INSTANCE.equals(entityType) && initAcl) {
            aclTCRService.remoteGrantACL(uuid, requests);
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    /**
     * Update a access on a domain object
     *
     * @param accessRequest
     */
    @ApiOperation(value = "batchGrant", tags = { "MID" }, notes = "Update Body: access_entry_id")
    @PutMapping(value = "/{type:.+}/{uuid:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<Boolean> updateAcl(@PathVariable("type") String type, @PathVariable("uuid") String uuid,
            @RequestBody AccessRequest accessRequest) throws IOException {
        checkSid(accessRequest);
        AclEntity ae = accessService.getAclEntity(type, uuid);
        Permission permission = AclPermissionFactory.getPermission(accessRequest.getPermission());
        if (Objects.isNull(permission)) {
            throw new KylinException(PERMISSION_DENIED,
                    "Operation failed, unknown permission:" + accessRequest.getPermission());
        }
        if (accessRequest.isPrincipal()) {
            accessService.checkGlobalAdmin(accessRequest.getSid());
        }

        accessService.update(ae, accessRequest.getAccessEntryId(), permission);
        boolean hasAdminProject = CollectionUtils.isNotEmpty(projectService.getAdminProjects());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, hasAdminProject, "");
    }

    @ApiOperation(value = "revokeAcl", tags = { "MID" }, //
            notes = "URL: entity_type; Update Param: entity_type; Update Body: access_entry_id")
    @DeleteMapping(value = "/{entity_type:.+}/{uuid:.+}")
    @ResponseBody
    public EnvelopeResponse<Boolean> revokeAcl(@PathVariable("entity_type") String entityType,
            @PathVariable("uuid") String uuid, //
            @RequestParam("access_entry_id") Integer accessEntryId, //
            @RequestParam("sid") String sid, //
            @RequestParam("principal") boolean principal) throws IOException {
        accessService.checkSidNotEmpty(sid, principal);
        if (principal) {
            accessService.checkGlobalAdmin(sid);
        }
        AclEntity ae = accessService.getAclEntity(entityType, uuid);
        accessService.revoke(ae, accessEntryId);
        if (AclEntityType.PROJECT_INSTANCE.equals(entityType)) {
            aclTCRService.remoteRevokeACL(uuid, sid, principal);
        }
        boolean hasAdminProject = CollectionUtils.isNotEmpty(projectService.getAdminProjects());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, hasAdminProject, "");
    }

    @ApiOperation(value = "batch revoke Acl", tags = { "MID" }, notes = "")
    @PostMapping(value = "/{entity_type:.+}/{uuid:.+}/deletion", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<Boolean> deleteAces(@PathVariable("entity_type") String entityType,
            @PathVariable("uuid") String uuid, @RequestBody List<AccessRequest> requests) throws IOException {
        accessService.checkSid(requests);
        List<String> users = requests.stream().filter(AccessRequest::isPrincipal).map(AccessRequest::getSid)
                .collect(Collectors.toList());
        accessService.checkGlobalAdmin(users);
        AclEntity ae = accessService.getAclEntity(entityType, uuid);
        accessService.batchRevoke(ae, requests);
        if (AclEntityType.PROJECT_INSTANCE.equals(entityType)) {
            requests.forEach(r -> aclTCRService.remoteRevokeACL(uuid, r.getSid(), r.isPrincipal()));
        }
        boolean hasAdminProject = CollectionUtils.isNotEmpty(projectService.getAdminProjects());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, hasAdminProject, "");
    }

    @ApiOperation(value = "updateExtensionAcl", tags = { "MID" }, notes = "Update Body: access_entry_id")
    @PutMapping(value = "/extension/{type:.+}/{uuid:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<Boolean> updateExtensionAcl(@PathVariable("type") String type,
            @PathVariable("uuid") String uuid, @RequestBody AccessRequest accessRequest) throws IOException {
        checkSid(accessRequest);
        AclEntity ae = accessService.getAclEntity(type, uuid);
        if (accessRequest.isPrincipal()) {
            accessService.checkGlobalAdmin(accessRequest.getSid());
        }
        accessService.updateExtensionPermission(ae, accessRequest);
        boolean hasAdminProject = CollectionUtils.isNotEmpty(projectService.getAdminProjects());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, hasAdminProject, "");
    }

    private List<String> getAllUserGroups() throws IOException {
        return userGroupService.getAllUserGroups();
    }

    private void checkSid(AccessRequest request) throws IOException {
        accessService.checkSid(Lists.newArrayList(request));
    }

    private List<AccessRequest> transform(List<BatchAccessRequest> batchAccessRequests) {
        List<AccessRequest> result = Optional.ofNullable(batchAccessRequests).map(List::stream).orElseGet(Stream::empty)
                .map(b -> Optional.ofNullable(b.getSids()).map(List::stream).orElseGet(Stream::empty).map(sid -> {
                    AccessRequest r = new AccessRequest();
                    r.setAccessEntryId(b.getAccessEntryId());
                    r.setPermission(b.getPermission());
                    r.setExtPermissions(b.getExtPermissions());
                    r.setPrincipal(b.isPrincipal());
                    if (b.isPrincipal()) {
                        r.setSid(makeUserNameCaseInSentive(sid));
                    } else {
                        r.setSid(sid);
                    }
                    return r;
                }).collect(Collectors.toList())).flatMap(List::stream).collect(Collectors.toList());
        Set<String> sids = Sets.newHashSet();
        result.stream().filter(r -> !sids.add(r.getSid() + r.isPrincipal())).findAny().ifPresent(r -> {
            throw new KylinException(DUPLICATE_USER_NAME, "Operation failed, duplicated sid: " + r.getSid());
        });
        return result;
    }

}
