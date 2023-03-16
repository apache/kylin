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

import static org.apache.kylin.common.exception.ServerErrorCode.ACCESS_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_USERGROUP_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.request.AclTCRRequest;
import org.apache.kylin.rest.service.AclTCRService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/acl", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenAclTCRController extends NBasicController {

    @Autowired
    @Qualifier("aclTCRService")
    private AclTCRService aclTCRService;

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("userGroupService")
    private IUserGroupService userGroupService;

    @ApiOperation(value = "updateProjectAcl", tags = { "MID" }, notes = "Update URL: {project}; Update Param: project")
    @PutMapping(value = "/sid/{sid_type:.+}/{sid:.+}")
    @ResponseBody
    public EnvelopeResponse<String> updateProject(@PathVariable("sid_type") String sidType, //
            @PathVariable("sid") String sid, //
            @RequestParam("project") String project, //
            @RequestBody List<AclTCRRequest> requests) throws IOException {
        checkProjectName(project);
        AclPermissionUtil.checkAclUpdatable(project, aclTCRService.getCurrentUserGroups());
        // Depreciated api can't use new field `row_filter`
        requests.stream().forEach(request -> Optional.ofNullable(request.getTables()).orElse(Lists.newArrayList())
                .stream().forEach(table -> {
                    table.setRowFilter(null);
                }));
        if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            sid = makeUserNameCaseInSentive(sid);
            mergeSidAclTCR(project, sid, true, requests);
        } else if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_GROUP)) {
            mergeSidAclTCR(project, sid, false, requests);
        } else {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getInvalidSidType());
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    private void mergeSidAclTCR(String project, String sid, boolean principal, List<AclTCRRequest> requests)
            throws IOException {
        mergeSidAclTCR(project, sid, principal, requests, null);
    }

    private void mergeSidAclTCR(String project, String sid, boolean principal, List<AclTCRRequest> requests, Set<String> allGroups)
            throws IOException {
        if (allGroups != null) {
            accessService.batchCheckSid(sid, principal, allGroups);
        } else {
            accessService.checkSid(sid, principal);
        }
        boolean hasProjectPermission = accessService.hasProjectPermission(project, sid, principal);

        if (!hasProjectPermission) {
            Message msg = MsgPicker.getMsg();
            throw new KylinException(ACCESS_DENIED,
                    String.format(Locale.ROOT, msg.getGrantTableWithSidHasNotProjectPermission(), sid, project));
        }
        aclTCRService.mergeAclTCR(project, sid, principal, requests);
    }

    @ApiOperation(value = "updateProjectAcl With New Foramt", tags = {
            "MID" }, notes = "Update URL: {project}; Update Param: project")
    @PutMapping(value = "/batch/{sid_type:.+}")
    @ResponseBody
    public EnvelopeResponse<String> batchUpdateProject(@PathVariable("sid_type") String sidType,
            @RequestParam("project") String project, @RequestBody Map<String, List<AclTCRRequest>> requests)
            throws IOException {
        checkProjectName(project);
        Preconditions.checkState(sidType.equalsIgnoreCase(MetadataConstants.TYPE_GROUP));
        AclPermissionUtil.checkAclUpdatable(project, aclTCRService.getCurrentUserGroups());
        List<String> allGroups = userGroupService.getAllUserGroups();
        if (CollectionUtils.isEmpty(allGroups)) {
            throw new KylinException(EMPTY_USERGROUP_NAME, MsgPicker.getMsg().getEmptySid());
        }
        Set<String> groupSet = Sets.newHashSet(allGroups);
        for (Map.Entry<String, List<AclTCRRequest>> entry : requests.entrySet()) {
            mergeSidAclTCR(project, entry.getKey(), false, entry.getValue(), groupSet);
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @ApiOperation(value = "updateProjectAcl With New Foramt", tags = {
            "MID" }, notes = "Update URL: {project}; Update Param: project")
    @PutMapping(value = "/{sid_type:.+}/{sid:.+}")
    @ResponseBody
    public EnvelopeResponse<String> updateProjectV2(@PathVariable("sid_type") String sidType, //
            @PathVariable("sid") String sid, //
            @RequestParam("project") String project, //
            @RequestBody List<AclTCRRequest> requests) throws IOException {
        checkProjectName(project);
        AclPermissionUtil.checkAclUpdatable(project, aclTCRService.getCurrentUserGroups());
        if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            sid = makeUserNameCaseInSentive(sid);
            mergeSidAclTCR(project, sid, true, requests);
        } else if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_GROUP)) {
            mergeSidAclTCR(project, sid, false, requests);
        } else {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getInvalidSidType());
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }
}
