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

import static org.apache.kylin.common.exception.ServerErrorCode.ACCESS_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.request.AclTCRRequest;
import org.apache.kylin.rest.response.AclTCRResponse;
import org.apache.kylin.rest.service.AclTCRService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/acl", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class AclTCRController extends NBasicController {

    @Autowired
    @Qualifier("aclTCRService")
    private AclTCRService aclTCRService;

    @Autowired
    @Qualifier("userService")
    protected UserService userService;

    @Autowired
    @Qualifier("userGroupService")
    private IUserGroupService userGroupService;

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @ApiOperation(value = "getProjectSidTCR", tags = { "MID" }, //
            notes = "Update URL: {project}; Update Param: project, authorized_only")
    @GetMapping(value = "/sid/{sid_type:.+}/{sid:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<List<AclTCRResponse>> getProjectSidTCR(@PathVariable("sid_type") String sidType,
            @PathVariable("sid") String sid, //
            @RequestParam("project") String project, //
            @RequestParam(value = "authorized_only", required = false, defaultValue = "false") boolean authorizedOnly)
            throws IOException {
        checkProjectName(project);
        List<AclTCRResponse> result;
        if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            sid = makeUserNameCaseInSentive(sid);
            result = getProjectSidTCR(project, sid, true, authorizedOnly);
        } else if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_GROUP)) {
            result = getProjectSidTCR(project, sid, false, authorizedOnly);
        } else {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getInvalidSidType());
        }
        // remove new field `row_filter`
        result.stream().forEach(resp -> {
            Optional.ofNullable(resp.getTables()).orElse(Lists.newArrayList()).stream().forEach(table -> {
                table.setRowFilter(null);
            });
        });
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "getProjectSidTCR", tags = { "MID" }, //
            notes = "Update URL: {project}; Update Param: project, authorized_only")
    @GetMapping(value = "/{sid_type:.+}/{sid:.+}", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<List<AclTCRResponse>> getProjectSidTCRV2(@PathVariable("sid_type") String sidType,
            @PathVariable("sid") String sid, //
            @RequestParam("project") String project, //
            @RequestParam(value = "authorized_only", required = false, defaultValue = "false") boolean authorizedOnly)
            throws IOException {
        checkProjectName(project);
        List<AclTCRResponse> result;
        if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_USER)) {
            sid = makeUserNameCaseInSentive(sid);
            result = getProjectSidTCR(project, sid, true, authorizedOnly);
        } else if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_GROUP)) {
            result = getProjectSidTCR(project, sid, false, authorizedOnly);
        } else {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getInvalidSidType());
        }
        // remove depreciated fields `rows` and `like_rows`
        result.stream().forEach(resp -> {
            Optional.ofNullable(resp.getTables()).orElse(Lists.newArrayList()).stream().forEach(table -> {
                table.setRows(null);
                table.setLikeRows(null);
            });
        });
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @ApiOperation(value = "updateProject", tags = { "MID" }, notes = "Update URL: {project}")
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
            updateSidAclTCR(project, sid, true, requests);
        } else if (sidType.equalsIgnoreCase(MetadataConstants.TYPE_GROUP)) {
            updateSidAclTCR(project, sid, false, requests);
        } else {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getInvalidSidType());
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @GetMapping(value = "/updatable", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<Boolean> getAllowAclUpdatable(@RequestParam("project") String project) throws IOException {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                AclPermissionUtil.isAclUpdatable(project, aclTCRService.getCurrentUserGroups()), "");
    }

    private List<AclTCRResponse> getProjectSidTCR(String project, String sid, boolean principal, boolean authorizedOnly)
            throws IOException {
        accessService.checkSid(sid, principal);
        return aclTCRService.getAclTCRResponse(project, sid, principal, authorizedOnly);
    }

    private void updateSidAclTCR(String project, String sid, boolean principal, List<AclTCRRequest> requests)
            throws IOException {
        accessService.checkSid(sid, principal);
        boolean hasProjectPermission = accessService.hasProjectPermission(project, sid, principal);

        if (!hasProjectPermission) {
            Message msg = MsgPicker.getMsg();
            throw new KylinException(ACCESS_DENIED,
                    String.format(Locale.ROOT, msg.getGrantTableWithSidHasNotProjectPermission(), sid, project));
        }
        aclTCRService.updateAclTCR(project, sid, principal, requests);
    }
}
