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
package org.apache.kylin.rest.controller.v2;

import static org.apache.kylin.common.exception.ServerErrorCode.USER_NOT_EXIST;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.service.AclTCRService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/access", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
public class NAccessControllerV2 extends NBasicController {

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("userService")
    protected UserService userService;

    @Autowired
    @Qualifier("aclTCRService")
    private AclTCRService aclTCRService;

    private static final String PROJECT_NAME = "project_name";
    private static final String TABLE_NAME = "table_name";

    private void checkUserName(String userName) {
        if (!userService.userExists(userName)) {
            throw new KylinException(USER_NOT_EXIST,
                    String.format(Locale.ROOT, "User '%s' does not exists.", userName));
        }
    }

    /**
     * Get user's all granted projects and tables
     *
     * @param username
     * @return
     * @throws IOException
     */
    @ApiOperation(value = "getAllAccessEntitiesOfUser", tags = { "MID" })
    @GetMapping(value = "/{userName:.+}")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse getAllAccessEntitiesOfUser(@PathVariable("userName") String username) throws IOException {
        checkUserName(username);

        List<Object> dataList = new ArrayList<>();
        List<String> projectList = accessService.getGrantedProjectsOfUser(username);

        for (String project : projectList) {
            Map<String, Object> data = new HashMap<>();
            data.put(PROJECT_NAME, project);
            List<String> tableList = aclTCRService.getAuthorizedTables(project, username).stream()
                    .map(TableDesc::getIdentity).collect(Collectors.toList());
            data.put(TABLE_NAME, tableList);
            dataList.add(data);
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, dataList, "");
    }

    @ApiOperation(value = "getAccessEntities", tags = { "MID" })
    @GetMapping(value = "/{type}/{project}", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<Map<String, Object>> getAccessEntities(@PathVariable("type") String type,
            @PathVariable("project") String project, @RequestParam(value = "name", required = false) String nameSeg,
            @RequestParam(value = "isCaseSensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        AclEntity ae = accessService.getAclEntity(type, getProject(project).getUuid());
        List<AccessEntryResponse> resultsAfterFuzzyMatching = this.accessService.generateAceResponsesByFuzzMatching(ae,
                nameSeg, isCaseSensitive);
        List<AccessEntryResponse> sublist = PagingUtil.cutPage(resultsAfterFuzzyMatching, pageOffset, pageSize);
        HashMap<String, Object> data = new HashMap<>();
        data.put("sids", sublist);
        data.put("size", resultsAfterFuzzyMatching.size());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, data, "");
    }
}
