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

package org.apache.kylin.rest.controller2;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.TableACLService;
import org.apache.kylin.rest.util.ValidateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/acl")
public class TableAclControllerV2 extends BasicController {

    @Autowired
    @Qualifier("TableAclService")
    private TableACLService tableACLService;

    @Autowired
    @Qualifier("validateUtil")
    private ValidateUtil validateUtil;

    @RequestMapping(value = "/table/{project}/{table:.+}", method = {RequestMethod.GET}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse getTableWhiteListByTable(@PathVariable String project, @PathVariable String table) throws IOException {
        validateUtil.vaildateArgs(project, table);
        project = project.toUpperCase();
        validateUtil.validateTable(project, table);
        List<String> allUsers = validateUtil.getAllUsers();
        List<String> whiteList = tableACLService.getTableWhiteListByTable(project, table, allUsers);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, whiteList, "get table acl");
    }

    @RequestMapping(value = "/table/{project}/black/{table:.+}", method = {RequestMethod.GET}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse getTableBlackListByTable(@PathVariable String project, @PathVariable String table) throws IOException {
        validateUtil.vaildateArgs(project, table);
        project = project.toUpperCase();
        validateUtil.validateTable(project, table);
        List<String> blackList = tableACLService.getBlockedUserByTable(project, table);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, blackList, "get table acl");
    }

    // because the frontend passes user can not visit, so that means put it to the table black list
    @RequestMapping(value = "/table/{project}/{table}/{username}", method = {RequestMethod.DELETE}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse putUserToTableBlackList(
            @PathVariable String project,
            @PathVariable String table,
            @PathVariable String username) throws IOException {
        validateUtil.vaildateArgs(project, table, username);
        project = project.toUpperCase();
        validateUtil.validateUser(username);
        validateUtil.validateTable(project, table);
        tableACLService.addToTableBlackList(project, username, table);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "revoke user table query permission and add user to table black list.");
    }

    // because the frontend passes user can visit, so that means remove the user from the table black list
    @RequestMapping(value = "/table/{project}/{table}/{username}", method = {RequestMethod.POST}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse deleteUserFromTableBlackList(
            @PathVariable String project,
            @PathVariable String table,
            @PathVariable String username) throws IOException {
        validateUtil.vaildateArgs(project, table, username);
        project = project.toUpperCase();
        validateUtil.validateUser(username);
        validateUtil.validateTable(project, table);
        tableACLService.deleteFromTableBlackList(project, username, table);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "grant user table query permission and remove user from table black list.");
    }
}
