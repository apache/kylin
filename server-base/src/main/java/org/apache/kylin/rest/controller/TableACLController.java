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

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.kylin.metadata.MetadataConstants.TYPE_USER;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.TableACLService;
import org.apache.kylin.rest.service.UserService;
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
public class TableACLController extends BasicController {

    @Autowired
    @Qualifier("TableAclService")
    private TableACLService tableACLService;

    @Autowired
    @Qualifier("validateUtil")
    private ValidateUtil validateUtil;

    @Autowired
    @Qualifier("userService")
    private UserService userService;

    @RequestMapping(value = "/table/{project}/{type}/{table:.+}", method = {RequestMethod.GET}, produces = {"application/json"})
    @ResponseBody
    public List<String> getUsersCanQueryTheTbl(@PathVariable String project, @PathVariable String type, @PathVariable String table) throws IOException {
        validateUtil.validateArgs(project, table);
        validateUtil.validateTable(project, table);
        Set<String> allIdentifiers = validateUtil.getAllIdentifiersInPrj(project, type);
        // add global admins
        if (type.equals(TYPE_USER)) {
            allIdentifiers.addAll(userService.listAdminUsers());
        } else {
            allIdentifiers.add(Constant.ROLE_ADMIN);
        }
        return tableACLService.getCanAccessList(project, table, allIdentifiers, type);
    }

    @RequestMapping(value = "/table/{project}/{type}/black/{table:.+}", method = {RequestMethod.GET}, produces = {"application/json"})
    @ResponseBody
    public List<String> getUsersCannotQueryTheTbl(@PathVariable String project, @PathVariable String type, @PathVariable String table) throws IOException {
        validateUtil.validateArgs(project, table);
        validateUtil.validateTable(project, table);
        return tableACLService.getNoAccessList(project, table, type);
    }

    // because the frontend passes user can not visit, so that means put it to the table black list
    @RequestMapping(value = "/table/{project}/{type}/{table}/{name}", method = {RequestMethod.DELETE}, produces = {"application/json"})
    @ResponseBody
    public void putUserToTableBlackList(
            @PathVariable String project,
            @PathVariable String type,
            @PathVariable String table,
            @PathVariable String name) throws IOException {
        validateUtil.validateArgs(project, table, name);
        validateUtil.validateIdentifiers(project, name, type);
        validateUtil.validateTable(project, table);
        tableACLService.addToTableACL(project, name, table, type);
    }

    // because the frontend passes user can visit, so that means remove the user from the table black list
    @RequestMapping(value = "/table/{project}/{type}/{table}/{name}", method = {RequestMethod.POST}, produces = {"application/json"})
    @ResponseBody
    public void deleteUserFromTableBlackList(
            @PathVariable String project,
            @PathVariable String type,
            @PathVariable String table,
            @PathVariable String name) throws IOException {
        validateUtil.validateArgs(project, table, name);
        validateUtil.validateIdentifiers(project, name, type);
        validateUtil.validateTable(project, table);
        tableACLService.deleteFromTableACL(project, name, table, type);
    }
}
