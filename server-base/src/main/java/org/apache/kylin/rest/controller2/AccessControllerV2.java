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

import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.request.AccessRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.security.AclPermissionFactory;
import org.apache.kylin.rest.service.AccessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author xduo
 * 
 */
@Controller
@RequestMapping(value = "/access")
public class AccessControllerV2 extends BasicController {

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    /**
     * Get access entry list of a domain object
     * 
     * @param uuid
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{type}/{uuid}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getAccessEntitiesV2(@PathVariable String type, @PathVariable String uuid) {

        AclEntity ae = accessService.getAclEntity(type, uuid);
        Acl acl = accessService.getAcl(ae);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, accessService.generateAceResponses(acl), "");
    }

    /**
     * List access entry list of a domain object including its parent
     * @param type
     * @param uuid
     * @return
     */
    @RequestMapping(value = "all/{type}/{uuid}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse listAccessEntitiesV2(@PathVariable String type, @PathVariable String uuid) {
        AclEntity ae = accessService.getAclEntity(type, uuid);
        Acl acl = accessService.getAcl(ae);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, accessService.generateAllAceResponses(acl), "");
    }

    /**
     * Grant a new access on a domain object to a user/role
     * 
     * @param accessRequest
     */

    @RequestMapping(value = "/{type}/{uuid}", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse grantV2(@PathVariable String type, @PathVariable String uuid,
            @RequestBody AccessRequest accessRequest) {

        AclEntity ae = accessService.getAclEntity(type, uuid);
        Sid sid = accessService.getSid(accessRequest.getSid(), accessRequest.isPrincipal());
        Permission permission = AclPermissionFactory.getPermission(accessRequest.getPermission());
        Acl acl = accessService.grant(ae, permission, sid);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, accessService.generateAceResponses(acl), "");
    }

    /**
     * Update a access on a domain object
     * 
     * @param accessRequest
     */

    @RequestMapping(value = "/{type}/{uuid}", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateV2(@PathVariable String type, @PathVariable String uuid,
            @RequestBody AccessRequest accessRequest) {

        AclEntity ae = accessService.getAclEntity(type, uuid);
        Permission permission = AclPermissionFactory.getPermission(accessRequest.getPermission());
        Acl acl = accessService.update(ae, accessRequest.getAccessEntryId(), permission);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, accessService.generateAceResponses(acl), "");
    }

    /**
     * Revoke access on a domain object from a user/role
     * 
     * @param accessRequest
     */

    @RequestMapping(value = "/{type}/{uuid}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse revokeV2(@PathVariable String type, @PathVariable String uuid,
            AccessRequest accessRequest) {

        AclEntity ae = accessService.getAclEntity(type, uuid);
        Acl acl = accessService.revoke(ae, accessRequest.getAccessEntryId());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, accessService.generateAceResponses(acl), "");
    }

    /**
     * @param accessService
     */
    public void setAccessService(AccessService accessService) {
        this.accessService = accessService;
    }

}
