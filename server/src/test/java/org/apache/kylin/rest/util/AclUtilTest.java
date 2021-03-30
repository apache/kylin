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

package org.apache.kylin.rest.util;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.controller.AccessController;
import org.apache.kylin.rest.request.AccessRequest;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.io.IOException;

public class AclUtilTest extends ServiceTestBase {
    @Autowired
    AccessController accessController;

    @Autowired
    AclUtil aclUtil;

    @Test
    public void testNull() {
        // ADMIN will go into hasRole first.
        swichUser("ANALYST", Constant.ROLE_ANALYST);
        try {
            aclUtil.hasProjectAdminPermission(null);
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals("Access is denied", e.getMessage());
        }
    }

    @Test
    public void testBasic() throws IOException {
        final String PROJECT = "default";
        final String ANALYST = "ANALYST";
        final String ADMIN = "ADMIN";
        final String READ = "READ";
        final String OPERATION = "OPERATION";
        final String MANAGEMENT = "MANAGEMENT";
        final String ADMINISTRATION = "ADMINISTRATION";

        swichUser(ADMIN, Constant.ROLE_ADMIN);
        ProjectInstance projectInstance = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(PROJECT);
        accessController.grant("ProjectInstance", projectInstance.getUuid(), getAccessRequest(ANALYST, READ));
        swichUser(ANALYST, Constant.ROLE_ANALYST);
        aclUtil.hasProjectReadPermission(projectInstance);
        try {
            aclUtil.hasProjectOperationPermission(projectInstance);
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals("Access is denied", e.getMessage());
        }

        swichUser(ADMIN, Constant.ROLE_ADMIN);
        accessController.grant("ProjectInstance", projectInstance.getUuid(), getAccessRequest(ANALYST, OPERATION));
        swichUser(ANALYST, Constant.ROLE_ANALYST);
        aclUtil.hasProjectOperationPermission(projectInstance);
        try {
            aclUtil.hasProjectWritePermission(projectInstance);
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals("Access is denied", e.getMessage());
        }

        swichUser(ADMIN, Constant.ROLE_ADMIN);
        accessController.grant("ProjectInstance", projectInstance.getUuid(), getAccessRequest(ANALYST, MANAGEMENT));
        swichUser(ANALYST, Constant.ROLE_ANALYST);
        aclUtil.hasProjectWritePermission(projectInstance);
        try {
            aclUtil.hasProjectAdminPermission(projectInstance);
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals("Access is denied", e.getMessage());
        }

        swichUser(ADMIN, Constant.ROLE_ADMIN);
        accessController.grant("ProjectInstance", projectInstance.getUuid(), getAccessRequest(ANALYST, ADMINISTRATION));
        swichUser(ANALYST, Constant.ROLE_ANALYST);
        aclUtil.hasProjectAdminPermission(projectInstance);
        try {
            aclUtil.checkIsGlobalAdmin();
            Assert.fail("expecting some AlreadyExistsException here");
        } catch (Exception e) {
            Assert.assertEquals("Access is denied", e.getMessage());
        }
        swichUser(ADMIN, Constant.ROLE_ADMIN);
        aclUtil.checkIsGlobalAdmin();
    }

    private void swichUser(String name, String auth) {
        Authentication token = new TestingAuthenticationToken(name, name, auth);
        SecurityContextHolder.getContext().setAuthentication(token);
    }

    private AccessRequest getAccessRequest(String role, String permission) {
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setPermission(permission);
        accessRequest.setSid(role);
        accessRequest.setPrincipal(true);
        return accessRequest;
    }
}
