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

import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PROJECT_NAME;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.service.UserAclServiceSupporter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;

@Component("aclEvaluate")
public class AclEvaluate {
    @Autowired
    private AclUtil aclUtil;

    @Autowired(required = false)
    @Qualifier("userAclService")
    private UserAclServiceSupporter userAclService;

    private ProjectInstance getProjectInstance(String projectName) {
        Message msg = MsgPicker.getMsg();
        if (StringUtils.isEmpty(projectName)) {
            throw new KylinException(EMPTY_PROJECT_NAME, msg.getEmptyProjectName());
        }

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance prjInstance = projectManager.getProject(projectName);
        if (prjInstance == null) {
            throw new KylinException(PROJECT_NOT_EXIST, projectName);
        }
        return prjInstance;
    }

    public void checkProjectQueryPermission(String projectName) {
        if (userAclService.canAdminUserQuery() || userAclService.hasUserAclPermissionInProject(projectName)) {
            return;
        }
        aclUtil.hasProjectDataQueryPermission(getProjectInstance(projectName));
    }

    //for raw project
    public void checkProjectReadPermission(String projectName) {
        aclUtil.hasProjectReadPermission(getProjectInstance(projectName));
    }

    public void checkProjectOperationPermission(String projectName) {
        aclUtil.hasProjectOperationPermission(getProjectInstance(projectName));
    }

    public void checkProjectWritePermission(String projectName) {
        aclUtil.hasProjectWritePermission(getProjectInstance(projectName));
    }

    public void checkProjectAdminPermission(String projectName) {
        aclUtil.hasProjectAdminPermission(getProjectInstance(projectName));
    }

    // ACL util's method, so that you can use AclEvaluate
    public String getCurrentUserName() {
        return aclUtil.getCurrentUserName();
    }

    public boolean hasProjectReadPermission(ProjectInstance project) {
        try {
            aclUtil.hasProjectReadPermission(project);
        } catch (AccessDeniedException e) {
            //ignore to continue
            return false;
        }
        return true;
    }

    public boolean hasProjectOperationPermission(ProjectInstance project) {
        try {
            aclUtil.hasProjectOperationPermission(project);
        } catch (AccessDeniedException e) {
            //ignore to continue
            return false;
        }
        return true;
    }

    public boolean hasProjectWritePermission(ProjectInstance project) {
        try {
            aclUtil.hasProjectWritePermission(project);
        } catch (AccessDeniedException e) {
            //ignore to continue
            return false;
        }
        return true;
    }

    public boolean hasProjectAdminPermission(String project) {
        return hasProjectAdminPermission(getProjectInstance(project));
    }

    public boolean hasProjectAdminPermission(ProjectInstance project) {
        try {
            aclUtil.hasProjectAdminPermission(project);
        } catch (AccessDeniedException e) {
            //ignore to continue
            return false;
        }
        return true;
    }

    public void checkIsGlobalAdmin() {
        aclUtil.checkIsGlobalAdmin();
    }

    public void checkProjectOperationDesignPermission(String projectName) {
        boolean indexEnableOperatorDesign = KylinConfig.getInstanceFromEnv().isIndexEnableOperatorDesign();
        aclUtil.hasProjectOperationDesignPermission(getProjectInstance(projectName), indexEnableOperatorDesign);
    }
}
