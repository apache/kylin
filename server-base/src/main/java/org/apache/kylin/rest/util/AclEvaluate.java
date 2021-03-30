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
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.CheckpointExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;

@Component("aclEvaluate")
public class AclEvaluate {
    @Autowired
    private AclUtil aclUtil;

    private ProjectInstance getProjectInstance(String projectName) {
        return ProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(projectName);
    }

    private ProjectInstance getProjectInstanceByCubeName(String cube) {
        CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cube);
        return cubeInstance.getProjectInstance();
    }

    private ProjectInstance getProjectByJob(JobInstance job) {
        AbstractExecutable executable = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getJob(job.getUuid());
        String projectName = null;
        if (executable instanceof CubingJob) {
            projectName = ((CubingJob) executable).getProjectName();
        } else if (executable instanceof CheckpointExecutable) {
            projectName = ((CheckpointExecutable) executable).getProjectName();
        } else {
            return null;
        }
        return getProjectInstance(projectName);
    }

    public void checkProjectAdminPermission(ProjectInstance projectInstance) {
        aclUtil.hasProjectAdminPermission(projectInstance);
    }

    //for raw project
    public void checkProjectReadPermission(ProjectInstance projectInstance) {
        aclUtil.hasProjectReadPermission(projectInstance);
    }

    public void checkProjectWritePermission(ProjectInstance projectInstance) {
        aclUtil.hasProjectWritePermission(projectInstance);
    }

    public void checkProjectOperationPermission(ProjectInstance projectInstance) {
        aclUtil.hasProjectOperationPermission(projectInstance);
    }

    public void checkProjectAdminPermission(String projectName) {
        ProjectInstance projectInstance = getProjectInstance(projectName);
        checkProjectAdminPermission(projectInstance);
    }

    //for raw project
    public void checkProjectReadPermission(String projectName) {
        ProjectInstance projectInstance = getProjectInstance(projectName);
        checkProjectReadPermission(projectInstance);
    }

    public void checkProjectWritePermission(String projectName) {
        ProjectInstance projectInstance = getProjectInstance(projectName);
        checkProjectWritePermission(projectInstance);
    }

    public void checkProjectOperationPermission(String projectName) {
        ProjectInstance projectInstance = getProjectInstance(projectName);
        checkProjectOperationPermission(projectInstance);
    }

    //for cube acl entity
    public void checkProjectReadPermission(CubeInstance cube) {
        aclUtil.hasProjectReadPermission(cube.getProjectInstance());
    }

    public void checkProjectWritePermission(CubeInstance cube) {
        aclUtil.hasProjectWritePermission(cube.getProjectInstance());
    }

    public void checkProjectOperationPermission(CubeInstance cube) {
        aclUtil.hasProjectOperationPermission(cube.getProjectInstance());
    }

    //for job acl entity
    public void checkProjectReadPermission(JobInstance job) {
        aclUtil.hasProjectReadPermission(getProjectByJob(job));
    }

    public void checkProjectWritePermission(JobInstance job) {
        aclUtil.hasProjectWritePermission(getProjectByJob(job));
    }

    public void checkProjectOperationPermission(JobInstance job) {
        aclUtil.hasProjectOperationPermission(getProjectByJob(job));
    }

    // ACL util's method, so that you can use AclEvaluate
    public String getCurrentUserName() {
        return aclUtil.getCurrentUserName();
    }

    public boolean hasCubeReadPermission(CubeInstance cube) {
        return hasProjectReadPermission(cube.getProjectInstance());
    }

    public boolean hasProjectReadPermission(ProjectInstance project) {
        boolean _hasProjectReadPermission = false;
        try {
            _hasProjectReadPermission = aclUtil.hasProjectReadPermission(project);
        } catch (AccessDeniedException e) {
            //ignore to continue
        }
        return _hasProjectReadPermission;
    }

    public boolean hasProjectOperationPermission(ProjectInstance project) {
        boolean _hasProjectOperationPermission = false;
        try {
            _hasProjectOperationPermission = aclUtil.hasProjectOperationPermission(project);
        } catch (AccessDeniedException e) {
            //ignore to continue
        }
        return _hasProjectOperationPermission;
    }

    public boolean hasProjectWritePermission(ProjectInstance project) {
        boolean _hasProjectWritePermission = false;
        try {
            _hasProjectWritePermission = aclUtil.hasProjectWritePermission(project);
        } catch (AccessDeniedException e) {
            //ignore to continue
        }
        return _hasProjectWritePermission;
    }

    public boolean hasProjectAdminPermission(ProjectInstance project) {
        boolean _hasProjectAdminPermission = false;
        try {
            _hasProjectAdminPermission = aclUtil.hasProjectAdminPermission(project);
        } catch (AccessDeniedException e) {
            //ignore to continue
        }
        return _hasProjectAdminPermission;
    }

    public void checkIsGlobalAdmin() {
        aclUtil.checkIsGlobalAdmin();
    }

}