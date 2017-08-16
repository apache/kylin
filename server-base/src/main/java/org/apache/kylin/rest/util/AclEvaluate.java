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
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.springframework.beans.factory.annotation.Autowired;
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
        String projectName = ((CubingJob) executable).getProjectName();
        return getProjectInstance(projectName);
    }

    public boolean checkProjectAdminPermission(String projectName) {
        ProjectInstance projectInstance = getProjectInstance(projectName);
        return aclUtil.hasProjectAdminPermission(projectInstance);
    }

    //for raw project
    public boolean checkProjectReadPermission(String projectName) {
        ProjectInstance projectInstance = getProjectInstance(projectName);
        return aclUtil.hasProjectReadPermission(projectInstance);
    }

    public boolean checkProjectWritePermission(String projectName) {
        ProjectInstance projectInstance = getProjectInstance(projectName);
        return aclUtil.hasProjectWritePermission(projectInstance);
    }

    public boolean checkProjectOperationPermission(String projectName) {
        ProjectInstance projectInstance = getProjectInstance(projectName);
        return aclUtil.hasProjectOperationPermission(projectInstance);
    }

    //for cube acl entity
    public boolean checkProjectReadPermission(CubeInstance cube) {
        return aclUtil.hasProjectReadPermission(cube.getProjectInstance());
    }


    public boolean checkProjectWritePermission(CubeInstance cube) {
        return aclUtil.hasProjectWritePermission(cube.getProjectInstance());
    }

    public boolean checkProjectOperationPermission(CubeInstance cube) {
        return aclUtil.hasProjectOperationPermission(cube.getProjectInstance());
    }

    //for job acl entity
    public boolean checkProjectReadPermission(JobInstance job) {
        return aclUtil.hasProjectReadPermission(getProjectByJob(job));
    }

    public boolean checkProjectWritePermission(JobInstance job) {
        return aclUtil.hasProjectWritePermission(getProjectByJob(job));
    }

    public boolean checkProjectOperationPermission(JobInstance job) {
        return aclUtil.hasProjectOperationPermission(getProjectByJob(job));
    }

    // ACL util's method, so that you can use AclEvaluate
    public String getCurrentUserName() {
        return aclUtil.getCurrentUserName();
    }

    public boolean hasCubeReadPermission(CubeInstance cube) {
        return hasProjectReadPermission(cube.getProjectInstance());
    }

    public boolean hasProjectReadPermission(ProjectInstance project) {
        return aclUtil.hasProjectReadPermission(project);
    }

    public boolean hasProjectOperationPermission(ProjectInstance project) {
        return aclUtil.hasProjectOperationPermission(project);
    }

    public boolean hasProjectWritePermission(ProjectInstance project) {
        return aclUtil.hasProjectWritePermission(project);
    }

    public boolean hasProjectAdminPermission(ProjectInstance project) {
        return aclUtil.hasProjectAdminPermission(project);
    }

    public boolean checkIsGlobalAdmin() {
        return aclUtil.checkIsGlobalAdmin();
    }

}