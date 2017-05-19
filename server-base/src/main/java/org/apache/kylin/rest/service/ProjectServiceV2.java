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

package org.apache.kylin.rest.service;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.security.AclPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * Created by luwei on 17-4-20.
 */
@Component("projectServiceV2")
public class ProjectServiceV2 extends ProjectService {

    private static final Logger logger = LoggerFactory.getLogger(ProjectServiceV2.class);

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public ProjectInstance createProject(ProjectInstance newProject) throws IOException {
        Message msg = MsgPicker.getMsg();

        String projectName = newProject.getName();
        String description = newProject.getDescription();
        LinkedHashMap<String, String> overrideProps = newProject.getOverrideKylinProps();

        ProjectInstance currentProject = getProjectManager().getProject(projectName);

        if (currentProject != null) {
            throw new BadRequestException(String.format(msg.getPROJECT_ALREADY_EXIST(), projectName));
        }
        String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        ProjectInstance createdProject = getProjectManager().createProject(projectName, owner, description, overrideProps);
        accessService.init(createdProject, AclPermission.ADMINISTRATION);
        logger.debug("New project created.");

        return createdProject;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION') or hasPermission(#project, 'MANAGEMENT')")
    public void deleteProject(String projectName, ProjectInstance project) throws IOException {
        getProjectManager().dropProject(projectName);

        accessService.clean(project, true);
    }

    public String getProjectOfModel(String modelName) {
        List<ProjectInstance> projectInstances = listProjects(null, null);
        for (ProjectInstance projectInstance : projectInstances) {
            if (projectInstance.containsModel(modelName))
                return projectInstance.getName();
        }
        return null;
    }


}
