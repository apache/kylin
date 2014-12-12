/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.rest.service;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.kylinolap.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.kylinolap.rest.constant.Constant;
import com.kylinolap.rest.exception.InternalErrorException;
import com.kylinolap.rest.request.CreateProjectRequest;
import com.kylinolap.rest.request.UpdateProjectRequest;
import com.kylinolap.rest.security.AclPermission;

/**
 * @author xduo
 * 
 */
@Component("projectService")
public class ProjectService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(ProjectService.class);

    @Autowired
    private AccessService accessService;

    public ProjectInstance createProject(CreateProjectRequest projectRequest) throws IOException {
        String projectName = projectRequest.getName();
        String description = projectRequest.getDescription();
        ProjectInstance currentProject = getProjectManager().getProject(projectName);

        if (currentProject != null) {
            throw new InternalErrorException("The project named " + projectName + " already exists");
        }
        String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        ProjectInstance createdProject = getProjectManager().createProject(projectName, owner, description);
        getCubeRealizationManager().loadProject(createdProject);
        accessService.init(createdProject, AclPermission.ADMINISTRATION);
        logger.debug("New project created.");

        return createdProject;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
    public ProjectInstance updateProject(UpdateProjectRequest projectRequest) throws IOException {
        String formerProjectName = projectRequest.getFormerProjectName();
        String newProjectName = projectRequest.getNewProjectName();
        String newDescription = projectRequest.getNewDescription();

        ProjectInstance currentProject = getProjectManager().getProject(formerProjectName);

        if (currentProject == null) {
            throw new InternalErrorException("The project named " + formerProjectName + " does not exists");
        }

        ProjectInstance updatedProject = getProjectManager().updateProject(currentProject, newProjectName, newDescription);

        logger.debug("Project updated.");

        return updatedProject;
    }

    public List<ProjectInstance> listAllProjects(final Integer limit, final Integer offset) {
        List<ProjectInstance> projects = getProjectManager().listAllProjects();

        int climit = (null == limit) ? 30 : limit;
        int coffset = (null == offset) ? 0 : offset;

        if (projects.size() <= coffset) {
            return Collections.emptyList();
        }

        if ((projects.size() - coffset) < climit) {
            return projects.subList(coffset, projects.size());
        }

        return projects.subList(coffset, coffset + climit);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
    public void deleteProject(String projectName) throws IOException {
        ProjectInstance project = getProjectManager().getProject(projectName);
        getProjectManager().dropProject(projectName);
        getCubeRealizationManager().unloadProject(project);

        accessService.clean(project, true);
    }

    /**
     * @param name
     * @throws IOException
     */
    public void reloadProjectCache(String name) throws IOException {
        ProjectInstance project = this.getProjectManager().getProject(name);
        this.getProjectManager().loadProjectCache(project, false);
    }

    /**
     * @param name
     */
    public void removeProjectCache(String name) {
        ProjectInstance project = this.getProjectManager().getProject(name);
        this.getProjectManager().removeProjectCache(project);
    }

}
