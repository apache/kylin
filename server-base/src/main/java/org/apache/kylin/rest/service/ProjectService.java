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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.directory.api.util.Strings;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PostFilter;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 * @author xduo
 * 
 */
@Component("projectService")
public class ProjectService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(ProjectService.class);

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @Autowired
    private AclEvaluate aclEvaluate;

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
        ProjectInstance createdProject = getProjectManager().createProject(projectName, owner, description,
                overrideProps);
        accessService.init(createdProject, AclPermission.ADMINISTRATION);
        logger.debug("New project created.");

        return createdProject;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#currentProject, 'ADMINISTRATION')")
    public ProjectInstance updateProject(ProjectInstance newProject, ProjectInstance currentProject)
            throws IOException {

        String newProjectName = newProject.getName();
        String newDescription = newProject.getDescription();
        LinkedHashMap<String, String> overrideProps = newProject.getOverrideKylinProps();

        ProjectInstance updatedProject = getProjectManager().updateProject(currentProject, newProjectName,
                newDescription, overrideProps);

        logger.debug("Project updated.");
        return updatedProject;
    }

    @PostFilter(Constant.ACCESS_POST_FILTER_READ)
    public List<ProjectInstance> listProjects(final Integer limit, final Integer offset) {
        List<ProjectInstance> projects = listAllProjects(limit, offset);
        return projects;
    }

    @Deprecated
    public List<ProjectInstance> listAllProjects(final Integer limit, final Integer offset) {
        List<ProjectInstance> projects = getProjectManager().listAllProjects();

        int climit = (null == limit) ? Integer.MAX_VALUE : limit;
        int coffset = (null == offset) ? 0 : offset;

        if (projects.size() <= coffset) {
            return Collections.emptyList();
        }

        if ((projects.size() - coffset) < climit) {
            return projects.subList(coffset, projects.size());
        }

        return projects.subList(coffset, coffset + climit);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void deleteProject(String projectName, ProjectInstance project) throws IOException {
        getProjectManager().dropProject(projectName);

        accessService.clean(project, true);
    }

    public String getProjectOfCube(String cubeName) {
        for (ProjectInstance p : getProjectManager().listAllProjects()) {
            if (p.containsRealization(RealizationType.CUBE, cubeName))
                return p.getName();
        }
        return null;
    }

    public String getProjectOfModel(String modelName) {
        for (ProjectInstance p : getProjectManager().listAllProjects()) {
            if (p.containsModel(modelName))
                return p.getName();
        }
        return null;
    }

    public List<ProjectInstance> getReadableProjects() {
        return getReadableProjects(null);
    }

    public List<ProjectInstance> getReadableProjects(final String projectName) {
        List<ProjectInstance> readableProjects = new ArrayList<ProjectInstance>();

        //list all projects first
        List<ProjectInstance> projectInstances = getProjectManager().listAllProjects();

        for (ProjectInstance projectInstance : projectInstances) {

            if (projectInstance == null) {
                continue;
            }
            boolean hasProjectPermission = aclEvaluate.hasProjectReadPermission(projectInstance);
            if (hasProjectPermission) {
                readableProjects.add(projectInstance);
            }

        }

        // listAll method may not need a single param.But almost all listAll method pass
        if (!Strings.isEmpty(projectName)) {
            readableProjects = Lists
                    .newArrayList(Iterators.filter(readableProjects.iterator(), new Predicate<ProjectInstance>() {
                        @Override
                        public boolean apply(@Nullable ProjectInstance input) {
                            return input.getName().equals(projectName);
                        }
                    }));
        }

        return readableProjects;
    }
}
