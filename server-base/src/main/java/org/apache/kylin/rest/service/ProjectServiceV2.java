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
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.util.AclUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
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

    @Autowired
    @Qualifier("cubeMgmtServiceV2")
    private CubeServiceV2 cubeServiceV2;

    @Autowired
    private AclUtil aclUtil;

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

    public List<ProjectInstance> getReadableProjects() {
        List<ProjectInstance> readableProjects = new ArrayList<ProjectInstance>();

        //list all projects first
        List<ProjectInstance> projectInstances = getProjectManager().listAllProjects();

        //get user infomation
        UserDetails userDetails = aclUtil.getCurrentUser();
        String userName = userDetails.getUsername();

        //check if ROLE_ADMIN return all,also get user role list
        List<String> userAuthority = aclUtil.getAuthorityList();
        for (String auth : userAuthority) {
            if (auth.equals(Constant.ROLE_ADMIN)) {
                return projectInstances;
            }
        }

        for (ProjectInstance projectInstance : projectInstances) {
            if (projectInstance == null) {
                continue;
            }

            boolean hasProjectPermission = false;
            AclEntity ae = accessService.getAclEntity("ProjectInstance", projectInstance.getId());
            Acl projectAcl = accessService.getAcl(ae);
            //project no Acl info will be skipped
            if (projectAcl != null) {

                //project owner has permission
                if (((PrincipalSid) projectAcl.getOwner()).getPrincipal().equals(userName)) {
                    readableProjects.add(projectInstance);
                    continue;
                }

                //check project permission and role
                for (AccessControlEntry ace : projectAcl.getEntries()) {
                    if (ace.getSid() instanceof PrincipalSid && ((PrincipalSid) ace.getSid()).getPrincipal().equals(userName)) {
                        hasProjectPermission = true;
                        readableProjects.add(projectInstance);
                        break;

                    } else if (ace.getSid() instanceof GrantedAuthoritySid) {
                        String projectAuthority = ((GrantedAuthoritySid) ace.getSid()).getGrantedAuthority();
                        if (userAuthority.contains(projectAuthority)) {
                            hasProjectPermission = true;
                            readableProjects.add(projectInstance);
                            break;
                        }

                    }

                }
            }

            if (!hasProjectPermission) {
                List<CubeInstance> cubeInstances = cubeServiceV2.listAllCubes(projectInstance.getName());

                for (CubeInstance cubeInstance : cubeInstances) {
                    if (cubeInstance == null) {
                        continue;
                    }

                    if (aclUtil.isHasCubePermission(cubeInstance)) {
                        hasProjectPermission = true;
                        break;
                    }
                }
                if (hasProjectPermission) {
                    readableProjects.add(projectInstance);
                }
            }

        }
        return readableProjects;
    }
}
