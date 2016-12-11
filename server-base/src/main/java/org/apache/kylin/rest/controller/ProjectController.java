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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.request.ProjectRequest;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.ProjectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author xduo
 */
@Controller
@RequestMapping(value = "/projects")
public class ProjectController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(ProjectController.class);

    private static final char[] VALID_PROJECTNAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_".toCharArray();

    @Autowired
    private ProjectService projectService;
    @Autowired
    private AccessService accessService;
    @Autowired
    private CubeService cubeService;

    /**
     * Get available project list
     *
     * @return Table metadata array
     * @throws IOException
     */
    @RequestMapping(value = "", method = { RequestMethod.GET })
    @ResponseBody
    public List<ProjectInstance> getProjects(@RequestParam(value = "limit", required = false) Integer limit, @RequestParam(value = "offset", required = false) Integer offset) {
        return projectService.listProjects(limit, offset);
    }

    @RequestMapping(value = "/readable", method = { RequestMethod.GET })
    @ResponseBody
    public List<ProjectInstance> getReadableProjects(@RequestParam(value = "limit", required = false) Integer limit, @RequestParam(value = "offset", required = false) Integer offset) {
        List<ProjectInstance> readableProjects = new ArrayList<ProjectInstance>();
        //list all projects first
        List<ProjectInstance> projectInstances = projectService.listAllProjects(limit, offset);

        //get user infomation
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        UserDetails userDetails = null;
        if (authentication == null) {
            logger.debug("authentication is null.");
            throw new InternalErrorException("Can not find authentication infomation.");
        }
        if (authentication.getPrincipal() instanceof UserDetails) {
            logger.debug("authentication.getPrincipal() is " + authentication.getPrincipal());
            userDetails = (UserDetails) authentication.getPrincipal();
        }
        if (authentication.getDetails() instanceof UserDetails) {
            logger.debug("authentication.getDetails() is " + authentication.getDetails());
            userDetails = (UserDetails) authentication.getDetails();
        }

        //check if ROLE_ADMIN return all,also get user role list
        List<String> userAuthority = new ArrayList<>();
        for (GrantedAuthority auth : authentication.getAuthorities()) {
            userAuthority.add(auth.getAuthority());
            if (auth.getAuthority().equals(Constant.ROLE_ADMIN))
                return projectInstances;
        }
        String userName = userDetails.getUsername();
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
                List<CubeInstance> cubeInstances = cubeService.listAllCubes(projectInstance.getName());

                for (CubeInstance cubeInstance : cubeInstances) {
                    if (cubeInstance == null) {
                        continue;
                    }
                    boolean hasCubePermission = false;
                    AclEntity cubeAe = accessService.getAclEntity("CubeInstance", cubeInstance.getId());
                    Acl cubeAcl = accessService.getAcl(cubeAe);
                    //cube no Acl info will not be used to filter project
                    if (cubeAcl != null) {
                        //cube owner will have permission to read project
                        if (((PrincipalSid) cubeAcl.getOwner()).getPrincipal().equals(userName)) {
                            hasProjectPermission = true;
                            break;
                        }
                        for (AccessControlEntry cubeAce : cubeAcl.getEntries()) {

                            if (cubeAce.getSid() instanceof PrincipalSid && ((PrincipalSid) cubeAce.getSid()).getPrincipal().equals(userName)) {
                                hasCubePermission = true;
                                break;
                            } else if (cubeAce.getSid() instanceof GrantedAuthoritySid) {
                                String cubeAuthority = ((GrantedAuthoritySid) cubeAce.getSid()).getGrantedAuthority();
                                if (userAuthority.contains(cubeAuthority)) {
                                    hasCubePermission = true;
                                    break;
                                }

                            }
                        }
                    }
                    if (hasCubePermission) {
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

    @RequestMapping(value = "", method = { RequestMethod.POST })
    @ResponseBody

    public ProjectInstance saveProject(@RequestBody ProjectRequest projectRequest) {
        ProjectInstance projectDesc = deserializeProjectDesc(projectRequest);

        if (StringUtils.isEmpty(projectDesc.getName())) {
            throw new InternalErrorException("A project name must be given to create a project");
        }

        if (!StringUtils.containsOnly(projectDesc.getName(), VALID_PROJECTNAME)) {
            logger.info("Invalid Project name {}, only letters, numbers and underline supported.", projectDesc.getName());
            throw new BadRequestException("Invalid Project name, only letters, numbers and underline supported.");
        }

        ProjectInstance createdProj = null;
        try {
            createdProj = projectService.createProject(projectDesc);
        } catch (Exception e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }

        return createdProj;
    }

    @RequestMapping(value = "", method = { RequestMethod.PUT })
    @ResponseBody
    public ProjectInstance updateProject(@RequestBody ProjectRequest projectRequest) {
        String formerProjectName = projectRequest.getFormerProjectName();
        if (StringUtils.isEmpty(formerProjectName)) {
            throw new InternalErrorException("A project name must be given to update a project");
        }

        ProjectInstance projectDesc = deserializeProjectDesc(projectRequest);

        ProjectInstance updatedProj = null;
        try {
            ProjectInstance currentProject = projectService.getProjectManager().getProject(formerProjectName);
            if (currentProject == null) {
                throw new InternalErrorException("The project named " + formerProjectName + " does not exists");
            }

            updatedProj = projectService.updateProject(projectDesc, currentProject);
        } catch (Exception e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }

        return updatedProj;
    }

    private ProjectInstance deserializeProjectDesc(ProjectRequest projectRequest) {
        ProjectInstance projectDesc = null;
        try {
            logger.debug("Saving project " + projectRequest.getProjectDescData());
            projectDesc = JsonUtil.readValue(projectRequest.getProjectDescData(), ProjectInstance.class);
        } catch (Exception e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }
        return projectDesc;
    }

    @RequestMapping(value = "/{projectName}", method = { RequestMethod.DELETE })
    @ResponseBody
    public void deleteProject(@PathVariable String projectName) {
        try {

            ProjectInstance project = projectService.getProjectManager().getProject(projectName);
            projectService.deleteProject(projectName, project);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to delete project. " + " Caused by: " + e.getMessage(), e);
        }
    }

    public void setProjectService(ProjectService projectService) {
        this.projectService = projectService;
    }

    public void setAccessService(AccessService accessService) {
        this.accessService = accessService;
    }

    public void setCubeService(CubeService cubeService) {
        this.cubeService = cubeService;
    }
}
