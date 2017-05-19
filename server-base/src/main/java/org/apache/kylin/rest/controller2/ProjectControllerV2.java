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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.ProjectRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.CubeServiceV2;
import org.apache.kylin.rest.service.ProjectServiceV2;
import org.apache.kylin.rest.util.AclUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author xduo
 */
@Controller
@RequestMapping(value = "/projects")
public class ProjectControllerV2 extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(ProjectControllerV2.class);

    private static final char[] VALID_PROJECTNAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_".toCharArray();

    @Autowired
    @Qualifier("projectServiceV2")
    private ProjectServiceV2 projectServiceV2;

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    private AclUtil aclUtil;

    @Autowired
    @Qualifier("cubeMgmtServiceV2")
    private CubeServiceV2 cubeServiceV2;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getProjectsV2(@RequestHeader("Accept-Language") String lang, @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset, @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize) {
        MsgPicker.setMsg(lang);

        int offset = pageOffset * pageSize;
        int limit = pageSize;

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, projectServiceV2.listProjects(limit, offset), "");
    }

    @RequestMapping(value = "/readable", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getReadableProjectsV2(@RequestHeader("Accept-Language") String lang, @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset, @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize) {
        MsgPicker.setMsg(lang);

        HashMap<String, Object> data = new HashMap<String, Object>();

        List<ProjectInstance> readableProjects = new ArrayList<ProjectInstance>();
        int offset = pageOffset * pageSize;
        int limit = pageSize;

        //list all projects first
        List<ProjectInstance> projectInstances = projectServiceV2.getProjectManager().listAllProjects();

        if (projectInstances.size() <= offset) {
            offset = projectInstances.size();
            limit = 0;
        }

        if ((projectInstances.size() - offset) < limit) {
            limit = projectInstances.size() - offset;
        }

        //get user infomation
        UserDetails userDetails = aclUtil.getCurrentUser();
        String userName = userDetails.getUsername();

        //check if ROLE_ADMIN return all,also get user role list
        List<String> userAuthority = aclUtil.getAuthorityList();
        for (String auth : userAuthority) {
            if (auth.equals(Constant.ROLE_ADMIN)) {
                data.put("readableProjects", projectInstances.subList(offset, offset + limit));
                data.put("size", projectInstances.size());
                return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
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

        if (readableProjects.size() <= offset) {
            offset = readableProjects.size();
            limit = 0;
        }

        if ((readableProjects.size() - offset) < limit) {
            limit = readableProjects.size() - offset;
        }
        data.put("readableProjects", readableProjects.subList(offset, offset + limit));
        data.put("size", readableProjects.size());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse saveProjectV2(@RequestHeader("Accept-Language") String lang, @RequestBody ProjectRequest projectRequest) throws IOException {
        MsgPicker.setMsg(lang);
        Message msg = MsgPicker.getMsg();

        ProjectInstance projectDesc = deserializeProjectDescV2(projectRequest);

        if (StringUtils.isEmpty(projectDesc.getName())) {
            throw new BadRequestException(msg.getEMPTY_PROJECT_NAME());
        }

        if (!StringUtils.containsOnly(projectDesc.getName(), VALID_PROJECTNAME)) {
            logger.info("Invalid Project name {}, only letters, numbers and underline supported.", projectDesc.getName());
            throw new BadRequestException(String.format(msg.getINVALID_PROJECT_NAME(), projectDesc.getName()));
        }

        ProjectInstance createdProj = null;
        createdProj = projectServiceV2.createProject(projectDesc);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, createdProj, "");
    }

    @RequestMapping(value = "", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateProjectV2(@RequestHeader("Accept-Language") String lang, @RequestBody ProjectRequest projectRequest) throws IOException {
        MsgPicker.setMsg(lang);
        Message msg = MsgPicker.getMsg();

        String formerProjectName = projectRequest.getFormerProjectName();
        if (StringUtils.isEmpty(formerProjectName)) {
            throw new BadRequestException(msg.getEMPTY_PROJECT_NAME());
        }

        ProjectInstance projectDesc = deserializeProjectDescV2(projectRequest);

        ProjectInstance updatedProj = null;

        ProjectInstance currentProject = projectServiceV2.getProjectManager().getProject(formerProjectName);
        if (currentProject == null) {
            throw new BadRequestException(String.format(msg.getPROJECT_NOT_FOUND(), formerProjectName));
        }

        updatedProj = projectServiceV2.updateProject(projectDesc, currentProject);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, updatedProj, "");
    }

    private ProjectInstance deserializeProjectDescV2(ProjectRequest projectRequest) throws IOException {
        ProjectInstance projectDesc = null;
        logger.debug("Saving project " + projectRequest.getProjectDescData());
        projectDesc = JsonUtil.readValue(projectRequest.getProjectDescData(), ProjectInstance.class);
        return projectDesc;
    }

    @RequestMapping(value = "/{projectName}", method = { RequestMethod.DELETE }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void deleteProjectV2(@RequestHeader("Accept-Language") String lang, @PathVariable String projectName) throws IOException {
        MsgPicker.setMsg(lang);
        Message msg = MsgPicker.getMsg();

        ProjectInstance project = projectServiceV2.getProjectManager().getProject(projectName);
        projectServiceV2.deleteProject(projectName, project);
    }

}
