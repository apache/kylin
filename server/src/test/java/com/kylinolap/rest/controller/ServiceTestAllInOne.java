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
package com.kylinolap.rest.controller;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import net.sf.ehcache.CacheManager;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.job.JobInstance;
import com.kylinolap.metadata.project.ProjectInstance;
import com.kylinolap.metadata.project.ProjectManager;
import com.kylinolap.rest.exception.BadRequestException;
import com.kylinolap.rest.exception.ForbiddenException;
import com.kylinolap.rest.exception.InternalErrorException;
import com.kylinolap.rest.exception.NotFoundException;
import com.kylinolap.rest.request.AccessRequest;
import com.kylinolap.rest.request.CreateProjectRequest;
import com.kylinolap.rest.request.CubeRequest;
import com.kylinolap.rest.request.JobBuildRequest;
import com.kylinolap.rest.request.JobListRequest;
import com.kylinolap.rest.request.MetaRequest;
import com.kylinolap.rest.request.SQLRequest;
import com.kylinolap.rest.request.UpdateProjectRequest;
import com.kylinolap.rest.response.AccessEntryResponse;
import com.kylinolap.rest.response.ErrorResponse;
import com.kylinolap.rest.service.AccessService;
import com.kylinolap.rest.service.AdminService;
import com.kylinolap.rest.service.CubeService;
import com.kylinolap.rest.service.JobService;
import com.kylinolap.rest.service.ProjectService;
import com.kylinolap.rest.service.QueryService;
import com.kylinolap.rest.service.ServiceTestBase;
import com.kylinolap.rest.util.QueryUtil;

/**
 * 
 * @author shaoshi
 *
 */
public class ServiceTestAllInOne extends ServiceTestBase {
    private AccessController accessController;
    private AdminController adminController;

    @Autowired
    AccessService accessService;
    @Autowired
    private AdminService adminService;
    @Autowired
    private CubeService cubeService;

    @Test
    public void testAdminControllerBasics() throws IOException {
        adminController = new AdminController();
        adminController.setAdminService(adminService);
        adminController.setCubeMgmtService(cubeService);
        Assert.assertNotNull(adminController.getConfig());
        Assert.assertNotNull(adminController.getEnv());
    }
    

    @Test
    public void testAccessControlBasics() throws IOException {
        accessController = new AccessController();
        accessController.setAccessService(accessService);
        
        List<AccessEntryResponse> aes = accessController.getAccessEntities("CubeInstance", "a24ca905-1fc6-4f67-985c-38fa5aeafd92");
        Assert.assertTrue(aes.size() == 0);

        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setPermission("ADMINISTRATION");
        accessRequest.setSid("MODELER");
        accessRequest.setPrincipal(true);

        aes = accessController.grant("CubeInstance", "a24ca905-1fc6-4f67-985c-38fa5aeafd92", accessRequest);
        Assert.assertTrue(aes.size() == 1);

        Long aeId = null;
        for (AccessEntryResponse ae : aes) {
            aeId = (Long) ae.getId();
        }
        Assert.assertNotNull(aeId);

        accessRequest = new AccessRequest();
        accessRequest.setAccessEntryId(aeId);
        accessRequest.setPermission("READ");

        aes = accessController.update("CubeInstance", "a24ca905-1fc6-4f67-985c-38fa5aeafd92", accessRequest);
        Assert.assertTrue(aes.size() == 1);
        for (AccessEntryResponse ae : aes) {
            aeId = (Long) ae.getId();
        }
        Assert.assertNotNull(aeId);

        accessRequest = new AccessRequest();
        accessRequest.setAccessEntryId(aeId);
        accessRequest.setPermission("READ");
        aes = accessController.revoke("CubeInstance", "a24ca905-1fc6-4f67-985c-38fa5aeafd92", accessRequest);
        Assert.assertTrue(aes.size() == 0);
    }
    
    private BasicController basicController;

    @Test
    public void testBasicControllerBasics() throws IOException {
        basicController = new BasicController();
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRequestURI("http://localhost");

        NotFoundException notFoundException = new NotFoundException("not found");
        ErrorResponse errorResponse = basicController.handleBadRequest(request, notFoundException);
        Assert.assertNotNull(errorResponse);

        ForbiddenException forbiddenException = new ForbiddenException("forbidden");
        errorResponse = basicController.handleForbidden(request, forbiddenException);
        Assert.assertNotNull(errorResponse);

        InternalErrorException internalErrorException = new InternalErrorException("error");
        errorResponse = basicController.handleInternalError(request, internalErrorException);
        Assert.assertNotNull(errorResponse);

        BadRequestException badRequestException = new BadRequestException("error");
        errorResponse = basicController.handleBadRequest(request, badRequestException);
        Assert.assertNotNull(errorResponse);
    }
    
    private CubeController cubeController;
    private CubeDescController cubeDescController;

    @Autowired
    JobService jobService;

    @Test
    public void testCubeControllerBasics() throws IOException {

        cubeController = new CubeController();
        cubeController.setCubeService(cubeService);
        cubeController.setJobService(jobService);
        cubeDescController = new CubeDescController();
        cubeDescController.setCubeService(cubeService);
        
        CubeDesc[] cubes = (CubeDesc[]) cubeDescController.getCube("test_kylin_cube_with_slr_ready");
        Assert.assertNotNull(cubes);
        Assert.assertNotNull(cubeController.getSql("test_kylin_cube_with_slr_ready", "20130331080000_20131212080000"));
        Assert.assertNotNull(cubeController.getCubes(null, null, 0, 5));

        CubeDesc cube = cubes[0];
        CubeDesc newCube = new CubeDesc();
        String newCubeName = cube.getName() + "_test_save";
        newCube.setName(newCubeName);
        newCube.setModelName(cube.getModelName());
        newCube.setModel(cube.getModel());
        newCube.setDimensions(cube.getDimensions());
        newCube.setHBaseMapping(cube.getHBaseMapping());
        newCube.setMeasures(cube.getMeasures());
        newCube.setConfig(cube.getConfig());
        newCube.setRowkey(cube.getRowkey());

        ObjectMapper mapper = new ObjectMapper();
        StringWriter stringWriter = new StringWriter();
        mapper.writeValue(stringWriter, newCube);

        CubeRequest cubeRequest = new CubeRequest();
        cubeRequest.setCubeDescData(stringWriter.toString());
        cubeRequest = cubeController.saveCubeDesc(cubeRequest);

        cubeController.deleteCube(newCubeName);
    }
    
    private JobController jobSchedulerController;


    @Test
    public void testJobControllerBasics() throws IOException {

        jobSchedulerController = new JobController();
        jobSchedulerController.setJobService(jobService);
        cubeController = new CubeController();
        cubeController.setJobService(jobService);
        cubeController.setCubeService(cubeService);
        
        JobListRequest jobRequest = new JobListRequest();
        Assert.assertNotNull(jobSchedulerController.list(jobRequest));

        JobInstance job = null;
        try {
            JobBuildRequest jobBuildRequest = new JobBuildRequest();
            jobBuildRequest.setBuildType("BUILD");
            jobBuildRequest.setStartTime(1386806400000L);
            jobBuildRequest.setEndTime(new Date().getTime());
            job = cubeController.rebuild("test_kylin_cube_with_slr_ready", jobBuildRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Assert.assertNotNull(job);
        Assert.assertNotNull(jobSchedulerController.get(job.getId()));
        Map<String, String> output = jobSchedulerController.getStepOutput(job.getId(), 0);
        Assert.assertNotNull(output);

        // jobSchedulerController.cancel(job.getId());
    }

    //@Test(expected = RuntimeException.class)
    public void testJobControllerResume() throws IOException {

        jobSchedulerController = new JobController();
        jobSchedulerController.setJobService(jobService);
        cubeController = new CubeController();
        cubeController.setJobService(jobService);
        cubeController.setCubeService(cubeService);
        
        JobBuildRequest jobBuildRequest = new JobBuildRequest();
        jobBuildRequest.setBuildType("BUILD");
        jobBuildRequest.setStartTime(20130331080000L);
        jobBuildRequest.setEndTime(20131212080000L);
        JobInstance job = cubeController.rebuild("test_kylin_cube_with_slr_ready", jobBuildRequest);

        Assert.assertNotNull(job);
        jobSchedulerController.resume(job.getId());
    }
    
    private ProjectController projectController;

    @Autowired
    ProjectService projectService;

    @Test
    public void testAddUpdateProject() throws IOException {
        projectController = new ProjectController();
        projectController.setProjectService(projectService);
        
        try {
            projectController.deleteProject("new_project");
        } catch (InternalErrorException e) {
            //project doesn't exist
        }
        try {
            projectController.deleteProject("new_project_2");
        } catch (InternalErrorException e) {
            //project doesn't exist
        }

        List<ProjectInstance> projects = projectController.getProjects(null, null);

        int originalProjectCount = projects.size();
        CreateProjectRequest request = new CreateProjectRequest();
        request.setName("new_project");
        ProjectInstance ret = projectController.saveProject(request);

        Assert.assertEquals(ret.getOwner(), "ADMIN");
        Assert.assertEquals(ProjectManager.getInstance(this.getTestConfig()).listAllProjects().size(), originalProjectCount + 1);

        UpdateProjectRequest updateR = new UpdateProjectRequest();
        updateR.setFormerProjectName("new_project");
        updateR.setNewProjectName("new_project_2");
        projectController.updateProject(updateR);

        Assert.assertEquals(ProjectManager.getInstance(this.getTestConfig()).listAllProjects().size(), originalProjectCount + 1);
        Assert.assertEquals(ProjectManager.getInstance(this.getTestConfig()).getProject("new_project"), null);

        Assert.assertNotEquals(ProjectManager.getInstance(this.getTestConfig()).getProject("new_project_2"), null);

        // only update desc:
        updateR = new UpdateProjectRequest();
        updateR.setFormerProjectName("new_project_2");
        updateR.setNewProjectName("new_project_2");
        updateR.setNewDescription("hello world");
        projectController.updateProject(updateR);

        Assert.assertEquals(ProjectManager.getInstance(this.getTestConfig()).listAllProjects().size(), originalProjectCount + 1);
        Assert.assertEquals(ProjectManager.getInstance(this.getTestConfig()).getProject("new_project"), null);
        Assert.assertNotEquals(ProjectManager.getInstance(this.getTestConfig()).getProject("new_project_2"), null);
        Assert.assertEquals(ProjectManager.getInstance(this.getTestConfig()).getProject("new_project_2").getDescription(), "hello world");
    }

    @Test(expected = InternalErrorException.class)
    public void testAddExistingProject() throws IOException {
        projectController = new ProjectController();
        projectController.setProjectService(projectService);
        
        CreateProjectRequest request = new CreateProjectRequest();
        request.setName("default");
        projectController.saveProject(request);
    }
    
    private QueryController queryController;
    @Autowired
    QueryService queryService;
    @Autowired
    private CacheManager cacheManager;

    @Test(expected = Exception.class)
    public void testQueryException() throws Exception {
        queryController = new QueryController();
        queryController.setQueryService(queryService);
        queryController.setCacheManager(cacheManager);
        
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setSql("select * from not_exist_table");
        sqlRequest.setProject("default");
        queryController.query(sqlRequest);
    }

    @Test
    public void testErrorMsg() {
        String errorMsg = "error while executing SQL \"select lkp.clsfd_ga_prfl_id, ga.sum_dt, sum(ga.bounces) as bounces, sum(ga.exits) as exits, sum(ga.entrances) as entrances, sum(ga.pageviews) as pageviews, count(distinct ga.GA_VSTR_ID, ga.GA_VST_ID) as visits, count(distinct ga.GA_VSTR_ID) as uniqVistors from CLSFD_GA_PGTYPE_CATEG_LOC ga left join clsfd_ga_prfl_lkp lkp on ga.SRC_GA_PRFL_ID = lkp.SRC_GA_PRFL_ID group by lkp.clsfd_ga_prfl_id,ga.sum_dt order by lkp.clsfd_ga_prfl_id,ga.sum_dt LIMIT 50000\": From line 14, column 14 to line 14, column 29: Column 'CLSFD_GA_PRFL_ID' not found in table 'LKP'";
        assert QueryUtil.makeErrorMsgUserFriendly(errorMsg).equals("From line 14, column 14 to line 14, column 29: Column 'CLSFD_GA_PRFL_ID' not found in table 'LKP'\n" + "while executing SQL: \"select lkp.clsfd_ga_prfl_id, ga.sum_dt, sum(ga.bounces) as bounces, sum(ga.exits) as exits, sum(ga.entrances) as entrances, sum(ga.pageviews) as pageviews, count(distinct ga.GA_VSTR_ID, ga.GA_VST_ID) as visits, count(distinct ga.GA_VSTR_ID) as uniqVistors from CLSFD_GA_PGTYPE_CATEG_LOC ga left join clsfd_ga_prfl_lkp lkp on ga.SRC_GA_PRFL_ID = lkp.SRC_GA_PRFL_ID group by lkp.clsfd_ga_prfl_id,ga.sum_dt order by lkp.clsfd_ga_prfl_id,ga.sum_dt LIMIT 50000\"");
    }

    @Test
    public void testGetMetadata() {
        queryController = new QueryController();
        queryController.setQueryService(queryService);
        queryController.setCacheManager(cacheManager);
        
        queryController.getMetadata(new MetaRequest(ProjectInstance.DEFAULT_PROJECT_NAME));
    }
    
    private UserController userController;


    @Test
    public void testUserControllerBasics() throws IOException {
        List<GrantedAuthority> authorities = new ArrayList<GrantedAuthority>();
        User user = new User("ADMIN", "ADMIN", authorities);
        Authentication authentication = new TestingAuthenticationToken(user, "ADMIN", "ROLE_ADMIN");
        SecurityContextHolder.getContext().setAuthentication(authentication);
 
        
        userController = new UserController();
        
        UserDetails userdetail = userController.authenticate();
        Assert.assertNotNull(userdetail);
        Assert.assertTrue(user.getUsername().equals("ADMIN"));
    }
}
