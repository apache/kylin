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
import java.io.StringWriter;

import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.request.ProjectRequest;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.kylin.rest.util.ValidateUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 */
public class ProjectControllerTest extends ServiceTestBase {

    private ProjectController projectController;

    @Autowired
    @Qualifier("projectService")
    ProjectService projectService;

    @Autowired
    @Qualifier("validateUtil")
    private ValidateUtil validateUtil;

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Before
    public void setup() throws Exception {
        super.setup();

        projectController = new ProjectController();
        projectController.setProjectService(projectService);
        projectController.setValidateUtil(validateUtil);
        projectController.setAccessService(accessService);

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
        try {
            projectController.deleteProject("new_project_3");
        } catch (InternalErrorException e) {
            //project doesn't exist
        }
    }

    @Test
    public void testAddUpdateProject() throws IOException {
        int originalProjectCount = projectController.getProjects(null, null).size();

        //test add project
        ProjectInstance project = new ProjectInstance();
        project.setName("new_project");
        ProjectInstance ret = projectController.saveProject(getProjectRequest(project, null));

        Assert.assertEquals(ret.getOwner(), "ADMIN");
        Assert.assertEquals(ProjectManager.getInstance(getTestConfig()).listAllProjects().size(), originalProjectCount + 1);

        //test update project description only
        ProjectInstance newProject2 = new ProjectInstance();
        newProject2.setName("new_project");
        newProject2.setDescription("hello world");
        projectController.updateProject(getProjectRequest(newProject2, "new_project"));

        Assert.assertEquals(ProjectManager.getInstance(getTestConfig()).listAllProjects().size(), originalProjectCount + 1);
        Assert.assertNotEquals(ProjectManager.getInstance(getTestConfig()).getProject("new_project"), null);
        Assert.assertEquals(ProjectManager.getInstance(getTestConfig()).getProject("new_project").getDescription(), "hello world");
    }

    @Test
    public void testUpdateProjectOwner() throws IOException {
        int originalProjectCount = projectController.getProjects(null, null).size();

        //test add project
        ProjectInstance project = new ProjectInstance();
        project.setName("new_project_3");
        ProjectInstance ret = projectController.saveProject(getProjectRequest(project, null));

        Assert.assertEquals(ret.getOwner(), "ADMIN");
        Assert.assertEquals(ProjectManager.getInstance(getTestConfig()).listAllProjects().size(), originalProjectCount + 1);

        //test update project owner only
        try {
            projectController.updateProjectOwner("new_project_3", "new_user");
        } catch (InternalErrorException e) {
            Assert.assertTrue(e.getMessage().equals("Operation failed, user:new_user not exists, please add first."));
        }
        projectController.updateProjectOwner("new_project_3", "MODELER");

        Assert.assertEquals(ProjectManager.getInstance(getTestConfig()).listAllProjects().size(), originalProjectCount + 1);
        Assert.assertNotEquals(ProjectManager.getInstance(getTestConfig()).getProject("new_project_3"), null);
        Assert.assertEquals(ProjectManager.getInstance(getTestConfig()).getProject("new_project_3").getOwner(), "MODELER");
    }

    @Test(expected = InternalErrorException.class)
    public void testAddExistingProject() throws IOException {
        ProjectInstance newProject = new ProjectInstance();
        newProject.setName("default");

        projectController.saveProject(getProjectRequest(newProject, null));
    }

    private ProjectRequest getProjectRequest(ProjectInstance project, String formerProjectName) throws IOException {
        ProjectRequest request = new ProjectRequest();
        request.setProjectDescData(getProjectDescData(project));
        request.setFormerProjectName(formerProjectName);

        return request;
    }

    private String getProjectDescData(ProjectInstance project) throws IOException {
        ObjectMapper projectMapper = new ObjectMapper();
        StringWriter projectWriter = new StringWriter();
        projectMapper.writeValue(projectWriter, project);

        System.err.println(projectWriter.toString());

        return projectWriter.toString();
    }
}
