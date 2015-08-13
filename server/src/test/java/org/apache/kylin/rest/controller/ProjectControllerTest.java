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
import java.util.List;

import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.request.CreateProjectRequest;
import org.apache.kylin.rest.request.UpdateProjectRequest;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 */
public class ProjectControllerTest extends ServiceTestBase {

    private ProjectController projectController;

    @Autowired
    ProjectService projectService;

    @Before
    public void setup() throws Exception {
        super.setup();

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

    }

    @Test
    public void testAddUpdateProject() throws IOException {

        List<ProjectInstance> projects = projectController.getProjects(null, null);

        int originalProjectCount = projects.size();
        CreateProjectRequest request = new CreateProjectRequest();
        request.setName("new_project");
        ProjectInstance ret = projectController.saveProject(request);

        Assert.assertEquals(ret.getOwner(), "ADMIN");
        Assert.assertEquals(ProjectManager.getInstance(getTestConfig()).listAllProjects().size(), originalProjectCount + 1);

        UpdateProjectRequest updateR = new UpdateProjectRequest();
        updateR.setFormerProjectName("new_project");
        updateR.setNewProjectName("new_project_2");
        projectController.updateProject(updateR);

        Assert.assertEquals(ProjectManager.getInstance(getTestConfig()).listAllProjects().size(), originalProjectCount + 1);
        Assert.assertEquals(ProjectManager.getInstance(getTestConfig()).getProject("new_project"), null);

        Assert.assertNotEquals(ProjectManager.getInstance(getTestConfig()).getProject("new_project_2"), null);

        // only update desc:
        updateR = new UpdateProjectRequest();
        updateR.setFormerProjectName("new_project_2");
        updateR.setNewProjectName("new_project_2");
        updateR.setNewDescription("hello world");
        projectController.updateProject(updateR);

        Assert.assertEquals(ProjectManager.getInstance(getTestConfig()).listAllProjects().size(), originalProjectCount + 1);
        Assert.assertEquals(ProjectManager.getInstance(getTestConfig()).getProject("new_project"), null);
        Assert.assertNotEquals(ProjectManager.getInstance(getTestConfig()).getProject("new_project_2"), null);
        Assert.assertEquals(ProjectManager.getInstance(getTestConfig()).getProject("new_project_2").getDescription(), "hello world");
    }

    @Test(expected = InternalErrorException.class)
    public void testAddExistingProject() throws IOException {
        CreateProjectRequest request = new CreateProjectRequest();
        request.setName("default");
        projectController.saveProject(request);
    }
}
