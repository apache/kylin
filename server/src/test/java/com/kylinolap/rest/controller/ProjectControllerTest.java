package com.kylinolap.rest.controller;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.kylinolap.cube.project.ProjectInstance;
import com.kylinolap.cube.project.ProjectManager;
import com.kylinolap.rest.exception.InternalErrorException;
import com.kylinolap.rest.request.CreateProjectRequest;
import com.kylinolap.rest.request.UpdateProjectRequest;
import com.kylinolap.rest.service.ProjectService;
import com.kylinolap.rest.service.ServiceTestBase;

/**
 * Created by honma on 8/7/14.
 */
public class ProjectControllerTest extends ServiceTestBase {

    private ProjectController projectController;

    @Autowired
    ProjectService projectService;

    @Before
    public void setup() {
        super.setUp();

        projectController = new ProjectController();
        projectController.setProjectService(projectService);
    }

    @Test
    public void testAddUpdateProject() throws IOException {

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
        CreateProjectRequest request = new CreateProjectRequest();
        request.setName("default");
        projectController.saveProject(request);
    }
}
