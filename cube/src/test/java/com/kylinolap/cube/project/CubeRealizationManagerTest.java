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

package com.kylinolap.cube.project;

import static org.junit.Assert.*;

import java.io.IOException;

import com.kylinolap.metadata.project.ProjectInstance;
import com.kylinolap.metadata.project.ProjectManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.CubeDescManager;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.metadata.MetadataManager;

/**
 * @author xduo
 * 
 */
public class CubeRealizationManagerTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        MetadataManager.removeInstance(this.getTestConfig());
        CubeManager.removeInstance(this.getTestConfig());
        CubeRealizationManager.removeInstance(this.getTestConfig());
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test(expected = IllegalStateException.class)
    public void testDropNonemptyProject1() throws IOException {
        ProjectInstance project = ProjectManager.getInstance(this.getTestConfig()).dropProject("DEFAULT");
        CubeRealizationManager.getInstance(this.getTestConfig()).unloadProject(project);
    }

    @Test(expected = IllegalStateException.class)
    public void testDropNonemptyProject2() throws IOException {
        ProjectInstance project = ProjectManager.getInstance(this.getTestConfig()).dropProject("DEFAULT???");
        CubeRealizationManager.getInstance(this.getTestConfig()).unloadProject(project);
    }

    @Test
    public void testNewProject() throws Exception {
        ProjectManager projectmanager = ProjectManager.getInstance(this.getTestConfig());
        CubeRealizationManager cubeRealizationManager = CubeRealizationManager.getInstance(this.getTestConfig());
        int originalProjectCount = projectmanager.listAllProjects().size();
        int originalCubeCount = CubeManager.getInstance(this.getTestConfig()).listAllCubes().size();
        int originalCubeCountInDefault = cubeRealizationManager.listAllCubes("default").size();
        ResourceStore store = getStore();

        // clean legacy in case last run failed
        store.deleteResource("/cube/cube_in_alien_project.json");

        CubeDescManager cubeDescMgr = getCubeDescManager();
        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_with_slr_desc");
        CubeInstance createdCube = CubeManager.getInstance(this.getTestConfig()).createCube("cube_in_alien_project", "alien", desc, null);
        assertTrue(createdCube == CubeManager.getInstance(this.getTestConfig()).getCube("cube_in_alien_project"));
        assertTrue(CubeRealizationManager.getInstance(getTestConfig()).listAllCubes("alien").contains(createdCube));

        System.out.println(JsonUtil.writeValueAsIndentString(createdCube));

        assertTrue(projectmanager.listAllProjects().size() == originalProjectCount + 1);
        assertTrue(cubeRealizationManager.listAllCubes("ALIEN").get(0).getName().equalsIgnoreCase("CUBE_IN_ALIEN_PROJECT"));
        assertTrue(CubeManager.getInstance(this.getTestConfig()).listAllCubes().size() == originalCubeCount + 1);

        projectmanager.updateCubeToProject("cube_in_alien_project", "default", null);
        CubeRealizationManager.getInstance(getTestConfig()).loadProject(projectmanager.getProject("default"));
        assertTrue(cubeRealizationManager.listAllCubes("ALIEN").size() == 0);
        assertTrue(cubeRealizationManager.listAllCubes("default").size() == originalCubeCountInDefault + 1);
        assertTrue(cubeRealizationManager.listAllCubes("default").contains(createdCube));

        projectmanager.updateCubeToProject("cube_in_alien_project", "alien", null);
        CubeRealizationManager.getInstance(getTestConfig()).loadProject(projectmanager.getProject("default"));
        assertTrue(cubeRealizationManager.listAllCubes("ALIEN").size() == 1);
        assertTrue(cubeRealizationManager.listAllCubes("default").size() == originalCubeCountInDefault);
        assertTrue(cubeRealizationManager.listAllCubes("alien").contains(createdCube));

        assertTrue(cubeRealizationManager.isCubeInProject("alien", createdCube));

        CubeInstance droppedCube = CubeManager.getInstance(this.getTestConfig()).dropCube("cube_in_alien_project", true);

        assertTrue(createdCube == droppedCube);
        assertNull(CubeManager.getInstance(this.getTestConfig()).getCube("cube_in_alien_project"));
        assertTrue(projectmanager.listAllProjects().size() == originalProjectCount + 1);
        assertTrue(CubeManager.getInstance(this.getTestConfig()).listAllCubes().size() == originalCubeCount);

        ProjectInstance project = projectmanager.dropProject("alien");
        CubeRealizationManager.getInstance(this.getTestConfig()).unloadProject(project);
        assertTrue(projectmanager.listAllProjects().size() == originalProjectCount);
    }

    @Test
    public void testExistingProject() throws Exception {
        int originalProjectCount = ProjectManager.getInstance(this.getTestConfig()).listAllProjects().size();
        int originalCubeCount = CubeManager.getInstance(this.getTestConfig()).listAllCubes().size();
        ResourceStore store = getStore();

        // clean legacy in case last run failed
        store.deleteResource("/cube/new_cube_in_default.json");

        CubeDescManager cubeDescMgr = getCubeDescManager();
        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_with_slr_desc");
        CubeInstance createdCube = CubeManager.getInstance(this.getTestConfig()).createCube("new_cube_in_default", ProjectInstance.DEFAULT_PROJECT_NAME, desc, null);
        assertTrue(createdCube == CubeManager.getInstance(this.getTestConfig()).getCube("new_cube_in_default"));

        System.out.println(JsonUtil.writeValueAsIndentString(createdCube));

        assertTrue(ProjectManager.getInstance(this.getTestConfig()).listAllProjects().size() == originalProjectCount);
        assertTrue(CubeManager.getInstance(this.getTestConfig()).listAllCubes().size() == originalCubeCount + 1);

        CubeInstance droppedCube = CubeManager.getInstance(this.getTestConfig()).dropCube("new_cube_in_default", true);

        assertTrue(createdCube == droppedCube);
        assertNull(CubeManager.getInstance(this.getTestConfig()).getCube("new_cube_in_default"));
        assertTrue(ProjectManager.getInstance(this.getTestConfig()).listAllProjects().size() == originalProjectCount);
        assertTrue(CubeManager.getInstance(this.getTestConfig()).listAllCubes().size() == originalCubeCount);
    }

    @Test
    public void testProjectsDrop() throws IOException {
        CubeRealizationManager cubeRealizationManager = CubeRealizationManager.getInstance(this.getTestConfig());
        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_with_slr_empty");
        assertTrue(cubeRealizationManager.getCubesByTable("default", "default.test_kylin_fact").contains(cube));
        assertTrue(cubeRealizationManager.listAllCubes("default").contains(cube));

        CubeManager.getInstance(getTestConfig()).dropCube(cube.getName(), true);

        assertTrue(!cubeRealizationManager.getCubesByTable("default", "default.test_kylin_fact").contains(cube));
        assertTrue(!cubeRealizationManager.listAllCubes("default").contains(cube));
    }

    @Test
    public void testProjectsLoadAfterProjectChange() throws IOException {
        CubeRealizationManager cubeRealizationManager = CubeRealizationManager.getInstance(this.getTestConfig());
        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_with_slr_empty");
        assertTrue(cubeRealizationManager.getCubesByTable("default", "default.test_kylin_fact").contains(cube));

        ProjectManager.getInstance(getTestConfig()).removeCubeFromProjects(cube.getName());
        CubeRealizationManager.getInstance(getTestConfig()).loadAllProjects();


        assertTrue(!cubeRealizationManager.getCubesByTable("default", "default.test_kylin_fact").contains(cube));

        ProjectManager.getInstance(getTestConfig()).updateCubeToProject(cube.getName(), "default", "tester");
        CubeRealizationManager.getInstance(getTestConfig()).loadProject(ProjectManager.getInstance(getTestConfig()).getProject("default"));

        assertTrue(cubeRealizationManager.getCubesByTable("default", "default.test_kylin_fact").contains(cube));
    }

    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(getTestConfig());
    }
    
    private CubeManager getCubeManager() {
        return CubeManager.getInstance(getTestConfig());
    }

    public CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(getTestConfig());
    }
}
