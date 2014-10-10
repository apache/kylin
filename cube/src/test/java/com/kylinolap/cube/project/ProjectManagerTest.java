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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.cube.CubeDesc;

/**
 * @author xduo
 * 
 */
public class ProjectManagerTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        MetadataManager.removeInstance(this.getTestConfig());
        CubeManager.removeInstance(this.getTestConfig());
        ProjectManager.removeInstance(this.getTestConfig());
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test(expected = IllegalStateException.class)
    public void testDropNonemptyProject1() throws IOException {
        ProjectManager.getInstance(this.getTestConfig()).dropProject("DEFAULT");
    }

    @Test(expected = IllegalStateException.class)
    public void testDropNonemptyProject2() throws IOException {
        ProjectManager.getInstance(this.getTestConfig()).dropProject("DEFAULT???");
    }

    @Test
    public void testNewProject() throws Exception {
        int originalProjectCount = ProjectManager.getInstance(this.getTestConfig()).listAllProjects().size();
        int originalCubeCount = CubeManager.getInstance(this.getTestConfig()).listAllCubes().size();
        int originalCubeCountInDefault = ProjectManager.getInstance(this.getTestConfig()).listAllCubes("default").size();
        ResourceStore store = getStore();

        // clean legacy in case last run failed
        store.deleteResource("/cube/cube_in_alien_project.json");

        MetadataManager metaMgr = getMetadataManager();
        CubeDesc desc = metaMgr.getCubeDesc("test_kylin_cube_with_slr_desc");
        CubeInstance createdCube = CubeManager.getInstance(this.getTestConfig()).createCube("cube_in_alien_project", "alien", desc, null);
        assertTrue(createdCube == CubeManager.getInstance(this.getTestConfig()).getCube("cube_in_alien_project"));
        assertTrue(ProjectManager.getInstance(getTestConfig()).listAllCubes("alien").contains(createdCube));

        System.out.println(JsonUtil.writeValueAsIndentString(createdCube));

        assertTrue(ProjectManager.getInstance(this.getTestConfig()).listAllProjects().size() == originalProjectCount + 1);
        assertTrue(ProjectManager.getInstance(this.getTestConfig()).listAllCubes("ALIEN").get(0).getName().equalsIgnoreCase("CUBE_IN_ALIEN_PROJECT"));
        assertTrue(CubeManager.getInstance(this.getTestConfig()).listAllCubes().size() == originalCubeCount + 1);

        ProjectManager.getInstance(this.getTestConfig()).updateCubeToProject("cube_in_alien_project", "default", null);
        assertTrue(ProjectManager.getInstance(this.getTestConfig()).listAllCubes("ALIEN").size() == 0);
        assertTrue(ProjectManager.getInstance(this.getTestConfig()).listAllCubes("default").size() == originalCubeCountInDefault + 1);
        assertTrue(ProjectManager.getInstance(getTestConfig()).listAllCubes("default").contains(createdCube));

        ProjectManager.getInstance(this.getTestConfig()).updateCubeToProject("cube_in_alien_project", "alien", null);
        assertTrue(ProjectManager.getInstance(this.getTestConfig()).listAllCubes("ALIEN").size() == 1);
        assertTrue(ProjectManager.getInstance(this.getTestConfig()).listAllCubes("default").size() == originalCubeCountInDefault);
        assertTrue(ProjectManager.getInstance(getTestConfig()).listAllCubes("alien").contains(createdCube));

        assertTrue(ProjectManager.getInstance(this.getTestConfig()).isCubeInProject("alien", createdCube));

        CubeInstance droppedCube = CubeManager.getInstance(this.getTestConfig()).dropCube("cube_in_alien_project", true);

        assertTrue(createdCube == droppedCube);
        assertNull(CubeManager.getInstance(this.getTestConfig()).getCube("cube_in_alien_project"));
        assertTrue(ProjectManager.getInstance(this.getTestConfig()).listAllProjects().size() == originalProjectCount + 1);
        assertTrue(CubeManager.getInstance(this.getTestConfig()).listAllCubes().size() == originalCubeCount);

        ProjectManager.getInstance(this.getTestConfig()).dropProject("alien");
        assertTrue(ProjectManager.getInstance(this.getTestConfig()).listAllProjects().size() == originalProjectCount);
    }

    @Test
    public void testExistingProject() throws Exception {
        int originalProjectCount = ProjectManager.getInstance(this.getTestConfig()).listAllProjects().size();
        int originalCubeCount = CubeManager.getInstance(this.getTestConfig()).listAllCubes().size();
        ResourceStore store = getStore();

        // clean legacy in case last run failed
        store.deleteResource("/cube/new_cube_in_default.json");

        MetadataManager metaMgr = getMetadataManager();
        CubeDesc desc = metaMgr.getCubeDesc("test_kylin_cube_with_slr_desc");
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
        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_with_slr_empty");
        assertTrue(ProjectManager.getInstance(this.getTestConfig()).getCubesByTable("default", "test_kylin_fact").contains(cube));
        assertTrue(ProjectManager.getInstance(this.getTestConfig()).listAllCubes("default").contains(cube));

        CubeManager.getInstance(getTestConfig()).dropCube(cube.getName(), true);

        assertTrue(!ProjectManager.getInstance(this.getTestConfig()).getCubesByTable("default", "test_kylin_fact").contains(cube));
        assertTrue(!ProjectManager.getInstance(this.getTestConfig()).listAllCubes("default").contains(cube));
    }

    @Test
    public void testProjectsLoadAfterProjectChange() throws IOException {
        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_with_slr_empty");
        assertTrue(ProjectManager.getInstance(this.getTestConfig()).getCubesByTable("default", "test_kylin_fact").contains(cube));

        ProjectManager.getInstance(getTestConfig()).removeCubeFromProjects(cube.getName());

        assertTrue(!ProjectManager.getInstance(this.getTestConfig()).getCubesByTable("default", "test_kylin_fact").contains(cube));

        ProjectManager.getInstance(getTestConfig()).updateCubeToProject(cube.getName(), "default", "tester");

        assertTrue(ProjectManager.getInstance(this.getTestConfig()).getCubesByTable("default", "test_kylin_fact").contains(cube));
    }

    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(getTestConfig());
    }
}
