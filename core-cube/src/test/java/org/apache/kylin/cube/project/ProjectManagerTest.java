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

package org.apache.kylin.cube.project;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author xduo
 */
public class ProjectManagerTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test(expected = IllegalStateException.class)
    public void testDropNonemptyProject() throws IOException {
        ProjectManager.getInstance(getTestConfig()).dropProject("DEFAULT");
    }

    @Test(expected = IllegalStateException.class)
    public void testDropNonexistProject() throws IOException {
        ProjectManager.getInstance(getTestConfig()).dropProject("DEFAULT???");
    }

    @Test
    public void testNewProject() throws Exception {
        ProjectManager prjMgr = ProjectManager.getInstance(getTestConfig());
        CubeManager cubeMgr = CubeManager.getInstance(getTestConfig());
        CubeDescManager cubeDescMgr = CubeDescManager.getInstance(getTestConfig());

        int originalProjectCount = prjMgr.listAllProjects().size();
        int originalCubeCount = cubeMgr.listAllCubes().size();
        int originalCubeCountInDefault = prjMgr.listAllRealizations("default").size();
        ResourceStore store = getStore();

        // clean legacy in case last run failed
        store.deleteResource("/cube/cube_in_alien_project.json");

        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_with_slr_desc");
        CubeInstance createdCube = cubeMgr.createCube("cube_in_alien_project", "alien", desc, null);
        assertTrue(createdCube.equals(cubeMgr.getCube("cube_in_alien_project")));
        ProjectManager proMgr = ProjectManager.getInstance(getTestConfig());
        Set<IRealization> realizations = proMgr.listAllRealizations("alien");
        assertTrue(realizations.contains(createdCube));

        //System.out.println(JsonUtil.writeValueAsIndentString(createdCube));

        assertTrue(prjMgr.listAllProjects().size() == originalProjectCount + 1);
        assertTrue(prjMgr.listAllRealizations("ALIEN").iterator().next().getName().equalsIgnoreCase("CUBE_IN_ALIEN_PROJECT"));
        assertTrue(cubeMgr.listAllCubes().size() == originalCubeCount + 1);

        prjMgr.moveRealizationToProject(RealizationType.CUBE, "cube_in_alien_project", "default", null);
        assertTrue(prjMgr.listAllRealizations("ALIEN").size() == 0);
        assertTrue(prjMgr.listAllRealizations("default").size() == originalCubeCountInDefault + 1);
        assertTrue(ProjectManager.getInstance(getTestConfig()).listAllRealizations("default").contains(createdCube));

        prjMgr.moveRealizationToProject(RealizationType.CUBE, "cube_in_alien_project", "alien", null);
        assertTrue(prjMgr.listAllRealizations("ALIEN").size() == 1);
        assertTrue(prjMgr.listAllRealizations("default").size() == originalCubeCountInDefault);
        assertTrue(ProjectManager.getInstance(getTestConfig()).listAllRealizations("alien").contains(createdCube));

        CubeInstance droppedCube = cubeMgr.dropCube("cube_in_alien_project", true);

        assertTrue(createdCube.equals(droppedCube));
        assertNull(cubeMgr.getCube("cube_in_alien_project"));
        assertTrue(prjMgr.listAllProjects().size() == originalProjectCount + 1);
        assertTrue(cubeMgr.listAllCubes().size() == originalCubeCount);

        prjMgr.dropProject("alien");
        assertTrue(prjMgr.listAllProjects().size() == originalProjectCount);
    }

    @Test
    public void testExistingProject() throws Exception {
        ProjectManager prjMgr = ProjectManager.getInstance(getTestConfig());
        CubeManager cubeMgr = CubeManager.getInstance(getTestConfig());
        CubeDescManager cubeDescMgr = CubeDescManager.getInstance(getTestConfig());

        int originalProjectCount = prjMgr.listAllProjects().size();
        int originalCubeCount = cubeMgr.listAllCubes().size();
        ResourceStore store = getStore();

        // clean legacy in case last run failed
        store.deleteResource("/cube/new_cube_in_default.json");

        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_with_slr_desc");
        CubeInstance createdCube = cubeMgr.createCube("new_cube_in_default", ProjectInstance.DEFAULT_PROJECT_NAME, desc, null);
        assertTrue(createdCube.equals(cubeMgr.getCube("new_cube_in_default")));

        //System.out.println(JsonUtil.writeValueAsIndentString(createdCube));

        assertTrue(prjMgr.listAllProjects().size() == originalProjectCount);
        assertTrue(cubeMgr.listAllCubes().size() == originalCubeCount + 1);

        CubeInstance droppedCube = cubeMgr.dropCube("new_cube_in_default", true);

        assertTrue(createdCube.equals(droppedCube));
        assertNull(cubeMgr.getCube("new_cube_in_default"));
        assertTrue(prjMgr.listAllProjects().size() == originalProjectCount);
        assertTrue(cubeMgr.listAllCubes().size() == originalCubeCount);
    }

    @Test
    public void testProjectsDrop() throws IOException {
        ProjectManager prjMgr = ProjectManager.getInstance(getTestConfig());
        CubeManager cubeMgr = CubeManager.getInstance(getTestConfig());

        CubeInstance cube = cubeMgr.getCube("ci_left_join_cube");
        assertTrue(prjMgr.getRealizationsByTable("default", "default.test_kylin_fact").contains(cube));
        assertTrue(prjMgr.listAllRealizations("default").contains(cube));

        cubeMgr.dropCube(cube.getName(), false);

        assertTrue(!prjMgr.getRealizationsByTable("default", "default.test_kylin_fact").contains(cube));
        assertTrue(!prjMgr.listAllRealizations("default").contains(cube));
    }

    @Test
    public void testProjectsLoadAfterProjectChange() throws IOException {
        ProjectManager prjMgr = ProjectManager.getInstance(getTestConfig());
        CubeManager cubeMgr = CubeManager.getInstance(getTestConfig());

        CubeInstance cube = cubeMgr.getCube("ci_left_join_cube");
        assertTrue(prjMgr.getRealizationsByTable("default", "default.test_kylin_fact").contains(cube));

        prjMgr.removeRealizationsFromProjects(RealizationType.CUBE, cube.getName());

        assertTrue(!prjMgr.getRealizationsByTable("default", "default.test_kylin_fact").contains(cube));

        prjMgr.moveRealizationToProject(RealizationType.CUBE, cube.getName(), "default", "tester");

        assertTrue(prjMgr.getRealizationsByTable("default", "default.test_kylin_fact").contains(cube));
    }

}
