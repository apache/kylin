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

package org.apache.kylin.metadata.project;

import java.util.Arrays;
import java.util.LinkedHashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.hystrix.NCircuitBreaker;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import lombok.val;

public class NProjectManagerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testGetProjectsFromResource() throws Exception {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        KylinConfig config = getTestConfig();
        KapConfig kapConf = KapConfig.wrap(config);

        String path = kapConf.getReadHdfsWorkingDirectory() + "dict-store/test";
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        Path metadataPath = new Path(path);
        if (!fs.exists(metadataPath)) {
            fs.mkdirs(metadataPath);
        }

        val projects = projectManager.listAllProjects();
        Assert.assertEquals(27, projects.size());
        Assert.assertTrue(projects.stream().noneMatch(p -> p.getName().equals("test")));
    }

    @Test
    public void testCreateProjectWithBreaker() {

        NProjectManager manager = Mockito.spy(NProjectManager.getInstance(getTestConfig()));
        val projects = Arrays.asList("test_ck__1", "test_ck_2", "test_ck_3");
        Mockito.doReturn(projects).when(manager).listAllProjects();

        getTestConfig().setProperty("kylin.circuit-breaker.threshold.project", "1");
        NCircuitBreaker.start(KapConfig.wrap(getTestConfig()));
        try {
            thrown.expect(KylinException.class);
            manager.createProject("test_ck_project", "admin", "", null);
        } finally {
            NCircuitBreaker.stop();
        }
    }

    @Test
    public void testAddNonCustomProjectConfigs() {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        projectManager.updateProject("default", update_project -> {
            LinkedHashMap<String, String> overrideKylinProps = update_project.getOverrideKylinProps();
            overrideKylinProps.put("kylin.query.implicit-computed-column-convert", "false");
        });
        {
            val project = projectManager.getProject("default");
            Assert.assertEquals("false",
                    project.getConfig().getExtendedOverrides().get("kylin.query.implicit-computed-column-convert"));
        }
        {
            val projectInstance = new ProjectInstance();
            projectInstance.setName("override_setting");
            val project = projectManager.getProject("default");
            projectInstance
                    .setOverrideKylinProps((LinkedHashMap<String, String>) project.getConfig().getExtendedOverrides());
            Assert.assertEquals("false",
                    projectInstance.getOverrideKylinProps().get("kylin.query.implicit-computed-column-convert"));
        }
        {
            getTestConfig().setProperty("kylin.server.non-custom-project-configs",
                    "kylin.query.implicit-computed-column-convert");
            projectManager.reloadAll();
            val project = projectManager.getProject("default");
            Assert.assertEquals("false",
                    project.getOverrideKylinProps().get("kylin.query.implicit-computed-column-convert"));
            Assert.assertNull(project.getLegalOverrideKylinProps().get("kylin.query.implicit-computed-column-convert"));
            Assert.assertNull(
                    project.getConfig().getExtendedOverrides().get("kylin.query.implicit-computed-column-convert"));
        }
    }
}
