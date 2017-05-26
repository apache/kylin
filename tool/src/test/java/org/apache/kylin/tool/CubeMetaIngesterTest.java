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

package org.apache.kylin.tool;

import java.util.Collections;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.hamcrest.BaseMatcher;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

@Ignore
public class CubeMetaIngesterTest extends LocalFileMetadataTestCase {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testHappyIngest() {
        String srcPath = Thread.currentThread().getContextClassLoader().getResource("cloned_cube_and_model.zip").getPath();
        CubeMetaIngester.main(new String[] { "-project", "default", "-srcPath", srcPath });

        ProjectInstance project = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject("default");
        Assert.assertEquals(1, Collections.frequency(project.getTables(), "DEFAULT.TEST_KYLIN_FACT"));
        Assert.assertTrue(project.getModels().contains("cloned_model"));
        Assert.assertTrue(project.getRealizationEntries().contains(RealizationEntry.create(RealizationType.CUBE, "cloned_cube")));

        MetadataManager.clearCache();
        CubeDescManager.clearCache();
        CubeManager.clearCache();
        CubeInstance instance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube("cloned_cube");
        Assert.assertTrue(instance != null);
    }

    @Test
    public void testHappyIngest2() {
        String srcPath = Thread.currentThread().getContextClassLoader().getResource("benchmark_meta.zip").getPath();
        CubeMetaIngester.main(new String[] { "-project", "default", "-srcPath", srcPath, "-overwriteTables", "true" });

        ProjectInstance project = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject("default");
        Assert.assertEquals(1, Collections.frequency(project.getTables(), "SSB.CUSTOMER"));
        Assert.assertTrue(project.getModels().contains("benchmark_model"));
        Assert.assertTrue(project.getRealizationEntries().contains(RealizationEntry.create(RealizationType.CUBE, "benchmark_cube")));

        MetadataManager.clearCache();
        CubeDescManager.clearCache();
        CubeManager.clearCache();
        CubeInstance instance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube("benchmark_cube");
        Assert.assertTrue(instance != null);
    }

    @Test
    public void testBadIngest() {
        thrown.expect(RuntimeException.class);

        //should not break at table duplicate check, should fail at model duplicate check
        thrown.expectCause(new BaseMatcher<Throwable>() {
            @Override
            public boolean matches(Object item) {
                if (item instanceof IllegalStateException) {
                    if (((IllegalStateException) item).getMessage().equals("Already exist a model called test_kylin_inner_join_model_desc")) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {
            }
        });

        String srcPath = this.getClass().getResource("/cloned_cube_meta.zip").getPath();
        CubeMetaIngester.main(new String[] { "-project", "default", "-srcPath", srcPath });
    }

    @Test
    public void testProjectNotExist() {

        thrown.expect(RuntimeException.class);
        thrown.expectCause(CoreMatchers.<IllegalStateException> instanceOf(IllegalStateException.class));

        String srcPath = this.getClass().getResource("/cloned_cube_meta.zip").getPath();
        CubeMetaIngester.main(new String[] { "-project", "Xdefault", "-srcPath", srcPath });
    }

}