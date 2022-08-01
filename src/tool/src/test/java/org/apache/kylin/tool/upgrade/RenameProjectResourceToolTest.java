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

package org.apache.kylin.tool.upgrade;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class RenameProjectResourceToolTest extends NLocalFileMetadataTestCase {

    private KylinConfig config;

    @Before
    public void setup() throws IOException {
        createTestMetadata("src/test/resources/ut_upgrade_tool");
        config = KylinConfig.getInstanceFromEnv();

        String defaultHdfsWorkingDirectory = config.getHdfsWorkingDirectory("default");

        HadoopUtil.getWorkingFileSystem().mkdirs(new Path(defaultHdfsWorkingDirectory));
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    @Test
    public void testRenameProject() {
        String data = "y\r\n";
        InputStream stdin = System.in;
        try {
            NProjectManager projectManager = NProjectManager.getInstance(config);

            // project
            ProjectInstance projectInstance = projectManager.getProject("default");
            Assert.assertNotNull(projectInstance);

            System.setIn(new ByteArrayInputStream(data.getBytes(Charset.defaultCharset())));
            val tool = new RenameProjectResourceTool();
            tool.execute(
                    new String[] { "-dir", config.getMetadataUrl().toString(), "-p", "default", "-collect", "false" });

            config.clearManagers();

            ResourceStore.clearCache(config);

            projectManager = NProjectManager.getInstance(config);

            val originProjectInstance = projectManager.getProject("default");
            Assert.assertNull(originProjectInstance);

            val destProjectInstance = projectManager.getProject("default1");
            Assert.assertNotNull(destProjectInstance);

            // dataflow
            val dataflowManager = NDataflowManager.getInstance(config, "default1");
            val dataflows = dataflowManager.listAllDataflows(true);
            Assert.assertEquals(8, dataflows.size());

            // index plan
            val indexPlanManager = NIndexPlanManager.getInstance(config, "default1");
            val indexPlans = indexPlanManager.listAllIndexPlans(true);
            Assert.assertEquals(8, indexPlans.size());

            //model desc
            val dataModelManager = NDataModelManager.getInstance(config, "default1");
            val dataModels = dataModelManager.listAllModels();
            Assert.assertEquals(8, dataModels.size());

            // rule
            val favoriteRuleManager = FavoriteRuleManager.getInstance(config, "default1");
            val favoriteRules = favoriteRuleManager.getAll();
            Assert.assertEquals(5, favoriteRules.size());

            // table
            val tableMetadataManager = NTableMetadataManager.getInstance(config, "default1");
            val tableDescs = tableMetadataManager.listAllTables();
            Assert.assertEquals(20, tableDescs.size());

        } finally {
            System.setIn(stdin);
        }
    }
}
