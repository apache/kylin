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

package org.apache.kylin.rest.service;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.ModelImportRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import lombok.val;

public class MetaStoreServiceWithRecTest extends ServiceTestBase {
    @Autowired
    private MetaStoreService metaStoreService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    JdbcTemplate jdbcTemplate = null;
    JdbcRawRecStore jdbcRawRecStore = null;
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setUp() {
        createTestMetadata("src/test/resources/ut_meta/metastore_model");
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        try {
            SecurityContextHolder.getContext().setAuthentication(authentication);
            jdbcTemplate = JdbcUtil.getJdbcTemplate(getTestConfig());
        } catch (Exception e) {
            e.printStackTrace();
        }
        jdbcTemplate.batchUpdate("SHUTDOWN;");
        try {
            jdbcRawRecStore = new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        super.tearDown();
    }

    @Test
    public void testImportModelMetadataWithRec() throws Exception {
        UnitOfWork.doInTransactionWithRetry(() -> {
            File file = new File(
                    "src/test/resources/ut_model_metadata/target_project_model_metadata_2020_11_21_16_40_43_5C7C7540C405F24159A5F6A3DBD164E4.zip");
            val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
            ModelImportRequest request = new ModelImportRequest();
            List<ModelImportRequest.ModelImport> models = new ArrayList<>();
            models.add(new ModelImportRequest.ModelImport("model_index", "model_index",
                    ModelImportRequest.ImportType.OVERWRITE));

            request.setModels(models);
            NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), "original_project");
            NDataModel dataModel = dataModelManager.getDataModelDescByAlias("model_index");
            List<RawRecItem> rawRecItems = jdbcRawRecStore.listAll("original_project", dataModel.getUuid(), 1, 10);
            Assert.assertEquals(0, rawRecItems.size());

            NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
            ProjectInstance projectInstance = projectManager.getProject("original_project");

            Assert.assertTrue(projectInstance.isExpertMode());

            projectManager.updateProject("original_project", copyForWrite -> {
                copyForWrite.putOverrideKylinProps("kylin.metadata.semi-automatic-mode", String.valueOf(true));
            });

            getTestConfig().clearManagers();
            projectManager = NProjectManager.getInstance(getTestConfig());
            projectInstance = projectManager.getProject("original_project");

            Assert.assertFalse(projectInstance.isExpertMode());
            metaStoreService.importModelMetadata("original_project", multipartFile, request);
            dataModel = dataModelManager.getDataModelDescByAlias("model_index");

            rawRecItems = jdbcRawRecStore.listAll("original_project", dataModel.getUuid(),
                    dataModel.getSemanticVersion(), 10);
            Assert.assertEquals(3, rawRecItems.size());

            val nDataModelManager = NDataModelManager.getInstance(getTestConfig(), "original_project");
            val nDataModel = nDataModelManager.getDataModelDescByAlias("model_index");
            Assert.assertEquals(2, nDataModel.getRecommendationsCount());
            return null;
        }, "original_project");

    }

}
