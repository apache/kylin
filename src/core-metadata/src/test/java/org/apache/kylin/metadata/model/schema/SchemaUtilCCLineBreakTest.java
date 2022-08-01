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

package org.apache.kylin.metadata.model.schema;

import static org.apache.kylin.metadata.model.schema.SchemaUtilTest.getModelMetadataProjectName;
import static org.apache.kylin.metadata.model.schema.SchemaUtilTest.getRawResourceFromUploadFile;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;

public class SchemaUtilCCLineBreakTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        this.createTestMetadata("src/test/resources/ut_meta/schema_utils/original_project_cc_linebreak");
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    public String getTargetProject() {
        return "original_project_cc_linebreak";
    }

    public String getTargetModel() {
        return "COR_KYL_MOD_PNL_RISK_RESULTS";
    }

    @Test
    public void testDifferentLineBreakInCC() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_cc_different_line_breaks/COR_KYL_MOD_PNL_RISK_RESULTS_b5c17c85a59e74f18a2b7f18c7575c16.zip");
        String expr = "(CASE WHEN PNL_RISK_RESULTS_VD.MEASURE = FX_FAMILY_ENRICHED.ATTRIBUTE_ID AND FX_FAMILY_ENRICHED.GCRS_PRODUCT_CODE = FX_FAMILY_ENRICHED.PRODUCTHIERARCHY_GCRS_CODEID \n"
                + "THEN 1 ELSE 0 END)";
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());
        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(newItem -> newItem.getType().equals(SchemaNodeType.MODEL_CC)
                        && newItem.getAttributes().get("expression").equals(expr)));
    }

}
