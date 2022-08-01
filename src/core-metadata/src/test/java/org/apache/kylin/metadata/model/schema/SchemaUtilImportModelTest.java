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

public class SchemaUtilImportModelTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        this.createTestMetadata("src/test/resources/ut_meta/schema_utils/original_project_model_import");
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    public String getTargetModel() {
        return "ssb_mdl";
    }

    @Test
    public void testDifferentDatabaseSameModelStruct() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_import_diff_db/34110_2_model_metadata_2022_05_03_21_43_58_620D9E784006043A4B6E9E5E36C9B06D.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String targetProject = "original_project_model_import";
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(targetProject, srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(targetProject, KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());
        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(newItem -> newItem.getType().equals(SchemaNodeType.MODEL_FACT)
                        && newItem.getDetail().equals("SSB4X.P_LINEORDER"))
                && modelSchemaChange.getReduceItems().stream()
                        .anyMatch(reduceItem -> reduceItem.getType().equals(SchemaNodeType.MODEL_FACT)
                                && reduceItem.getDetail().equals("SSB.P_LINEORDER"))
                && modelSchemaChange.getMissingItems().stream()
                        .anyMatch(missingItem -> missingItem.getType().equals(SchemaNodeType.MODEL_TABLE)
                                && missingItem.getDetail().equals("SSB4X.P_LINEORDER")));
    }

    @Test
    public void testCaseSensitiveCCExpression() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_import_casesensitive_cc_expr/35930_2_model_metadata_2022_05_09_16_53_38_F219D2C04B1792E7DB87B634DE058AD8.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String targetProject = "original_project_model_import_2";
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(targetProject, srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(targetProject, KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());
        Assert.assertEquals(1, modelSchemaChange.getDifferences());
        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream()
                .anyMatch(updatedItem -> updatedItem.getType().equals(SchemaNodeType.MODEL_CC)
                        && updatedItem.getFirstAttributes().get("expression")
                                .equals("'ABC' || LINEORDER.LO_SHIPMODE || 'aBC' || LINEORDER.LO_ORDERPRIOTITY")
                        && updatedItem.getSecondAttributes().get("expression")
                                .equals("'aBc' || LINEORDER.LO_SHIPMODE || 'Abc' || LINEORDER.LO_ORDERPRIOTITY")));
    }

}
