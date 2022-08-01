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
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.ReflectionUtils;

import lombok.val;

public class ImportModelContextTest extends NLocalFileMetadataTestCase {
    @Before
    public void setup() {
        this.createTestMetadata("src/test/resources/ut_meta/schema_utils/original_project");
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testPrepareIdChangedMap() throws IOException, NoSuchMethodException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_create/model_create_model_metadata_2020_11_14_17_11_19_B6A82E50A2B4A7EE5CD606F01045CA84.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);

        NDataModelManager originalDataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "original_project");

        NDataModel originalModel = originalDataModelManager.getDataModelDescByAlias("ssb_model");

        KylinConfig importConfig = getImportConfig(rawResourceMap);
        NDataModelManager targetDataModelManager = NDataModelManager.getInstance(importConfig, "model_create");

        NDataModel targetDataModel = targetDataModelManager.getDataModelDescByAlias("ssb_model_new");

        Method prepareIdChangedMapMethod = ImportModelContext.class.getDeclaredMethod("prepareIdChangedMap",
                NDataModel.class, NDataModel.class);

        Unsafe.changeAccessibleObject(prepareIdChangedMapMethod, true);
        Map<Integer, Integer> idChangedMap = (Map<Integer, Integer>) ReflectionUtils
                .invokeMethod(prepareIdChangedMapMethod, ImportModelContext.class, originalModel, targetDataModel);

        Assert.assertEquals(0, idChangedMap.size());
    }

    @Test
    public void testPrepareIdChangedMapWithTombColumnMeasure() throws IOException, NoSuchMethodException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_different_column_measure_id_update/original_project_model_metadata_2020_11_14_15_24_56_4B2101A84E908397A8E711864FC8ADF2.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);

        NDataModelManager originalDataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "original_project");

        NDataModel originalModel = originalDataModelManager.getDataModelDescByAlias("ssb_model");

        KylinConfig importConfig = getImportConfig(rawResourceMap);
        NDataModelManager targetDataModelManager = NDataModelManager.getInstance(importConfig, "original_project");

        NDataModel targetDataModel = targetDataModelManager.getDataModelDescByAlias("ssb_model");

        Method prepareIdChangedMapMethod = ImportModelContext.class.getDeclaredMethod("prepareIdChangedMap",
                NDataModel.class, NDataModel.class);

        Unsafe.changeAccessibleObject(prepareIdChangedMapMethod, true);
        Map<Integer, Integer> idChangedMap = (Map<Integer, Integer>) ReflectionUtils
                .invokeMethod(prepareIdChangedMapMethod, ImportModelContext.class, originalModel, targetDataModel);

        Assert.assertEquals(21, idChangedMap.size());
        Assert.assertEquals(21, new HashSet<>(idChangedMap.values()).size());
    }

    @Test
    public void testTableNameContainsProjectName() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/table_name_contains_project_name/LINEORDER_model_metadata_2020_11_14_17_11_19_25E6007633A4793DB1790C2E5D3B940A.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        Assert.assertEquals("LINEORDER", srcProject);
        val importModelContext = new ImportModelContext("original_project", srcProject, rawResourceMap);

        ResourceStore resourceStore = ResourceStore.getKylinMetaStore(importModelContext.getTargetKylinConfig());
        RawResource resource = resourceStore.getResource("/original_project/table/SSB.P_LINEORDER.json");
        Assert.assertNotNull(resource);
    }

    private KylinConfig getImportConfig(Map<String, RawResource> rawResourceMap) {
        KylinConfig importKylinConfig = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());
        ResourceStore importResourceStore = new InMemResourceStore(importKylinConfig);

        ResourceStore.setRS(importKylinConfig, importResourceStore);

        rawResourceMap.forEach((resPath, raw) -> {
            importResourceStore.putResourceWithoutCheck(resPath, raw.getByteSource(), raw.getTimestamp(), 0);
        });

        return importKylinConfig;
    }
}
