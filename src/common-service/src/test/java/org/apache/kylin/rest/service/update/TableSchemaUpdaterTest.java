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

package org.apache.kylin.rest.service.update;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;

public class TableSchemaUpdaterTest extends NLocalFileMetadataTestCase {

    private final String mappingRootPath = "src/test/resources/update";
    private final String mappingFileName = "TableSchemaUpdateMapping.json";
    private Map<String, TableSchemaUpdateMapping> mappings;
    private static String PROJECT_NAME = "default";

    @Before
    public void setUp() throws IOException {
        this.createTestMetadata();

        File mappingFile = new File(mappingRootPath + "/" + mappingFileName);
        String content = new String(Files.readAllBytes(mappingFile.toPath()), StandardCharsets.UTF_8);
        mappings = JsonUtil.readValue(content, new TypeReference<Map<String, TableSchemaUpdateMapping>>() {
        });
    }

    @Test
    public void testDealWithMappingForTable() throws IOException {
        NTableMetadataManager tableMetaManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT_NAME);
        ResourceStore store = tableMetaManager.getStore();

        Set<TableDesc> tables = Sets.newHashSet();
        for (String tableIdentity : mappings.keySet()) {
            tables.add(store.getResource(TableDesc.concatResourcePath(tableIdentity, "default"),
                    NTableMetadataManager.getInstance(getTestConfig(), PROJECT_NAME).getTableMetadataSerializer()));
        }

        for (TableDesc tableDesc : tables) {
            TableDesc updated = TableSchemaUpdater.dealWithMappingForTable(tableDesc, mappings);
            updated = reInit(updated, NTableMetadataManager.getInstance(getTestConfig(), PROJECT_NAME).getTableMetadataSerializer());

            try (DataInputStream bis = new DataInputStream(new FileInputStream(
                    new File(mappingRootPath + TableDesc.concatResourcePath(updated.getIdentity(), PROJECT_NAME))))) {
                TableDesc expected = NTableMetadataManager.getInstance(getTestConfig(), PROJECT_NAME).getTableMetadataSerializer().deserialize(bis);
                Assert.assertEquals(expected, updated);
            } catch (Exception e) {
                Assert.fail("Table identity is not updated correctly");
            }
        }
    }

    @Test
    public void testDealWithMappingForModel() throws IOException {
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT_NAME);
        NDataModel model = dataModelManager.getDataModelDescByAlias("ut_inner_join_cube_partial");

        NDataModel updated = TableSchemaUpdater.dealWithMappingForModel(getTestConfig(), PROJECT_NAME, model, mappings);
        updated = reInit(updated, dataModelManager.getDataModelSerializer());

        try (DataInputStream bis = new DataInputStream(
                new FileInputStream(new File(mappingRootPath + NDataModel.concatResourcePath(updated.getUuid(), PROJECT_NAME))))) {
            NDataModel expected = dataModelManager.getDataModelSerializer().deserialize(bis);
            Assert.assertTrue(expected.equalsRaw(updated));
        } catch (Exception e) {
            Assert.fail("Model is not updated correctly");
        }
    }

    @Test
    public void testDealWithMappingForDataflow() throws IOException {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), PROJECT_NAME);
        NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NDataflow updated = TableSchemaUpdater.dealWithMappingForDataFlow(getTestConfig(), PROJECT_NAME, dataflow, mappings);
        updated = reInit(updated, dataflowManager.getDataflowSerializer());

        try (DataInputStream bis = new DataInputStream(
                new FileInputStream(new File(mappingRootPath + NDataflow.concatResourcePath(updated.getUuid(), PROJECT_NAME))))) {
            NDataflow expected = dataflowManager.getDataflowSerializer().deserialize(bis);
            Assert.assertTrue(expected.equalsRaw(updated));
        } catch (Exception e) {
            Assert.fail("Dataflow is not updated correctly");
        }
    }

    private <T extends RootPersistentEntity> T reInit(T obj, Serializer<T> serializer) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        serializer.serialize(obj, dos);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        return serializer.deserialize(dis);
    }
}
