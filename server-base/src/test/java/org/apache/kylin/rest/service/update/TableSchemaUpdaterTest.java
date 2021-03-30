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
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;

public class TableSchemaUpdaterTest extends LocalFileMetadataTestCase {

    private final String mappingRootPath = "src/test/resources/update";
    private final String mappingFileName = "TableSchemaUpdateMapping.json";
    private Map<String, TableSchemaUpdateMapping> mappings;

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
        TableMetadataManager tableMetaManager = TableMetadataManager.getInstance(getTestConfig());
        ResourceStore store = tableMetaManager.getStore();

        Set<TableDesc> tables = Sets.newHashSet();
        for (String tableIdentity : mappings.keySet()) {
            tables.add(store.getResource(TableDesc.concatResourcePath(tableIdentity, null),
                    TableMetadataManager.TABLE_SERIALIZER));
        }

        for (TableDesc tableDesc : tables) {
            TableDesc updated = TableSchemaUpdater.dealWithMappingForTable(tableDesc, mappings);
            updated = reinit(updated, TableMetadataManager.TABLE_SERIALIZER);

            try (DataInputStream bis = new DataInputStream(new FileInputStream(
                    new File(mappingRootPath + TableDesc.concatResourcePath(updated.getIdentity(), null))))) {
                TableDesc expected = TableMetadataManager.TABLE_SERIALIZER.deserialize(bis);
                Assert.assertEquals(expected, updated);
            } catch (Exception e) {
                Assert.fail("Table identity is not updated correctly");
            }
        }
    }

    @Test
    public void testDealWithMappingForModel() throws IOException {
        DataModelManager dataModelManager = DataModelManager.getInstance(getTestConfig());
        DataModelDesc model = dataModelManager.getDataModelDesc("ci_inner_join_model");

        DataModelDesc updated = TableSchemaUpdater.dealWithMappingForModel(model, mappings);
        updated = reinit(updated, dataModelManager.getDataModelSerializer());

        try (DataInputStream bis = new DataInputStream(
                new FileInputStream(new File(mappingRootPath + DataModelDesc.concatResourcePath(updated.getName()))))) {
            DataModelDesc expected = dataModelManager.getDataModelSerializer().deserialize(bis);
            Assert.assertTrue(expected.equalsRaw(updated));
        } catch (Exception e) {
            Assert.fail("Model is not updated correctly");
        }
    }

    @Test
    public void testDealWithMappingForCubeDesc() throws IOException {
        CubeDescManager cubeDescManager = CubeDescManager.getInstance(getTestConfig());
        CubeDesc cubeDesc = cubeDescManager.getCubeDesc("ci_left_join_cube");

        CubeDesc updated = TableSchemaUpdater.dealWithMappingForCubeDesc(cubeDesc, mappings);
        updated = reinit(updated, cubeDescManager.CUBE_DESC_SERIALIZER);

        try (DataInputStream bis = new DataInputStream(
                new FileInputStream(new File(mappingRootPath + CubeDesc.concatResourcePath(updated.getName()))))) {
            CubeDesc expected = cubeDescManager.CUBE_DESC_SERIALIZER.deserialize(bis);
            Assert.assertTrue(expected.equalsRaw(updated));
        } catch (Exception e) {
            Assert.fail("CubeDesc is not updated correctly");
        }
    }

    @Test
    public void testDealWithMappingForCube() throws IOException {
        CubeManager cubeManager = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = cubeManager.getCube("test_kylin_cube_with_slr_left_join_ready");

        CubeInstance updated = TableSchemaUpdater.dealWithMappingForCube(cube, mappings);
        updated = reinit(updated, cubeManager.CUBE_SERIALIZER);

        try (DataInputStream bis = new DataInputStream(
                new FileInputStream(new File(mappingRootPath + CubeInstance.concatResourcePath(updated.getName()))))) {
            CubeInstance expected = cubeManager.CUBE_SERIALIZER.deserialize(bis);
            Assert.assertTrue(expected.equalsRaw(updated));
        } catch (Exception e) {
            Assert.fail("CubeInstance is not updated correctly");
        }
    }

    private <T extends RootPersistentEntity> T reinit(T obj, Serializer<T> serializer) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        serializer.serialize(obj, dos);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        return serializer.deserialize(dis);
    }
}
