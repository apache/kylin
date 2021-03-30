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

package org.apache.kylin.rest.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.rest.request.CubeMigrationRequest;
import org.apache.kylin.rest.service.CacheService;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author shaoshi
 */
public class CacheControllerTest extends ServiceTestBase {

    private CacheController cacheController;

    @Autowired
    @Qualifier("cacheService")
    private CacheService cacheService;

    @Before
    public void setup() throws Exception {
        super.setup();

        cacheController = new CacheController();
        cacheController.setCacheService(cacheService);
    }

    @Test
    public void testBasics() throws IOException {
        cacheController.wipeCache("cube_desc", "drop", "test_kylin_cube_with_slr_desc");
    }

    @Test
    public void testClearCacheForCubeMigration() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String CUBENAME = "test_kylin_cube_without_slr_desc";

        CubeDescManager cubeDescManager = CubeDescManager.getInstance(config);
        CubeDesc cubeDesc = cubeDescManager.getCubeDesc(CUBENAME);
        DataModelDesc modelDesc = cubeDesc.getModel();
        Map<String, String> tableToProjects = new HashMap<>();
        for (TableRef tableRef : modelDesc.getAllTables()) {
            tableToProjects.put(tableRef.getTableIdentity(), tableRef.getTableDesc().getProject());
        }

        String uuid = cubeDesc.getUuid();
        String signature = cubeDesc.getSignature();

        assertEquals(cubeDesc.getRetentionRange(), 0);

        //update cubeDesc
        cubeDesc.setRetentionRange(2018);
        cubeDesc.updateRandomUuid();

        //directly update metadata in store to simulate cube migration
        Serializer<CubeDesc> cubeDescSerializer = new JsonSerializer<CubeDesc>(CubeDesc.class);
        getStore().checkAndPutResource(cubeDesc.getResourcePath(), cubeDesc, cubeDescSerializer);

        CubeMigrationRequest request = new CubeMigrationRequest();
        request.setCube(cubeDesc.getName());
        request.setModel(modelDesc.getName());
        request.setProject(modelDesc.getProject());
        request.setTableToProjects(tableToProjects);

        cacheController.clearCacheForCubeMigration(request);

        assertEquals(2018, cubeDescManager.getCubeDesc(CUBENAME).getRetentionRange());
        assertEquals(signature, cubeDescManager.getCubeDesc(CUBENAME).getSignature());
        assertNotEquals(uuid, cubeDescManager.getCubeDesc(CUBENAME).getUuid());
    }

}
