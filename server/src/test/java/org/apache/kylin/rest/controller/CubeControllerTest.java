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
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.rest.request.CubeRequest;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.*;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.collect.Lists;

/**
 * @author xduo
 */
public class CubeControllerTest extends ServiceTestBase {
    private static final String SRC_CUBE_NAME = "test_kylin_cube_with_slr_ready";
    private static final String TEST_CUBE_NAME = SRC_CUBE_NAME + "_test_save";

    private CubeController cubeController;
    private CubeDescController cubeDescController;
    private ModelController modelController;
    private CubeDesc srcCubeDesc;

    @Autowired
    CubeService cubeService;
    @Autowired
    JobService jobService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        cubeController = new CubeController();
        cubeController.setCubeService(cubeService);
        cubeController.setJobService(jobService);

        cubeDescController = new CubeDescController();
        cubeDescController.setCubeService(cubeService);

        modelController = new ModelController();
        modelController.setCubeService(cubeService);

        srcCubeDesc = getCubeDescByName(SRC_CUBE_NAME);

        saveTestCube(TEST_CUBE_NAME);
    }

    @After
    public void tearDown() throws Exception {
        cubeController.deleteCube(TEST_CUBE_NAME);
        super.after();
    }

    private CubeDesc getCubeDescByName(String cubeDescName) {
        CubeDesc[] cubes = cubeDescController.getCube(cubeDescName);
        if (cubes == null || cubes.length < 1) {
            throw new IllegalStateException("cube desc " + cubeDescName + " not existed");
        }
        return cubes[0];
    }

    private void saveTestCube(final String newCubeName) throws IOException {
        CubeDesc newCube = new CubeDesc();

        try {
            cubeController.deleteCube(newCubeName);
        } catch (Exception e) {
            // it may not exist, ignore the exception
        }

        newCube.setName(newCubeName);
        newCube.setModelName(newCubeName);
        newCube.setModel(srcCubeDesc.getModel());
        newCube.setDimensions(srcCubeDesc.getDimensions());
        newCube.setHBaseMapping(srcCubeDesc.getHBaseMapping());
        newCube.setMeasures(srcCubeDesc.getMeasures());
        newCube.setConfig(srcCubeDesc.getConfig());
        newCube.setRowkey(srcCubeDesc.getRowkey());

        newCube.getModel().setName(newCubeName);
        newCube.getModel().setLastModified(0);

        CubeRequest cubeRequest = new CubeRequest();
        cubeRequest.setCubeDescData(JsonUtil.writeValueAsIndentString(newCube));
        cubeRequest.setModelDescData(JsonUtil.writeValueAsIndentString(newCube.getModel()));

        CubeRequest result = cubeController.saveCubeDesc(cubeRequest);
        Assert.assertTrue(result.getSuccessful());
    }

    @Test
    public void testBasics() throws IOException {

        Assert.assertNotNull(cubeController.getSql(SRC_CUBE_NAME, "20130331080000_20131212080000"));
        Assert.assertNotNull(cubeController.getCubes(null, null, 0, 5));

        DataModelDesc model = modelController.getModel(TEST_CUBE_NAME);
        Assert.assertNotNull(model);

        List<String> notifyList = Lists.newArrayList();
        notifyList.add("john@example.com");
        cubeController.updateNotifyList(TEST_CUBE_NAME, notifyList);
        cubeController.updateCubeCost(TEST_CUBE_NAME, 80);

        List<CubeInstance> cubeInstances = cubeController.getCubes(TEST_CUBE_NAME, "default", 1, 0);

        CubeInstance cubeInstance = cubeInstances.get(0);
        Assert.assertTrue(cubeInstance.getDescriptor().getNotifyList().contains("john@example.com"));
        Assert.assertTrue(cubeInstance.getCost() == 80);
    }

    @Test
    public void testUpdateCubeDesc() throws IOException {
        CubeDesc newCubeDesc = getCubeDescByName(TEST_CUBE_NAME);

        // -------------------------------------------------------
        // negative case
        // -------------------------------------------------------

        // invalid cube desc
        CubeRequest req = new CubeRequest();
        req.setCubeDescData("invalid");
        assertUpdateFail(req);

        // invalid data model
        req = new CubeRequest();
        req.setCubeDescData(JsonUtil.writeValueAsIndentString(newCubeDesc));
        req.setModelDescData("invalid");
        assertUpdateFail(req);

        // data model's model_name not consistent with model name
        req = new CubeRequest();
        req.setCubeDescData("{\"name\" : \"myCube\", \"model_name\" : \"anotherModelName\"}");
        req.setModelDescData("{\"name\" : \"myCube\"}");
        assertUpdateFail(req);

        // non-existed data model
        req = new CubeRequest();
        req.setCubeDescData("{\"name\" : \"noSuchCube\", \"model_name\" : \"noSuchModel\"}");
        req.setModelDescData("{\"name\" : \"noSuchModel\"}");
        assertUpdateFail(req);

        // modified data model
        req = new CubeRequest();
        req.setCubeDescData(JsonUtil.writeValueAsIndentString(newCubeDesc));

        DataModelDesc modifiedModel = new DataModelDesc();
        modifiedModel.setName(TEST_CUBE_NAME);
        modifiedModel.setFactTable("anotherFactTable");
        req.setModelDescData(JsonUtil.writeValueAsIndentString(modifiedModel));

        assertUpdateFail(req);

        // -------------------------------------------------------
        // positive case
        // -------------------------------------------------------
        req = new CubeRequest();
        req.setModelDescData(JsonUtil.writeValueAsIndentString(newCubeDesc.getModel()));

        // no signature change
        newCubeDesc.setDescription("hello cube");
        req.setCubeDescData(JsonUtil.writeValueAsIndentString(newCubeDesc));
        CubeRequest res = cubeController.updateCubeDesc(req);
        Assert.assertTrue(res.getSuccessful());

        CubeDesc resultDesc = getCubeDescByName(TEST_CUBE_NAME);
        Assert.assertEquals("hello cube", resultDesc.getDescription());
        Assert.assertEquals(newCubeDesc.getSignature(), resultDesc.getSignature());

        // signature change (reverse row key column order)
        newCubeDesc = getCubeDescByName(TEST_CUBE_NAME);
        RowKeyColDesc[] rowkeyColumns = newCubeDesc.getRowkey().getRowKeyColumns();
        for (int i = 0, j = rowkeyColumns.length - 1; i < j; i++, j--) {
            RowKeyColDesc tmp = rowkeyColumns[i];
            rowkeyColumns[i] = rowkeyColumns[j];
            rowkeyColumns[j] = tmp;
        }
        req = new CubeRequest();
        req.setCubeDescData(JsonUtil.writeValueAsIndentString(newCubeDesc));
        req.setModelDescData(JsonUtil.writeValueAsIndentString(newCubeDesc.getModel()));
        res = cubeController.updateCubeDesc(req);
        Assert.assertTrue(res.getSuccessful());

        resultDesc = getCubeDescByName(TEST_CUBE_NAME);
        Assert.assertNotEquals(newCubeDesc.getSignature(), resultDesc.getSignature());
        Assert.assertEquals(newCubeDesc.calculateSignature(), resultDesc.getSignature());
    }

    private void assertUpdateFail(CubeRequest req) throws JsonProcessingException {
        CubeRequest res = cubeController.updateCubeDesc(req);
        Assert.assertFalse(res.getSuccessful());
    }
}
