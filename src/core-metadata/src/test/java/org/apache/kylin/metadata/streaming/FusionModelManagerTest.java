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

package org.apache.kylin.metadata.streaming;

import org.apache.kylin.common.NativeQueryRealization;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.FusionModel;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;

public class FusionModelManagerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private FusionModelManager mgr;
    private static String PROJECT = "streaming_test";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        mgr = FusionModelManager.getInstance(getTestConfig(), PROJECT);
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetFusionModel() {
        val emptyId = "";
        Assert.assertNull(mgr.getFusionModel(emptyId));

        val id = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val fusionModel = mgr.getFusionModel(id);
        Assert.assertNotNull(fusionModel);

        val id_not_existed = "b05034a8-c037-416b-aa26-9e6b4a41ee41";
        val notExisted = mgr.getFusionModel(id_not_existed);
        Assert.assertNull(notExisted);
    }

    @Test
    public void testNewFusionModel() {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        val batchModel = modelManager.getDataModelDesc("334671fd-e383-4fc9-b5c2-94fce832f77a");
        val streamingModel = modelManager.getDataModelDesc("e78a89dd-847f-4574-8afa-8768b4228b74");
        FusionModel fusionModel = new FusionModel(streamingModel, batchModel);

        FusionModel copy = new FusionModel(fusionModel);

        mgr.createModel(copy);
        Assert.assertNotNull(mgr.getFusionModel("e78a89dd-847f-4574-8afa-8768b4228b74"));

        Assert.assertEquals(2, copy.getModelsId().size());

        Assert.assertEquals("e78a89dd-847f-4574-8afa-8768b4228b74", copy.resourceName());
        Assert.assertEquals("334671fd-e383-4fc9-b5c2-94fce832f77a", copy.getBatchModel().getUuid());
    }

    @Test
    public void testDropFusionModel() {
        val id = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        val fusionModel = mgr.dropModel(id);
        Assert.assertNotNull(fusionModel);

        Assert.assertNull(mgr.getFusionModel(id));

        val id_not_existed = "b05034a8-c037-416b-aa26-9e6b4a41ee41";
        val notExisted = mgr.dropModel(id_not_existed);
        Assert.assertNull(notExisted);
    }

    @Test
    public void testFusionModelCheck() {
        val modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        val batchModel = modelManager.getDataModelDesc("334671fd-e383-4fc9-b5c2-94fce832f77a");
        val streamingModel = modelManager.getDataModelDesc("b05034a8-c037-416b-aa26-9e6b4a41ee40");

        Assert.assertTrue(batchModel.fusionModelBatchPart());
        Assert.assertTrue(batchModel.isFusionModel());
        Assert.assertTrue(streamingModel.fusionModelStreamingPart());
    }

    @Test
    public void testGetModelId() {
        String streamingModelId = "14e00a6f-d910-14b6-ee67-e0a5775012c4";
        String batchModelId = "3d69e1c0-0165-c144-7dae-8ae5dc0cf16c";

        val realization = new NativeQueryRealization();
        realization.setModelId(streamingModelId);
        Assert.assertEquals(batchModelId, mgr.getModelId(realization));

        realization.setModelId(streamingModelId);
        realization.setStreamingLayout(true);
        Assert.assertEquals(streamingModelId, mgr.getModelId(realization));

        realization.setModelId(batchModelId);
        realization.setStreamingLayout(false);
        Assert.assertEquals(batchModelId, mgr.getModelId(realization));

        realization.setModelId(streamingModelId);
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        modelMgr.dropModel(batchModelId);
        Assert.assertEquals("", mgr.getModelId(realization));
    }

}
