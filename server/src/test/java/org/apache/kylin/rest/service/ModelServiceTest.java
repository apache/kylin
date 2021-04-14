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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.metadata.draft.Draft;
import org.apache.kylin.metadata.draft.DraftManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

public class ModelServiceTest extends ServiceTestBase {

    @Autowired
    @Qualifier("modelMgmtService")
    ModelService modelService;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testSuccessModelUpdate() throws IOException, JobException {
        Serializer<DataModelDesc> serializer = modelService.getDataModelManager().getDataModelSerializer();

        List<DataModelDesc> dataModelDescs = modelService.listAllModels("ci_inner_join_model", "default", true);
        Assert.assertTrue(dataModelDescs.size() == 1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataModelDesc deserialize = serializer.deserialize(new DataInputStream(bais));

        deserialize.setOwner("somebody");
        DataModelDesc dataModelDesc = modelService.updateModelAndDesc("default", deserialize);
        Assert.assertTrue(dataModelDesc.getOwner().equals("somebody"));
    }

    @Test
    public void testVerifyFilterCondition() throws IOException {
        Serializer<DataModelDesc> serializer = modelService.getDataModelManager()
            .getDataModelSerializer();
        List<DataModelDesc> dataModelDescs = modelService
            .listAllModels("ci_inner_join_model", "default", true);
        Assert.assertTrue(dataModelDescs.size() == 1);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataModelDesc deserialize = serializer.deserialize(new DataInputStream(bais));
        deserialize.setOwner("somebody");
        deserialize.setFilterCondition("TRANS_ID = 1");
        modelService.validateModel("default", deserialize);
        try {
            deserialize.setFilterCondition("kylin_account.TRANS_IDD = 1");
            modelService.validateModel("default", deserialize);
            Assert.fail("should throw an exception");
        } catch (Exception e){
            Assert.assertTrue(e.getMessage().equals("java.lang.IllegalArgumentException: filter condition col: TRANS_IDD is not a column in the table "));
        }
    }

    @Test
    public void testRevisableModelInCaseOfDeleteMeasure() throws IOException {
        List<DataModelDesc> dataModelDescs = modelService.listAllModels("ci_left_join_model", "default", true);
        Assert.assertTrue(dataModelDescs.size() == 1);

        DataModelDesc revisableModel = dataModelDescs.get(0);

        String[] originM = revisableModel.getMetrics();
        String[] reviseM = cutItems(originM, 1);

        revisableModel.setMetrics(reviseM);

        expectedEx.expect(org.apache.kylin.rest.exception.BadRequestException.class);
        expectedEx.expectMessage(
                "Measure: TEST_KYLIN_FACT.ITEM_COUNT can't be removed, It is referred in Cubes: [ci_left_join_cube]");
        modelService.updateModelToResourceStore(revisableModel, "default");
    }

    @Test
    public void testRevisableModelInCaseOfDeleteDims() throws IOException {
        List<DataModelDesc> dataModelDescs = modelService.listAllModels("ci_left_join_model", "default", true);
        Assert.assertTrue(dataModelDescs.size() == 1);

        DataModelDesc revisableModel = dataModelDescs.get(0);

        List<ModelDimensionDesc> originDims = revisableModel.getDimensions();
        String[] reviseDims = cutItems(originDims.get(0).getColumns(), 1);
        originDims.get(0).setColumns(reviseDims);
        revisableModel.setDimensions(originDims);

        // actually the TEST_COUNT_DISTINCT_BITMAP was defined in dimension (not measure)
        expectedEx.expect(org.apache.kylin.rest.exception.BadRequestException.class);
        expectedEx.expectMessage(
                "Measure: TEST_KYLIN_FACT.TEST_COUNT_DISTINCT_BITMAP can't be removed, It is referred in Cubes: [ci_left_join_cube]");
        modelService.updateModelToResourceStore(revisableModel, "default");
    }

    @Test
    public void testRevisableModelInCaseOfMoveMeasureToDim() throws IOException {
        List<DataModelDesc> dataModelDescs = modelService.listAllModels("ci_left_join_model", "default", true);
        Assert.assertTrue(dataModelDescs.size() == 1);

        DataModelDesc revisableModel = dataModelDescs.get(0);

        String[] originM = revisableModel.getMetrics();
        String[] reviseM = cutItems(originM, 1);
        revisableModel.setMetrics(reviseM);

        String col = originM[originM.length - 1];
        col = col.substring(col.indexOf('.') + 1);

        List<ModelDimensionDesc> originDims = revisableModel.getDimensions();
        String[] reviseDims = addItems(originDims.get(0).getColumns(), col);
        originDims.get(0).setColumns(reviseDims);
        revisableModel.setDimensions(originDims);
        modelService.updateModelToResourceStore(revisableModel, "default");
        //It should pass without any exceptions.
    }

    @Test
    public void testModelOrder() throws IOException {
        List<DataModelDesc> dataModelDescs = modelService.listAllModels(null, "default", true);
        long oldLastModified = Long.MAX_VALUE;

        for (DataModelDesc dataModelDesc : dataModelDescs) {
            Assert.assertTrue(dataModelDesc.getLastModified() <= oldLastModified);
            oldLastModified = dataModelDesc.getLastModified();
        }
    }

    @Test
    public void testModelDraft() throws IOException {
        DraftManager mgr = DraftManager.getInstance(getTestConfig());
        // Create a draft of model
        Draft d = new Draft();
        d.setProject("default");
        d.updateRandomUuid();
        DataModelDesc modelDesc = modelService.getModel("ci_left_join_model", "default");
        d.setEntity(modelDesc);
        mgr.save(d);

        // Check list draft
        List<Draft> draftList = modelService.listModelDrafts("", "default");
        Assert.assertEquals(draftList.size(), 1);
    }


    private String[] cutItems(String[] origin, int count) {
        if (origin == null)
            return null;

        String[] ret = new String[origin.length - count];
        for (int i = 0; i < ret.length; i++)
            ret[i] = origin[i];
        return ret;
    }

    private String[] addItems(String[] origin, String... toAddItems) {
        if (origin == null)
            return null;
        String[] ret = new String[origin.length + toAddItems.length];
        System.arraycopy(origin, 0, ret, 0, origin.length);
        System.arraycopy(toAddItems, 0, ret, origin.length, toAddItems.length);
        return ret;
    }
}
