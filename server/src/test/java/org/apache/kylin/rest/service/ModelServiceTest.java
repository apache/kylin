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
import java.lang.reflect.Field;
import java.util.List;

import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
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
        List<DataModelDesc> dataModelDescs = modelService.listAllModels("ci_inner_join_model", "default", true);
        Assert.assertTrue(dataModelDescs.size() == 1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MetadataManager.MODELDESC_SERIALIZER.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataModelDesc deserialize = MetadataManager.MODELDESC_SERIALIZER.deserialize(new DataInputStream(bais));

        deserialize.setOwner("somebody");
        DataModelDesc dataModelDesc = modelService.updateModelAndDesc("default", deserialize);
        Assert.assertTrue(dataModelDesc.getOwner().equals("somebody"));
    }

    @Test
    public void testSuccessModelUpdateOnComputedColumn()
            throws IOException, JobException, NoSuchFieldException, IllegalAccessException {

        List<DataModelDesc> dataModelDescs = modelService.listAllModels("ci_left_join_model", "default", true);
        Assert.assertTrue(dataModelDescs.size() == 1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MetadataManager.MODELDESC_SERIALIZER.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataModelDesc deserialize = MetadataManager.MODELDESC_SERIALIZER.deserialize(new DataInputStream(bais));

        Field field = ComputedColumnDesc.class.getDeclaredField("comment");
        field.setAccessible(true);
        field.set(deserialize.getComputedColumnDescs().get(0), "change on comment is okay");
        DataModelDesc dataModelDesc = modelService.updateModelAndDesc("default", deserialize);
    }

    @Test
    public void testFailureModelUpdateDueToComputedColumnConflict()
            throws IOException, JobException, NoSuchFieldException, IllegalAccessException {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage(
                "Column name for computed column DEFAULT.TEST_KYLIN_FACT.DEAL_AMOUNT is already used in model ci_inner_join_model, you should apply the same expression ' PRICE * ITEM_COUNT ' here, or use a different column name.");

        List<DataModelDesc> dataModelDescs = modelService.listAllModels("ci_left_join_model", "default", true);
        Assert.assertTrue(dataModelDescs.size() == 1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MetadataManager.MODELDESC_SERIALIZER.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataModelDesc deserialize = MetadataManager.MODELDESC_SERIALIZER.deserialize(new DataInputStream(bais));

        Field field = ComputedColumnDesc.class.getDeclaredField("expression");
        field.setAccessible(true);
        field.set(deserialize.getComputedColumnDescs().get(0), "another expression");
        DataModelDesc dataModelDesc = modelService.updateModelAndDesc("default", deserialize);
    }

    @Test
    public void testFailureModelUpdateDueToComputedColumnConflict2()
            throws IOException, JobException, NoSuchFieldException, IllegalAccessException {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage(
                "There is already a column named cal_dt on table DEFAULT.TEST_KYLIN_FACT, please change your computed column name");

        List<DataModelDesc> dataModelDescs = modelService.listAllModels("ci_left_join_model", "default", true);
        Assert.assertTrue(dataModelDescs.size() == 1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MetadataManager.MODELDESC_SERIALIZER.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataModelDesc deserialize = MetadataManager.MODELDESC_SERIALIZER.deserialize(new DataInputStream(bais));

        Field field = ComputedColumnDesc.class.getDeclaredField("columnName");
        field.setAccessible(true);
        field.set(deserialize.getComputedColumnDescs().get(0), "cal_dt");
        DataModelDesc dataModelDesc = modelService.updateModelAndDesc("default", deserialize);
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
                "Measure: TEST_KYLIN_FACT.DEAL_AMOUNT can't be removed, It is referred in Cubes: [ci_left_join_cube]");
        modelService.updateModelToResourceStore(revisableModel, "default");
    }

    @Test
    public void testRevisableModelInCaseOfDeleteDims() throws IOException {
        List<DataModelDesc> dataModelDescs = modelService.listAllModels("ci_left_join_model", "default", true);
        Assert.assertTrue(dataModelDescs.size() == 1);

        DataModelDesc revisableModel = dataModelDescs.get(0);

        List<ModelDimensionDesc> originDims = revisableModel.getDimensions();
        String[] reviseDims = cutItems(originDims.get(0).getColumns(), 2);
        originDims.get(0).setColumns(reviseDims);
        revisableModel.setDimensions(originDims);

        expectedEx.expect(org.apache.kylin.rest.exception.BadRequestException.class);
        expectedEx.expectMessage(
                "Dimension: TEST_KYLIN_FACT.SELLER_ID_AND_COUNTRY_NAME can't be removed, It is referred in Cubes: [ci_left_join_cube]");
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
