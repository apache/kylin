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
        List<DataModelDesc> dataModelDescs = modelService.listAllModels("ci_inner_join_model", "default");
        Assert.assertTrue(dataModelDescs.size() == 1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MetadataManager.MODELDESC_SERIALIZER.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataModelDesc deserialize = MetadataManager.MODELDESC_SERIALIZER.deserialize(new DataInputStream(bais));

        deserialize.setOwner("somebody");
        DataModelDesc dataModelDesc = modelService.updateModelAndDesc(deserialize);
        Assert.assertTrue(dataModelDesc.getOwner().equals("somebody"));
    }

    @Test
    public void testSuccessModelUpdateOnComputedColumn()
            throws IOException, JobException, NoSuchFieldException, IllegalAccessException {

        List<DataModelDesc> dataModelDescs = modelService.listAllModels("ci_left_join_model", "default");
        Assert.assertTrue(dataModelDescs.size() == 1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MetadataManager.MODELDESC_SERIALIZER.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataModelDesc deserialize = MetadataManager.MODELDESC_SERIALIZER.deserialize(new DataInputStream(bais));

        Field field = ComputedColumnDesc.class.getDeclaredField("comment");
        field.setAccessible(true);
        field.set(deserialize.getComputedColumnDescs().get(0), "change on comment is okay");
        DataModelDesc dataModelDesc = modelService.updateModelAndDesc(deserialize);
    }

    @Test
    public void testFailureModelUpdateDueToComputedColumnConflict()
            throws IOException, JobException, NoSuchFieldException, IllegalAccessException {
        expectedEx.expect(IllegalStateException.class);
        expectedEx.expectMessage(
                "Computed column named DEFAULT.TEST_KYLIN_FACT.DEAL_AMOUNT is already defined in other models: [DataModelDesc [name=ci_left_join_model], DataModelDesc [name=ci_inner_join_model]]. Please change another name, or try to keep consistent definition");

        List<DataModelDesc> dataModelDescs = modelService.listAllModels("ci_left_join_model", "default");
        Assert.assertTrue(dataModelDescs.size() == 1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MetadataManager.MODELDESC_SERIALIZER.serialize(dataModelDescs.get(0), new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataModelDesc deserialize = MetadataManager.MODELDESC_SERIALIZER.deserialize(new DataInputStream(bais));

        Field field = ComputedColumnDesc.class.getDeclaredField("expression");
        field.setAccessible(true);
        field.set(deserialize.getComputedColumnDescs().get(0), "another expression");
        DataModelDesc dataModelDesc = modelService.updateModelAndDesc(deserialize);
    }
}
