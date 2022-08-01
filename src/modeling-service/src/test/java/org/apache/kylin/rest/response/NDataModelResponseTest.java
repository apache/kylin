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

package org.apache.kylin.rest.response;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_COUNT;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class NDataModelResponseTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void cleanup() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetSelectedColumnsAndSimplifiedDimensionsNormal() throws Exception {
        List<NDataModel.NamedColumn> allNamedColumns = Lists.newArrayList();
        NDataModel.NamedColumn namedColumn = new NDataModel.NamedColumn();
        namedColumn.setName("PRICE1");
        namedColumn.setAliasDotColumn("TEST_KYLIN_FACT.PRICE");
        namedColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
        allNamedColumns.add(namedColumn);

        NDataModel model = new NDataModel();
        model.setUuid("model");
        model.setProject(PROJECT);
        model.setAllNamedColumns(allNamedColumns);
        model.setAllMeasures(Lists.newArrayList(createMeasure()));
        model.setRootFactTableName("DEFAULT.TEST_KYLIN_FACT");

        createModel(model);

        NDataModelResponse modelResponse = new NDataModelResponse(model);
        modelResponse.setConfig(KylinConfig.getInstanceFromEnv());
        modelResponse.setProject(PROJECT);
        List<NDataModelResponse.SimplifiedNamedColumn> selectedColumns = modelResponse.getSelectedColumns();
        Assert.assertEquals(1, selectedColumns.size());
        List<NDataModelResponse.SimplifiedNamedColumn> namedColumns = modelResponse.getNamedColumns();
        Assert.assertEquals(1, namedColumns.size());
    }

    @Test
    public void testGetSelectedColumnsAndSimplifiedDimensionsWhenModelBroken() throws Exception {
        List<NDataModel.NamedColumn> allNamedColumns = Lists.newArrayList();
        NDataModel.NamedColumn namedColumn = new NDataModel.NamedColumn();
        namedColumn.setName("PRICE1");
        namedColumn.setAliasDotColumn("TEST_KYLIN_FACT.PRICE");
        namedColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
        allNamedColumns.add(namedColumn);

        NDataModel model = new NDataModel();
        model.setUuid("model");
        model.setProject(PROJECT);
        model.setAllNamedColumns(allNamedColumns);
        model.setAllMeasures(Lists.newArrayList(createMeasure()));
        model.setRootFactTableName("DEFAULT.TEST_KYLIN_FACT");

        createModel(model);

        NDataModelResponse modelResponse = new NDataModelResponse(model);
        modelResponse.setBroken(true);
        List<NDataModelResponse.SimplifiedNamedColumn> selectedColumns = modelResponse.getSelectedColumns();
        Assert.assertEquals(1, selectedColumns.size());
        List<NDataModelResponse.SimplifiedNamedColumn> namedColumns = modelResponse.getNamedColumns();
        Assert.assertEquals(1, namedColumns.size());
    }

    private NDataModel.Measure createMeasure() {
        NDataModel.Measure countOneMeasure = new NDataModel.Measure();
        countOneMeasure.setName("COUNT_ONE");
        countOneMeasure.setFunction(
                FunctionDesc.newInstance(FUNC_COUNT, newArrayList(ParameterDesc.newInstance("1")), "bigint"));
        countOneMeasure.setId(200001);
        return countOneMeasure;
    }

    private void createModel(NDataModel model) {
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        modelManager.createDataModelDesc(model, "root");
    }
}
