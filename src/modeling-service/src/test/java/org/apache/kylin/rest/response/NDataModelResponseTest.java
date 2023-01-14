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
import static org.apache.kylin.common.util.TestUtils.getTestConfig;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_COUNT;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.util.MetadataTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@MetadataInfo(project = "default")
class NDataModelResponseTest {

    private static final String PROJECT = "default";

    @Test
    void testGetSelectedColumnsAndSimplifiedDimensionsNormal() {
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
        Assertions.assertEquals(1, selectedColumns.size());
        List<NDataModelResponse.SimplifiedNamedColumn> namedColumns = modelResponse.getNamedColumns();
        Assertions.assertEquals(1, namedColumns.size());
    }

    @Test
    void testGetSelectedColumnAndSimplifiedDimensionsWithExcludedColumn() {
        MetadataTestUtils.mockExcludedCols(PROJECT, "DEFAULT.TEST_MEASURE1", Sets.newHashSet("PRICE6"));

        String modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);

        // prepare lookup dimension
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataModelManager modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
            modelMgr.updateDataModel(modelId, copyForWrite -> copyForWrite.getAllNamedColumns().get(33)
                    .setStatus(NDataModel.ColumnStatus.DIMENSION));
            return null;
        }, PROJECT);

        NDataModel model = modelManager.getDataModelDesc(modelId);
        NDataModel.NamedColumn namedColumn = model.getAllNamedColumns().get(33);
        Assertions.assertTrue(namedColumn.isDimension());
        model.init(getTestConfig(), PROJECT, Lists.newArrayList());

        NDataModelResponse modelResponse = new NDataModelResponse(model);
        modelResponse.setConfig(KylinConfig.getInstanceFromEnv());
        modelResponse.setProject(PROJECT);
        List<NDataModelResponse.SimplifiedNamedColumn> selectedColumns = modelResponse.getSelectedColumns();
        Assertions.assertEquals(2, selectedColumns.size());
        selectedColumns.forEach(simplifiedNamedColumn -> {
            if (simplifiedNamedColumn.getName().equalsIgnoreCase("price6")) {
                Assertions.assertTrue(simplifiedNamedColumn.isExcluded());
            } else {
                Assertions.assertFalse(simplifiedNamedColumn.isExcluded());
            }
        });
        List<NDataModelResponse.SimplifiedNamedColumn> namedColumns = modelResponse.getNamedColumns();
        Assertions.assertEquals(2, namedColumns.size());
        for (NDataModelResponse.SimplifiedNamedColumn column : namedColumns) {
            if (column.getAliasDotColumn().equals("TEST_MEASURE1.PRICE6")) {
                Assertions.assertTrue(column.isExcluded());
            } else {
                Assertions.assertFalse(column.isExcluded());
            }
        }
    }

    //transColumnToDim
    @Test
    void testGetSelectedColumnsWithExcluded() {
        MetadataTestUtils.mockExcludedTable(PROJECT, "DEFAULT.TEST_KYLIN_FACT");
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
        Assertions.assertEquals(1, selectedColumns.size());
        List<NDataModelResponse.SimplifiedNamedColumn> namedColumns = modelResponse.getNamedColumns();
        Assertions.assertEquals(1, namedColumns.size());
        Assertions.assertFalse(selectedColumns.get(0).isExcluded());
        Assertions.assertFalse(namedColumns.get(0).isExcluded());
    }

    @Test
    void testGetSelectedColumnsAndSimplifiedDimensionsWhenModelBroken() {
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
        Assertions.assertEquals(1, selectedColumns.size());
        List<NDataModelResponse.SimplifiedNamedColumn> namedColumns = modelResponse.getNamedColumns();
        Assertions.assertEquals(1, namedColumns.size());
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
