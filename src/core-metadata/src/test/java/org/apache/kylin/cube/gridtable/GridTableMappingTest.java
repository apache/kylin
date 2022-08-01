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

package org.apache.kylin.cube.gridtable;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.gridtable.NLayoutToGridTableMapping;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class GridTableMappingTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private String projectDefault = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testHandlerCountReplace() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.query.replace-count-column-with-count-star", "false");
        NIndexPlanManager mgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        IndexPlan cube = mgr.getIndexPlanByModelAlias("nmodel_basic");
        IndexEntity first = Iterables.getFirst(cube.getAllIndexes(), null);
        LayoutEntity cuboidLayout = first.getLastLayout();
        NLayoutToGridTableMapping gridTableMapping = new NLayoutToGridTableMapping(cuboidLayout);
        FunctionDesc functionDesc = new FunctionDesc();
        ParameterDesc parameterDesc = new ParameterDesc();
        parameterDesc.setType("field");
        parameterDesc.setValue("CUSTOMER_ID");
        functionDesc.setParameters(Lists.newArrayList(parameterDesc));
        functionDesc.setExpression("COUNT");
        functionDesc.setReturnType("bigint");
        Integer measureIndex = gridTableMapping.handlerCountReplace(functionDesc);
        Assert.assertNull(measureIndex);
        FunctionDesc functionDesc1 = new FunctionDesc();
        functionDesc1.setParameters(Lists.newArrayList(parameterDesc));
        functionDesc1.setExpression("SUM");
        functionDesc1.setReturnType("bigint");
        measureIndex = gridTableMapping.handlerCountReplace(functionDesc1);
        Assert.assertNull(measureIndex);
        config.setProperty("kylin.query.replace-count-column-with-count-star", "true");
        measureIndex = gridTableMapping.handlerCountReplace(functionDesc);
        Assert.assertEquals("33", measureIndex.toString());
        measureIndex = gridTableMapping.handlerCountReplace(functionDesc1);
        Assert.assertNull(measureIndex);
    }

    @Test
    public void testGetIndexOf() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.query.replace-count-column-with-count-star", "false");
        NIndexPlanManager mgr = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        IndexPlan cube = mgr.getIndexPlanByModelAlias("nmodel_basic");
        IndexEntity first = Iterables.getFirst(cube.getAllIndexes(), null);
        LayoutEntity cuboidLayout = first.getLastLayout();
        NLayoutToGridTableMapping gridTableMapping = new NLayoutToGridTableMapping(cuboidLayout);
        FunctionDesc functionDesc = new FunctionDesc();
        ParameterDesc parameterDesc = new ParameterDesc();
        parameterDesc.setType("field");
        parameterDesc.setValue("CUSTOMER_ID");
        functionDesc.setParameters(Lists.newArrayList(parameterDesc));
        functionDesc.setExpression("COUNT");
        functionDesc.setReturnType("bigint");
        Integer measureIndex = gridTableMapping.getIndexOf(functionDesc);
        Assert.assertEquals("-1", measureIndex.toString());
        FunctionDesc functionDesc1 = new FunctionDesc();
        functionDesc1.setParameters(Lists.newArrayList(parameterDesc));
        functionDesc1.setExpression("SUM");
        functionDesc1.setReturnType("bigint");
        measureIndex = gridTableMapping.getIndexOf(functionDesc1);
        Assert.assertEquals("-1", measureIndex.toString());
        config.setProperty("kylin.query.replace-count-column-with-count-star", "true");
        measureIndex = gridTableMapping.handlerCountReplace(functionDesc);
        Assert.assertEquals("33", measureIndex.toString());
        measureIndex = gridTableMapping.getIndexOf(functionDesc1);
        Assert.assertEquals("-1", measureIndex.toString());
    }

}
