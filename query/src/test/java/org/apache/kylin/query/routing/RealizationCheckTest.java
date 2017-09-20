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
package org.apache.kylin.query.routing;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class RealizationCheckTest extends LocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testRealizationCheck() {
        RealizationCheck realizationCheck = new RealizationCheck();
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("ssb");
        DataModelDesc dataModelDesc = cubeDesc.getModel();
        IRealization iRealization = CubeInstance.create("ssb", cubeDesc);
        realizationCheck.addCubeIncapableReason(iRealization,
                RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.CUBE_NOT_CONTAIN_ALL_COLUMN));
        realizationCheck.addCubeIncapableReason(iRealization,
                RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.CUBE_NOT_CONTAIN_ALL_COLUMN));
        Assert.assertTrue(realizationCheck.getCubeIncapableReasons().size() == 1);

        realizationCheck.addModelIncapableReason(dataModelDesc,
                RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.CUBE_NOT_CONTAIN_ALL_COLUMN));
        realizationCheck.addModelIncapableReason(dataModelDesc,
                RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.CUBE_NOT_CONTAIN_ALL_MEASURE));
        realizationCheck.addModelIncapableReason(dataModelDesc,
                RealizationCheck.IncapableReason.notContainAllColumn(Lists.<TblColRef> newArrayList()));
        realizationCheck.addModelIncapableReason(dataModelDesc,
                RealizationCheck.IncapableReason.notContainAllColumn(Lists.<TblColRef> newArrayList()));
        Assert.assertTrue(realizationCheck.getModelIncapableReasons().size() == 1);
        Assert.assertTrue(realizationCheck.getModelIncapableReasons().get(dataModelDesc).size() == 3);

        realizationCheck.addModelIncapableReason(dataModelDesc, RealizationCheck.IncapableReason
                .notContainAllColumn(Lists.<TblColRef> newArrayList(dataModelDesc.findColumn("LO_DATE"))));
        Assert.assertTrue(realizationCheck.getModelIncapableReasons().size() == 1);
        Assert.assertTrue(realizationCheck.getModelIncapableReasons().get(dataModelDesc).size() == 4);
    }
}
