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

import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.NDataModel;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class RealizationCheckTest {

    @Test
    public void testRealizationCheck() {
        RealizationCheck realizationCheck = new RealizationCheck();
        NDataModel dataModel = Mockito.mock(NDataModel.class);
        Mockito.when(dataModel.findColumn("LO_DATE")).thenReturn(Mockito.mock(TblColRef.class));

        realizationCheck.addModelIncapableReason(dataModel,
                RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.CUBE_NOT_CONTAIN_ALL_COLUMN));
        realizationCheck.addModelIncapableReason(dataModel,
                RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.CUBE_NOT_CONTAIN_ALL_MEASURE));
        realizationCheck.addModelIncapableReason(dataModel,
                RealizationCheck.IncapableReason.notContainAllColumn(Lists.<TblColRef> newArrayList()));
        realizationCheck.addModelIncapableReason(dataModel,
                RealizationCheck.IncapableReason.notContainAllColumn(Lists.<TblColRef> newArrayList()));
        Assert.assertTrue(realizationCheck.getModelIncapableReasons().size() == 1);
        Assert.assertTrue(realizationCheck.getModelIncapableReasons().get(dataModel).size() == 3);

        realizationCheck.addModelIncapableReason(dataModel, RealizationCheck.IncapableReason
                .notContainAllColumn(Lists.<TblColRef> newArrayList(dataModel.findColumn("LO_DATE"))));
        Assert.assertTrue(realizationCheck.getModelIncapableReasons().size() == 1);
        Assert.assertTrue(realizationCheck.getModelIncapableReasons().get(dataModel).size() == 4);
    }
}
