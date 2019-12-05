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

package io.kyligence.kap.engine.spark.utils;

import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.builder.CreateFlatTable;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;

public class NUtilsTest extends NLocalWithSparkSessionTest {

    @Test
    public void testDotConversion() {
        String condition = "TEST_KYLIN_FACT.CAL_DT > 2017-09-12 AND TEST_KYLIN_FACT.PRICE < 9.9";
        String col = "TEST_KYLIN_FACT.CAL_DT";
        String withoutDot = NSparkCubingUtil.convertFromDot(col);
        Assert.assertEquals("TEST_KYLIN_FACT" + NSparkCubingUtil.SEPARATOR + "CAL_DT", withoutDot);
        String withDot = NSparkCubingUtil.convertToDot(withoutDot);
        Assert.assertEquals(col, withDot);

        NDataModel model = (NDataModel) NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        String replaced = CreateFlatTable.replaceDot(condition, model);
        Assert.assertEquals("TEST_KYLIN_FACT" + NSparkCubingUtil.SEPARATOR + "CAL_DT > 2017-09-12 AND TEST_KYLIN_FACT"
                + NSparkCubingUtil.SEPARATOR + "PRICE < 9.9", replaced);
    }
}
