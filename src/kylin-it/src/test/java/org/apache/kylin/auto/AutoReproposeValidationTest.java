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

package org.apache.kylin.auto;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.util.ExecAndComp.CompareLevel;
import org.apache.kylin.util.SuggestTestBase;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class AutoReproposeValidationTest extends SuggestTestBase {

    @Test
    public void testReproposeSQLWontChangeOriginMetadata() throws Exception {
        val folder = "sql_for_automodeling/repropose";
        try {
            // 1. create metadata
            proposeWithSmartMaster(getProject(), Lists.newArrayList(new TestScenario(CompareLevel.SAME, folder)));
            buildAllModels(KylinConfig.getInstanceFromEnv(), getProject());

            // 2. ensure metadata
            List<MeasureDesc> measureDescs = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .listEffectiveRewriteMeasures(getProject(), "EDW.TEST_SITES");
            measureDescs.stream().filter(measureDesc -> measureDesc.getFunction().isSum()).forEach(measureDesc -> {
                FunctionDesc func = measureDesc.getFunction();
                Assert.assertFalse(func.getColRefs().isEmpty());
            });
        } finally {
            FileUtils.deleteQuietly(new File("../kylin-it/metastore_db"));
        }
    }

}
