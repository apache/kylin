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
package org.apache.kylin.metadata.cube;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModelManager;

import lombok.val;

public class CubeTestUtils {

    public static void createTmpModel(KylinConfig config, IndexPlan indexPlan) {
        val modelMgr = NDataModelManager.getInstance(config, "default");
        val model = modelMgr.copyForWrite(modelMgr.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        model.setUuid(indexPlan.getUuid());
        model.setAlias("tmp" + indexPlan.getUuid());
        model.setMvcc(-1);
        modelMgr.createDataModelDesc(model, null);

    }

    public static void createTmpModelAndCube(KylinConfig config, IndexPlan indexPlan) {
        createTmpModel(config, indexPlan);
        val indePlanManager = NIndexPlanManager.getInstance(config, "default");
        if (indePlanManager.getIndexPlan(indexPlan.getUuid()) == null) {
            indexPlan.setMvcc(-1);
            indePlanManager.createIndexPlan(indexPlan);
        }
    }
}
