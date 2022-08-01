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

package org.apache.kylin.tool;

import java.io.File;
import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.NDataModelManager;

import lombok.val;

public class MetadataToolTestFixture {

    public static void fixtureRestoreTest(KylinConfig kylinConfig, File junitFolder, String folder) throws IOException {
        // copy an metadata image to junit folder
        ResourceTool.copy(kylinConfig, KylinConfig.createInstanceFromUri(junitFolder.getAbsolutePath()), folder);
        fixtureRestoreTest();

    }

    public static void fixtureRestoreTest() {
        val dataModelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");

        // Make the current resource store state inconsistent with the image store
        val dataModel1 = dataModelMgr.getDataModelDescByAlias("nmodel_basic");
        dataModel1.setOwner("who");
        dataModelMgr.updateDataModelDesc(dataModel1);

        val dataModel2 = dataModelMgr.getDataModelDescByAlias("nmodel_basic_inner");
        dataModelMgr.dropModel(dataModel2);

        val dataModel3 = dataModelMgr.copyForWrite(dataModel2);
        dataModel3.setUuid(RandomUtil.randomUUIDStr());
        dataModel3.setAlias("data_model_3");
        dataModel3.setMvcc(-1L);
        dataModelMgr.createDataModelDesc(dataModel3, "who");
    }
}
