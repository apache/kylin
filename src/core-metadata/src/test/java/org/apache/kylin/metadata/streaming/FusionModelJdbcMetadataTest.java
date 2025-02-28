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

package org.apache.kylin.metadata.streaming;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.FusionModel;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo
@JdbcMetadataInfo
public class FusionModelJdbcMetadataTest {

    private static String PROJECT = "streaming_test";
    private static FusionModelManager mgr;

    @Test
    public void testNewFusionModel() {
        FusionModel copy = new FusionModel();
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(
                () -> FusionModelManager.getInstance(getTestConfig(), PROJECT).createModel(copy), PROJECT);
        mgr = FusionModelManager.getInstance(getTestConfig(), PROJECT);
        Assertions.assertNotNull(mgr.getFusionModel(copy.getId()));
    }
}
