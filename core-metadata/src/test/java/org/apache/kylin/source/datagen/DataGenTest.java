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

package org.apache.kylin.source.datagen;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DataGenTest extends LocalFileMetadataTestCase {

    @BeforeEach
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @AfterEach
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    void testCIConfigured() throws IOException {
        DataModelDesc model = getModel("ci_inner_join_model");
        ModelDataGenerator gen = new ModelDataGenerator(model, 100);
        gen.outprint = false;
        
        gen.generate();
    }

    @Test
    void testSSBNoConfig() throws IOException {
        DataModelDesc model = getModel("ssb");
        ModelDataGenerator gen = new ModelDataGenerator(model, 100);
        gen.outprint = false;
        
        gen.generate();
    }

    private DataModelDesc getModel(String name) {
        DataModelManager mgr = DataModelManager.getInstance(KylinConfig.getInstanceFromEnv());
        DataModelDesc model = mgr.getDataModelDesc(name);
        return model;
    }

}
