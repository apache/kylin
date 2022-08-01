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

package org.apache.kylin.metadata.model;

import static org.apache.kylin.metadata.datatype.DataType.BOOLEAN;
import static org.apache.kylin.metadata.datatype.DataType.INT;
import static org.apache.kylin.metadata.datatype.DataType.STRING;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.junit.Before;
import org.junit.Test;

public class NMutiPartitionDescTest {
    private static final String DEFAULT_PROJECT = "default";
    KylinConfig config;
    NDataModelManager mgr;

    @Before
    public void setUp() throws Exception {
        String tempMetadataDir = TempMetadataBuilder.prepareLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        config = KylinConfig.getInstanceFromEnv();
        mgr = NDataModelManager.getInstance(config, DEFAULT_PROJECT);
    }

    @Test
    public void testGenerateFormattedValue() {
        assert MultiPartitionDesc.generateFormattedValue(DataType.getType(INT), "1").equals("'1'");
        assert MultiPartitionDesc.generateFormattedValue(DataType.getType(STRING), "1").equals("'1'");
        assert MultiPartitionDesc.generateFormattedValue(DataType.getType(BOOLEAN), "1").equals("cast('1' as boolean)");

    }
}
