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

package org.apache.kylin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.realization.RealizationRegistry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class RealizationRegistryTest extends LocalFileMetadataTestCase {
    @Before
    public void setup() throws Exception {

        createTestMetadata();
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void test() throws Exception {
        final RealizationRegistry registry = RealizationRegistry.getInstance(KylinConfig.getInstanceFromEnv());
        final Set<RealizationType> realizationTypes = registry.getRealizationTypes();
        assertEquals(RealizationType.values().length, realizationTypes.size());
        for (RealizationType type : RealizationType.values()) {
            assertTrue(realizationTypes.contains(type));
        }
    }
}
