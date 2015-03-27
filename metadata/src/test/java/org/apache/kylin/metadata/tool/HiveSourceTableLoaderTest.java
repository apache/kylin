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

package org.apache.kylin.metadata.tool;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.metadata.util.HiveSourceTableLoader;

public class HiveSourceTableLoaderTest extends HBaseMetadataTestCase {

    @BeforeClass
    public static void setup() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void after() throws Exception {
        staticCleanupTestMetadata();
    }

    @Test
    public void test() throws IOException {
        if (!useSandbox())
            return;

        KylinConfig config = getTestConfig();
        String[] toLoad = new String[] { "DEFAULT.TEST_KYLIN_FACT", "EDW.TEST_CAL_DT" };
        Set<String> loaded = HiveSourceTableLoader.reloadHiveTables(toLoad, config);

        assertTrue(loaded.size() == toLoad.length);
        for (String str : toLoad)
            assertTrue(loaded.contains(str));
    }

}
