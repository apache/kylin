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

package org.apache.kylin.source.hive;

import static org.junit.Assert.assertTrue;

import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ITHiveSourceTableLoaderTest extends HBaseMetadataTestCase {

    @Before
    public void setup() throws Exception {
        super.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        super.cleanupTestMetadata();
    }

    @Test
    public void test() throws Exception {
        ISource source = SourceManager.getDefaultSource();
        ISourceMetadataExplorer explr = source.getSourceMetadataExplorer();
        Pair<TableDesc, TableExtDesc> pair;
        
        pair = explr.loadTableMetadata("DEFAULT", "TEST_KYLIN_FACT", "default");
        assertTrue(pair.getFirst().getIdentity().equals("DEFAULT.TEST_KYLIN_FACT"));
        
        pair = explr.loadTableMetadata("EDW", "TEST_CAL_DT", "default");
        assertTrue(pair.getFirst().getIdentity().equals("EDW.TEST_CAL_DT"));
        
    }

}
