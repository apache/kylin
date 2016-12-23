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

import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.JoinsTree.Chain;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class JoinsTreeTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() {
        MetadataManager mgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        DataModelDesc model = mgr.getDataModelDesc("ci_left_join_model");
        JoinsTree joinsTree = model.getJoinsTree();
        
        Chain chain = joinsTree.tableChains.get("BUYER_COUNTRY");
        assertTrue(chain.table == model.findTable("BUYER_COUNTRY"));
        assertTrue(chain.fkSide.table == model.findTable("BUYER_ACCOUNT"));
        assertTrue(chain.fkSide.fkSide.table == model.findTable("TEST_ORDER"));
        assertTrue(chain.fkSide.fkSide.fkSide.table == model.findTable("TEST_KYLIN_FACT"));
        assertTrue(chain.fkSide.fkSide.fkSide.join == null);
        assertTrue(chain.fkSide.fkSide.fkSide.fkSide == null);
    }
    
    @Test
    public void testMatch() {
        MetadataManager mgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        DataModelDesc model = mgr.getDataModelDesc("ci_inner_join_model");
        JoinsTree joinsTree = model.getJoinsTree();

        Map<String, String> matchUp = joinsTree.matches(joinsTree);
        for (Entry<String, String> e : matchUp.entrySet()) {
            assertTrue(e.getKey().equals(e.getValue()));
        }
        assertTrue(model.getAllTables().size() == matchUp.size());
    }
}
