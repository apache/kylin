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

package org.apache.kylin.metadata.draft;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DraftManagerTest extends LocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws Exception {
        DraftManager mgr = DraftManager.getInstance(getTestConfig());
        Draft d = new Draft();
        d.setProject("default");
        d.updateRandomUuid();
        d.setEntity(getSampleModel());
        mgr.save(d);

        {
            List<Draft> list = mgr.list(null);
            assertEquals(1, list.size());
        }

        {
            List<Draft> list = mgr.list("default");
            assertEquals(1, list.size());
        }

        {
            List<Draft> list = mgr.list("default2");
            assertEquals(0, list.size());
        }
        
        Draft d2 = mgr.load(d.getUuid());
        assertEquals(d.getEntity().getUuid(), d2.getEntity().getUuid());
        assertEquals("default", d2.getProject());
    }

    private RootPersistentEntity getSampleModel() {
        DataModelManager metaMgr = DataModelManager.getInstance(getTestConfig());
        DataModelDesc model = metaMgr.getDataModelDesc("ci_left_join_model");
        return model;
    }
}
