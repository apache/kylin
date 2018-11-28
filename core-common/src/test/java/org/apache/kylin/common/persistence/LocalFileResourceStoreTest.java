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

package org.apache.kylin.common.persistence;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore.Checkpoint;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LocalFileResourceStoreTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testFileStore() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStoreTest.testAStore(config.getMetadataUrl().toString(), config);
    }

    @Test
    public void testRollback() throws Exception {
        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        byte[] bytes = new byte[] { 0, 1, 2 };
        RawResource raw;
        Checkpoint cp;

        cp = store.checkpoint();
        try {
            store.putResource("/res1", new StringEntity("data1"), 1000, StringEntity.serializer);
        } finally {
            cp.close();
        }
        StringEntity str = store.getResource("/res1", StringEntity.serializer);
        assertEquals("data1", str.toString());

        cp = store.checkpoint();
        try {
            ByteArrayInputStream is = new ByteArrayInputStream(bytes);
            store.putResource("/res2", is, 2000);
            is.close();
            
            store.putResource("/res1", str, 2000, StringEntity.serializer);
            store.deleteResource("/res1");

            assertEquals(null, store.getResource("/res1"));
            assertEquals(2000, (raw = store.getResource("/res2")).lastModified());
            raw.content().close();
            
            cp.rollback();
            
            assertEquals(null, store.getResource("/res2"));
            assertEquals(1000, (raw = store.getResource("/res1")).lastModified());
            raw.content().close();
        } finally {
            cp.close();
        }
    }

}
