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

package org.apache.kylin.invertedindex.invertedindex;

import java.io.IOException;
import java.util.UUID;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.invertedindex.IIDescManager;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by shaoshi on 1/30/15.
 */
public class IIDescManagerTest extends LocalFileMetadataTestCase {

    public static final String TEST_II_DESC_NAME = "test_kylin_ii_left_join_desc";

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void clean() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCRUD() throws IOException {
        IIDescManager mgr = IIDescManager.getInstance(getTestConfig());

        String newDescName = "Copy of " + TEST_II_DESC_NAME;

        try {
            IIDesc testRecord = mgr.getIIDesc(newDescName);
            if (testRecord != null)
                mgr.removeIIDesc(testRecord);
        } catch (IOException e) {
            // just ensure the old one is removed
        }

        Assert.assertNull(mgr.getIIDesc(newDescName));
        IIDesc desc = mgr.getIIDesc(TEST_II_DESC_NAME);

        desc.setName(newDescName);
        desc.setLastModified(0);

        mgr.createIIDesc(desc);


        desc = mgr.getIIDesc(newDescName);

        Assert.assertNotNull(desc);

        mgr.updateIIDesc(desc); // this will trigger cache wipe; please ignore the HTTP error in logs.

        mgr.removeIIDesc(desc);

        Assert.assertNull(mgr.getIIDesc(newDescName));


    }

    @Test
    public void testReload() throws IOException {
        IIDescManager mgr = IIDescManager.getInstance(getTestConfig());

        IIDesc desc = mgr.getIIDesc(TEST_II_DESC_NAME);

        // do some modification
        desc.setUuid(UUID.randomUUID().toString());

        IIDesc newDesc = mgr.getIIDesc(TEST_II_DESC_NAME);

        Assert.assertEquals(desc, newDesc);

        // reload the cache
        mgr.reloadIIDesc(TEST_II_DESC_NAME);

        newDesc = mgr.getIIDesc(TEST_II_DESC_NAME);

        Assert.assertNotEquals(desc, newDesc);

    }

}
