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
import java.util.List;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.invertedindex.IIDescManager;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by shaoshi on 2/5/15.
 */
public class IIInstanceTest extends LocalFileMetadataTestCase {
    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void clean() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetIIsByDesc() throws IOException {
        IIManager mgr = IIManager.getInstance(getTestConfig());

        List<IIInstance> iiInstances = mgr.getIIsByDesc("test_kylin_ii_desc");

        Assert.assertTrue(iiInstances.size() > 0);

        IIInstance instance = iiInstances.get(0);

        Dictionary dict = mgr.getDictionary(instance.getFirstSegment(), instance.getDescriptor().findColumnRef("DEFAULT.TEST_KYLIN_FACT", "LSTG_SITE_ID"));

        Assert.assertNotNull(dict);
    }

    @Test
    public void testCreateIIInstance() throws IOException {

        IIDesc iiDesc = IIDescManager.getInstance(getTestConfig()).getIIDesc("test_kylin_ii_desc");

        IIInstance ii = IIInstance.create("new_ii", "default", iiDesc);

        IIManager iiMgr = IIManager.getInstance(getTestConfig());

        List<IIInstance> allIIList = iiMgr.listAllIIs();

        iiMgr.createII(ii);

        Assert.assertNotNull(iiMgr.getII("new_ii"));
    }

}
