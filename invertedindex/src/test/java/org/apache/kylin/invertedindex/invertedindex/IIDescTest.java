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

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.invertedindex.IIDescManager;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


/**
 * Created by shaoshi on 1/30/15.
 */
public class IIDescTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() {
        this.createTestMetadata();

    }

    @After
    public void clear() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetIIDesc() {

        IIDesc iiDesc = IIDescManager.getInstance(getTestConfig()).getIIDesc("test_kylin_ii_left_join_desc");
        DataModelDesc model = iiDesc.getModel();
        Assert.assertNotNull(iiDesc);
        Assert.assertNotNull(model);

    }

    @Test
    public void testSerialization() throws IOException {
        IIDesc iiDesc = IIDescManager.getInstance(getTestConfig()).getIIDesc("test_kylin_ii_left_join_desc");
        String str = JsonUtil.writeValueAsIndentString(iiDesc);
        System.out.println(str);
        IIDesc desc2 = JsonUtil.readValue(str, IIDesc.class);

        Assert.assertEquals(iiDesc, desc2);
    }
}


