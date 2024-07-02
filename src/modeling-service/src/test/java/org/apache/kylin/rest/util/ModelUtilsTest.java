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
package org.apache.kylin.rest.util;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.profiler.AsyncProfiler" })
public class ModelUtilsTest extends NLocalFileMetadataTestCase {
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setUp() {
        SecurityContextHolder.getContext().setAuthentication(authentication);
        overwriteSystemProp("HADOOP_USER_NAME", "root");
    }

    @After
    public void teardown() {
    }

    @Test
    public void testComputeExpansionRate() {
        Assert.assertEquals("0", ModelUtils.computeExpansionRate(0, 1024));
        Assert.assertEquals("-1", ModelUtils.computeExpansionRate(1024, 0));

        String result = ModelUtils.computeExpansionRate(2048, 1024);
        Assert.assertEquals("200.00", result);
    }

    @Test
    public void testIsArgMatch() {
        Assert.assertTrue(ModelUtils.isArgMatch(null, true, "ADMIN"));
        Assert.assertTrue(ModelUtils.isArgMatch("ADMIN", true, "ADMIN"));
        Assert.assertTrue(ModelUtils.isArgMatch("ADMIN", false, "ADMIN"));
    }
}
