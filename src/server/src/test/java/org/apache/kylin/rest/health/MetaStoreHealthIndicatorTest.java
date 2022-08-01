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
package org.apache.kylin.rest.health;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.boot.actuate.health.Health;
import org.springframework.test.util.ReflectionTestUtils;

public class MetaStoreHealthIndicatorTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testHealth() {
        MetaStoreHealthIndicator indicator = Mockito.spy(new MetaStoreHealthIndicator());
        ReflectionTestUtils.setField(indicator, "isHealth", true);
        Assert.assertEquals(Health.up().build().getStatus(), indicator.health().getStatus());

        ReflectionTestUtils.setField(indicator, "isHealth", false);
        Assert.assertEquals(Health.down().build().getStatus(), indicator.health().getStatus());
    }

    @Test
    public void testHealthCheck() {
        MetaStoreHealthIndicator indicator = Mockito.spy(new MetaStoreHealthIndicator());
        Assert.assertFalse((boolean) ReflectionTestUtils.getField(indicator, "isHealth"));

        indicator.healthCheck();
        Assert.assertTrue((boolean) ReflectionTestUtils.getField(indicator, "isHealth"));

        Mockito.doThrow(RuntimeException.class).when(indicator).allNodeCheck();
        indicator.healthCheck();
        Assert.assertFalse((boolean) ReflectionTestUtils.getField(indicator, "isHealth"));

        Mockito.doReturn(null).when(indicator).allNodeCheck();
        indicator.healthCheck();
        Assert.assertFalse((boolean) ReflectionTestUtils.getField(indicator, "isHealth"));
    }

}
