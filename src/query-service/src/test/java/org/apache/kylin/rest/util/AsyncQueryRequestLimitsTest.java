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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AsyncQueryRequestLimitsTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @Test
    public void testException() throws Exception {
        getTestConfig().setProperty("kylin.query.async-query.max-concurrent-jobs", "3");
        reloadAsyncQueryRequestLimits();
        AsyncQueryRequestLimits limits1 = new AsyncQueryRequestLimits();
        AsyncQueryRequestLimits limits2 = new AsyncQueryRequestLimits();
        AsyncQueryRequestLimits limit3 = new AsyncQueryRequestLimits();
        Assert.assertThrows(KylinException.class, () -> AsyncQueryRequestLimits.checkCount());
        limit3.close();
        AsyncQueryRequestLimits.checkCount();
        limits1.close();
        limits2.close();

    }

    @Test
    public void testcase2() throws Exception {
        getTestConfig().setProperty("kylin.query.async-query.max-concurrent-jobs", "0");
        reloadAsyncQueryRequestLimits();
        AsyncQueryRequestLimits limits1 = new AsyncQueryRequestLimits();
        AsyncQueryRequestLimits limits2 = new AsyncQueryRequestLimits();
        AsyncQueryRequestLimits limits3 = new AsyncQueryRequestLimits();
        AsyncQueryRequestLimits.checkCount();
        limits1.close();
        limits2.close();
        limits3.close();

    }

    private void reloadAsyncQueryRequestLimits() throws Exception {
        Field field = AsyncQueryRequestLimits.class.getDeclaredField("MAX_COUNT");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        int count = KylinConfig.getInstanceFromEnv().getAsyncQueryMaxConcurrentJobs();
        field.setInt(null, count);
    }

}
