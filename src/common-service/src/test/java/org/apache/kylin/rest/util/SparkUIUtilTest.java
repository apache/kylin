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

import static org.junit.Assert.assertEquals;

import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SparkUIUtilTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        createTestMetadata();
        overwriteSystemProp("kylin.job.yarn-app-rest-check-status-url", "");
        overwriteSystemProp("kylin.job.tracking-url-pattern", "");
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testValidateProxyUrl() {
        assertEquals("127.0.0.1", SparkUIUtil.validateProxyUrl("http://127.0.0.1:4040/jobs").getHost());

        Assert.assertThrows(KylinRuntimeException.class, () -> SparkUIUtil.validateProxyUrl("ftp://127.0.0.1"));
        Assert.assertThrows(KylinRuntimeException.class,
                () -> SparkUIUtil.validateProxyUrl("http://user:pass@127.0.0.1"));
        Assert.assertThrows(KylinRuntimeException.class, () -> SparkUIUtil.validateProxyUrl("http://127.0.0.1/#x"));
    }

    @Test
    public void testValidateJobTrackingUrl() {
        SparkUIUtil.validateJobTrackingUrl("http://127.0.0.1:4040", "127.0.0.1");

        Assert.assertThrows(KylinRuntimeException.class,
                () -> SparkUIUtil.validateJobTrackingUrl("http://10.0.0.1:4040", "127.0.0.1"));
        Assert.assertThrows(KylinRuntimeException.class,
                () -> SparkUIUtil.validateJobTrackingUrl("ftp://127.0.0.1", "127.0.0.1"));
    }
}

