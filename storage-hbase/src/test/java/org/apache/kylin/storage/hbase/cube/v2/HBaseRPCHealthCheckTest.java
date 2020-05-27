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

package org.apache.kylin.storage.hbase.cube.v2;

import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.opentracing.Span;

public class HBaseRPCHealthCheckTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testFormatEmail() {
        QueryContext queryContext = QueryContextFacade.startQuery("testProject", "select count(*) from testTable",
                "unittest");
        Span epRangeSpan = queryContext.startEPRangeQuerySpan("testRange", "testCube", "testSegment", "testHTable", 111,
                11111, "no");
        Span regionRPCSpan = queryContext.startRegionRPCSpan("testRegionServer", epRangeSpan);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        regionRPCSpan.finish();
        epRangeSpan.finish();
        Pair<String, String> result = HBaseRPCHealthCheck.formatNotifications(queryContext, epRangeSpan, regionRPCSpan,
                "slow region detected.", "WARN");
        Assert.assertEquals(result.getFirst(), "[QUERY ALERT]-[WARN]-[DEV]-[testProject]-[testCube]");
        System.out.println(result.getSecond());
    }
}
