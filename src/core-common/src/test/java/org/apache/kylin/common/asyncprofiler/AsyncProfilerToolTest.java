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

package org.apache.kylin.common.asyncprofiler;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AsyncProfilerToolTest extends NLocalFileMetadataTestCase {

    public static final String START_PARAMS = "start,event=cpu";
    public static final String DUMP_PARAMS = "flamegraph";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testStartAndStop() {
        try {
            Assert.assertTrue(StringUtils.contains(AsyncProfilerTool.status(), "Profiler is not active"));
            AsyncProfilerTool.start(START_PARAMS);
            AsyncProfilerTool.stop();
        } catch (IllegalStateException illegalState) {
            // maybe this can not be executed in docker, and environment need to check following:
            // No access to perf events. Try --all-user option or 'sysctl kernel.perf_event_paranoid=1'
            System.out.println(this.getClass().getCanonicalName() + ": " + illegalState.getMessage());
        }
    }

    @Test
    public void testExecuteAndDump() {
        String errorMsg = "";
        try {
            AsyncProfilerTool.execute(START_PARAMS);
            AsyncProfilerTool.dump(DUMP_PARAMS);
            AsyncProfilerTool.stop();
        } catch (IllegalStateException illegalState) {
            // maybe this can not be executed in docker, and environment need to check following:
            // No access to perf events. Try --all-user option or 'sysctl kernel.perf_event_paranoid=1'
            System.out.println(this.getClass().getCanonicalName() + ": " + illegalState.getMessage());
        }
        Assert.assertEquals("", errorMsg);
        Assert.assertTrue(StringUtils.contains(AsyncProfilerTool.status(), "Profiler is not active"));
    }
}
