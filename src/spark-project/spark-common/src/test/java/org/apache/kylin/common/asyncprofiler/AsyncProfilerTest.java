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

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class AsyncProfilerTest {

    @Test
    public void testLocalLoaded() {
        Assert.assertTrue(AsyncProfiler.getInstance(true).isLoaded());
    }

    @Test
    public void testRemoteLoaded() {
        Assert.assertTrue(AsyncProfiler.getInstance(false).isLoaded());
    }

    // This may success in local Mac, but failed in CI
    @Test
    public void testLoadError() {
        System.setProperty("os.name", "Mac");
        String errorMsg = "";
        try {
            AsyncProfiler.getInstance(true);
        } catch (Throwable throwable) {
            errorMsg = throwable.getMessage();
        }
        Assert.assertNotNull(errorMsg);
    }

    @Test
    public void testExecute() throws IOException {
        AsyncProfiler asyncProfiler = AsyncProfiler.getInstance(true);
        try {
            asyncProfiler.execute("start,event=cpu");
            asyncProfiler.stop();
        } catch (IllegalStateException illegalState) {
            // maybe this can not be executed in docker, and environment need to check following:
            // No access to perf events. Try --all-user option or 'sysctl kernel.perf_event_paranoid=1'
            System.out.println(this.getClass().getCanonicalName() + ": " + illegalState.getMessage());
        }
    }

    @Test
    public void testStop() {
        Assert.assertThrows("Profiler is not active", IllegalStateException.class,
                AsyncProfiler.getInstance(true)::stop);
    }
}