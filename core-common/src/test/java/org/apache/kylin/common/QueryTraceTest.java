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

package org.apache.kylin.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QueryTraceTest {

    @Test
    void test() throws InterruptedException {
        QueryTrace trace = new QueryTrace();
        trace.startSpan("span 1");
        Thread.sleep(100);
        trace.startSpan("span 2");
        Thread.sleep(100);
        trace.endLastSpan();
        Assertions.assertEquals(2, trace.spans().size());
        Assertions.assertTrue(trace.getLastSpan().isPresent());
        Assertions.assertEquals("span 2", trace.getLastSpan().get().name);
        assertTimeEqual(100, trace.getLastSpan().get().duration);

        trace.amendLast("span 2", trace.getLastSpan().get().start + trace.getLastSpan().get().getDuration() + 1000);
        assertTimeEqual(1100, trace.getLastSpan().get().duration);
    }

    private void assertTimeEqual(long expected, long actual) {
        Assertions.assertTrue(Math.abs(expected - actual) < 1000);
    }

}
