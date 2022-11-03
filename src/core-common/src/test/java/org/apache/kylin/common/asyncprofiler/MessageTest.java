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

import org.junit.Assert;
import org.junit.Test;

import scala.Tuple3;

public class MessageTest {

    @Test
    public void testDriverMessage() {
        String driverMessage = Message.createDriverMessage(Message.START(), AsyncProfilerToolTest.START_PARAMS);
        Tuple3<String, String, String> tuple = Message.processMessage(driverMessage);
        Assert.assertEquals("STA-1:start,event=cpu", driverMessage);
        Assert.assertEquals(Message.START(), tuple._1());
        Assert.assertEquals("-1", tuple._2());
        Assert.assertEquals(AsyncProfilerToolTest.START_PARAMS, tuple._3());
    }

    @Test
    public void testExecutorMessage() {
        String executorMessage = Message.createExecutorMessage(Message.START(), "-0", AsyncProfilerToolTest.DUMP_PARAMS);
        Tuple3<String, String, String> tuple = Message.processMessage(executorMessage);
        Assert.assertEquals("STA-0:flamegraph", executorMessage);
        Assert.assertEquals(Message.START(), tuple._1());
        Assert.assertEquals("-0", tuple._2());
        Assert.assertEquals(AsyncProfilerToolTest.DUMP_PARAMS, tuple._3());
    }

}
