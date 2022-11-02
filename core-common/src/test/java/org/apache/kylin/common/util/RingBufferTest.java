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

package org.apache.kylin.common.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * @author zhaoliu4
 * @date 2022/11/9
 */
public class RingBufferTest {
    @Test
    public void test() throws IOException {
        String log1 = "2022-11-09 18:25:48,678 INFO  [task-result-getter-0] scheduler.TaskSetManager:54 : Starting task 0.0 in stage 384.0 (TID 621, kylin-hadoop-001, executor 7, partition 0, NODE_LOCAL, 7780 bytes)\n";
        String log2 = "2022-11-09 18:25:48,678 INFO  [dispatcher-event-loop-26] spark.MapOutputTrackerMasterEndpoint:54 : Asked to send map output locations for shuffle 138 to x.x.x.x:46334\n";
        String log3 = "2022-11-09 18:25:48,689 INFO  [task-result-getter-0] scheduler.TaskSetManager:54 : Finished task 0.0 in stage 384.0 (TID 621) in 31 ms on kylin-hadoop-001 (executor 7) (1/1)\n";
        String log4 = "2022-11-09 18:25:48,689 INFO  [task-result-getter-0] cluster.YarnScheduler:54 : Removed TaskSet 384.0, whose tasks have all completed, from pool vip_tasks\n";

        // mock yarn application log input stream
        byte[] logBytes = (log1 + log2 + log3 + log4).getBytes(StandardCharsets.UTF_8);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(logBytes)))) {
            String line = null;
            // allocate 300B mem
            RingBuffer ringBuffer = RingBuffer.allocate(300);
            while ((line = reader.readLine()) != null) {
                ringBuffer.put((line + '\n').getBytes(StandardCharsets.UTF_8));
            }

            // assert actual log data > 600B, but only keep the last 300B
            Assert.assertTrue(logBytes.length > 600 && ringBuffer.get().length == 300);

            Assert.assertEquals(" [task-result-getter-0] scheduler.TaskSetManager:54 : Finished task 0.0 in stage 384.0 (TID 621) in 31 ms on kylin-hadoop-001 (executor 7) (1/1)\n" +
                            "2022-11-09 18:25:48,689 INFO  [task-result-getter-0] cluster.YarnScheduler:54 : Removed TaskSet 384.0, whose tasks have all completed, from pool vip_tasks\n",
                    new String(ringBuffer.get(), StandardCharsets.UTF_8));
        }
    }
}
