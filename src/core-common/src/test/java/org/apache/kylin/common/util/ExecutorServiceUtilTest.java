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

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExecutorServiceUtilTest extends LogOutputTestCase {

    @Test
    @Ignore
    public void testShutdownGracefully() throws Exception {
        ExecutorService pool = Executors.newScheduledThreadPool(1);
        pool.execute(() -> {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                log.error("thread interrupted.");
            }
        });
        Thread.sleep(2000);
        ExecutorServiceUtil.shutdownGracefully(pool, 1);
        Assert.assertTrue(containsLog("thread interrupted."));
    }

    @Test
    public void testForceShutdown() {
        ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);
        Future task = pool.schedule(() -> log.info("thread execute"), 10, TimeUnit.SECONDS);
        ExecutorServiceUtil.forceShutdown(pool);
        try {
            //Avoid getting stuck
            task.get(60, TimeUnit.SECONDS);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof CancellationException);
        }
        Assert.assertFalse(containsLog("thread execute"));
    }

}
