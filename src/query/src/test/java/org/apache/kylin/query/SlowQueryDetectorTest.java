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

package org.apache.kylin.query;

import java.util.concurrent.TimeUnit;

import org.apache.kylin.query.util.SlowQueryDetector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SlowQueryDetectorTest {

    private SlowQueryDetector slowQueryDetector = new SlowQueryDetector(500, 10 * 1000);
    private int interruptNum = 0;

    @Before
    public void setUp() throws Exception {
        slowQueryDetector.start();
        interruptNum = 0;
    }

    @After
    public void after() throws Exception {
        slowQueryDetector.interrupt();
        interruptNum = 0;
    }

    @Test
    public void testCheckStopByUser() throws InterruptedException {

        Thread rt1 = new Thread() {

            private Thread t;
            private String threadName = "testCheckStopByUser";

            public void run() {
                slowQueryDetector.queryStart("123");
                int i = 0;
                while (i <= 5) {
                    try {
                        // will be interrupted and throw InterruptedException
                        TimeUnit.SECONDS.sleep(2);
                    } catch (InterruptedException e) {
                        interruptNum++;
                        // assert trigger InterruptedException
                        // Assert.assertNotNull(e);
                    } finally {
                        i++;
                    }
                }
                slowQueryDetector.queryEnd();
            }

            public void start() {
                if (t == null) {
                    t = new Thread(this, threadName);
                    t.start();
                }
            }

        };

        rt1.start();
        TimeUnit.SECONDS.sleep(1);

        for (SlowQueryDetector.QueryEntry e : SlowQueryDetector.getRunningQueries().values()) {
            e.setStopByUser(true);
            e.getThread().interrupt();
        }

        TimeUnit.SECONDS.sleep(2);
        // query is running
        Assert.assertEquals(1, SlowQueryDetector.getRunningQueries().values().size());
        // interrupt Number > 1
        Assert.assertTrue(interruptNum > 1);

        TimeUnit.SECONDS.sleep(2);
        // query is stop
        Assert.assertEquals(0, SlowQueryDetector.getRunningQueries().values().size());

    }

}
