/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.kylin.cube.inmemcubing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.common.util.MemoryBudgetController.NotEnoughBudgetException;
import org.junit.Test;

public class MemoryBudgetControllerTest {

    @Test
    public void test() {
        final int n = MemoryBudgetController.getSystemAvailMB() / 2;
        final MemoryBudgetController mbc = new MemoryBudgetController(n);

        ArrayList<Consumer> mbList = new ArrayList<Consumer>();
        for (int i = 0; i < n; i++) {
            mbList.add(new Consumer(mbc));
            assertEquals(mbList.size(), mbc.getTotalReservedMB());
        }

        // a's reservation will free up all the previous
        final Consumer a = new Consumer();
        mbc.reserve(a, n);
        for (int i = 0; i < n; i++) {
            assertEquals(null, mbList.get(i).data);
        }

        // cancel a in 2 seconds
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                mbc.reserve(a, 0);
            }
        }.start();

        // b will success after some wait
        long bWaitStart = System.currentTimeMillis();
        final Consumer b = new Consumer();
        mbc.reserveInsist(b, n);
        assertTrue(System.currentTimeMillis() - bWaitStart > 1000);

        try {
            mbc.reserve(a, 1);
            fail();
        } catch (NotEnoughBudgetException ex) {
            // expected
        }
    }

    class Consumer implements MemoryBudgetController.MemoryConsumer {

        byte[] data;

        Consumer() {
        }

        Consumer(MemoryBudgetController mbc) {
            mbc.reserve(this, 1);
            data = new byte[MemoryBudgetController.ONE_MB - 24]; // 24 is object shell of this + object shell of data + reference of data 
        }

        @Override
        public int freeUp(int mb) {
            if (data != null) {
                data = null;
                return 1;
            } else {
                return 0;
            }
        }

    }
}
