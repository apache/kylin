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

package org.apache.kylin.cube.inmemcubing;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.gridtable.GTRecord;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumeBlockingQueueControllerTest {
    private static final Logger logger = LoggerFactory.getLogger(ConsumeBlockingQueueControllerTest.class);

    @Test
    public void testIterator() {
        final int nRecord = 4345;
        final int nCut = 2000;
        final int nBatch = 60;

        final BlockingQueue<String> input = new LinkedBlockingQueue<>();
        final RecordConsumeBlockingQueueController inputController = RecordConsumeBlockingQueueController
                .getQueueController(new InputConverterUnitTest(), input, nBatch);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 1; i <= nRecord; i++) {
                        input.put("test");
                        if (i % nCut == 0) {
                            input.put(InputConverterUnitTest.CUT_ROW);
                        }
                    }
                    input.put(InputConverterUnitTest.END_ROW);
                } catch (InterruptedException e) {
                    logger.warn("Fail to produce records into BlockingQueue due to: " + e);
                }
            }
        }).start();

        final AtomicInteger nRecordConsumed = new AtomicInteger(0);
        final AtomicInteger nSplit = new AtomicInteger(0);
        while (true) {
            // producer done & consume the end row flag
            if (inputController.ifEnd()) {
                break;
            }
            nSplit.incrementAndGet();
            Thread consumer = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (inputController.hasNext()) {
                        inputController.next();
                        nRecordConsumed.incrementAndGet();
                    }
                    System.out.println(nRecordConsumed.get() + " records consumed when finished split " + nSplit.get());
                }
            });
            consumer.start();
            try {
                consumer.join();
            } catch (InterruptedException e) {
                logger.warn("Fail to join consumer thread: " + e);
            }
        }

        Assert.assertEquals(nRecord, nRecordConsumed.get());
    }

    private static class InputConverterUnitTest implements InputConverterUnit<String> {
        public static final String END_ROW = new String();
        public static final String CUT_ROW = "0";

        @Override
        public void convert(String currentObject, GTRecord record) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean ifEnd(String currentObject) {
            return currentObject == END_ROW;
        }

        @Override
        public boolean ifCut(String currentObject) {
            return currentObject == CUT_ROW;
        }

        @Override
        public String getEndRow() {
            return END_ROW;
        }

        @Override
        public String getCutRow() {
            return CUT_ROW;
        }

        @Override
        public boolean ifChange() {
            throw new UnsupportedOperationException();
        }
    }
}
