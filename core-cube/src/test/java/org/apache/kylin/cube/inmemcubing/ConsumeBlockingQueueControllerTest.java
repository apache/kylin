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
        Thread producer = new Thread(new Runnable() {
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
        });

        final AtomicInteger nRecordConsumed = new AtomicInteger(0);
        Thread consumer = new Thread(new Runnable() {
            @Override
            public void run() {
                int nSplit = 0;
                while (true) {
                    RecordConsumeBlockingQueueController blockingQueueController = RecordConsumeBlockingQueueController
                            .getQueueController(new InputConverterUnitTest(), input, nBatch);
                    while (blockingQueueController.hasNext()) {
                        blockingQueueController.next();
                        nRecordConsumed.incrementAndGet();
                    }
                    System.out.println(nRecordConsumed.get() + " records consumed when finished split " + nSplit);
                    nSplit++;

                    if (blockingQueueController.ifEnd()) {
                        break;
                    }
                }
            }
        });

        producer.start();
        consumer.start();

        try {
            producer.join();
            consumer.join();
        } catch (InterruptedException e) {
            logger.warn("Fail to join threads: " + e);
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
