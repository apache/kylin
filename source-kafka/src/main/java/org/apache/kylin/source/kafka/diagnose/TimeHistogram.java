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

package org.apache.kylin.source.kafka.diagnose;

import java.util.concurrent.atomic.AtomicLong;

public class TimeHistogram {
    private long[] bucketsBoundary;
    private AtomicLong[] counters;
    private String id;

    private static Object printLock = new Object();

    /**
     * example: [10,20] will generate three  buckets: (-∞,10), [10,20),[20,+∞)
     * unit: second
     */
    public TimeHistogram(long[] bucketsBoundary, String id) {
        this.bucketsBoundary = bucketsBoundary;
        this.counters = new AtomicLong[this.bucketsBoundary.length + 1];
        for (int i = 0; i < counters.length; i++) {
            this.counters[i] = new AtomicLong();
        }
        this.id = id;
    }

    /**
     * @param second in seconds
     */
    public void process(long second) {
        for (int i = 0; i < bucketsBoundary.length; ++i) {
            if (second < bucketsBoundary[i]) {
                counters[i].incrementAndGet();
                return;
            }
        }

        counters[bucketsBoundary.length].incrementAndGet();
    }

    /**
     * @param millis in milli seconds
     */
    public void processMillis(long millis) {
        process(millis / 1000);
    }

    public void printStatus() {
        long[] countersSnapshot = new long[counters.length];
        for (int i = 0; i < countersSnapshot.length; i++) {
            countersSnapshot[i] = counters[i].get();
        }

        long sum = 0;
        for (long counter : countersSnapshot) {
            sum += counter;
        }

        synchronized (printLock) {
            System.out.println("============== status of TimeHistogram " + id + " =================");

            for (int i = 0; i < countersSnapshot.length; ++i) {
                System.out.println(String.format("bucket: %d , count: %d ,percentage: %.4f", i, countersSnapshot[i], 1.0 * countersSnapshot[i] / (sum == 0 ? 1 : sum)));
            }

        }
    }

}
