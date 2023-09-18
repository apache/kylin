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

package io.kyligence.kap.secondstorage;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.common.KylinConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Only used for concurrent integration test
 */
public class SecondStorageConcurrentTestUtil {
    // for load data to clickhouse
    public static final String WAIT_PAUSED = "WAIT_PAUSED";
    public static final String WAIT_BEFORE_COMMIT = "WAIT_BEFORE_COMMIT";
    public static final String WAIT_AFTER_COMMIT = "WAIT_AFTER_COMMIT";

    private static final Map<String, Integer> latchMap = new ConcurrentHashMap<>();
    private static final Map<String, Boolean> waitingMap = new ConcurrentHashMap<>();

    public static void wait(String point) {
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            try {
                if (latchMap.containsKey(point)) {
                    waitingMap.put(point, true);
                    Thread.sleep(latchMap.get(point));
                    latchMap.remove(point);
                    waitingMap.remove(point);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void registerWaitPoint(String point, int ms) {
        Preconditions.checkState(!latchMap.containsKey(point));
        latchMap.put(point, ms);
    }

    public static boolean existWaitPoint(String point) {
        return latchMap.containsKey(point);
    }

    public static boolean isWaiting(String point) {
        return waitingMap.containsKey(point);
    }

    private SecondStorageConcurrentTestUtil() {
    }
}
