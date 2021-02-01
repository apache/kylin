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

package org.apache.kylin.query.monitor;

import org.apache.kylin.common.KylinConfig;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SparderContextCanary {
    private static final Logger logger = LoggerFactory.getLogger(SparderContextCanary.class);
    private static volatile boolean isStarted = false;

    private static final int THRESHOLD_TO_RESTART_SPARK = KylinConfig.getInstanceFromEnv().getThresholdToRestartSparder();
    private static final int PERIOD_MINUTES = KylinConfig.getInstanceFromEnv().getSparderCanaryPeriodMinutes();

    private static volatile int errorAccumulated = 0;
    private static volatile long lastResponseTime = -1;
    private static volatile boolean sparderRestarting = false;

    private SparderContextCanary() {
    }

    public static int getErrorAccumulated() {
        return errorAccumulated;
    }

    @SuppressWarnings("unused")
    public long getLastResponseTime() {
        return lastResponseTime;
    }

    @SuppressWarnings("unused")
    public boolean isSparderRestarting() {
        return sparderRestarting;
    }

    public static void init() {
        if (!isStarted) {
            synchronized (SparderContextCanary.class) {
                if (!isStarted) {
                    isStarted = true;
                    logger.info("Start monitoring Sparder");
                    Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(SparderContextCanary::monitor,
                            PERIOD_MINUTES, PERIOD_MINUTES, TimeUnit.MINUTES);
                }
            }
        }
    }

    public static boolean isError() {
        return errorAccumulated >= THRESHOLD_TO_RESTART_SPARK;
    }

    public static void monitor() {
        try {
            long startTime = System.currentTimeMillis();
            // check sparder context
            if (!SparderContext.isSparkAvailable()) {
                logger.info("Sparder is unavailable, need to restart immediately.");
                errorAccumulated = Math.max(errorAccumulated + 1, THRESHOLD_TO_RESTART_SPARK);
            } else {
                try {
                    JavaSparkContext jsc =
                            JavaSparkContext.fromSparkContext(SparderContext.getOriginalSparkSession().sparkContext());
                    jsc.setLocalProperty("spark.scheduler.pool", "vip_tasks");

                    long t = System.currentTimeMillis();
                    long ret = numberCount(jsc).get(KylinConfig.getInstanceFromEnv().getSparderCanaryErrorResponseMs(),
                            TimeUnit.MILLISECONDS);
                    logger.info("SparderContextCanary numberCount returned successfully with value {}, takes {} ms.", ret,
                            (System.currentTimeMillis() - t));
                    // reset errorAccumulated once good context is confirmed
                    errorAccumulated = 0;
                } catch (TimeoutException te) {
                    errorAccumulated++;
                    logger.error("SparderContextCanary numberCount timeout, didn't return in {} ms, error {} times.",
                            KylinConfig.getInstanceFromEnv().getSparderCanaryErrorResponseMs(), errorAccumulated);
                } catch (ExecutionException ee) {
                    logger.error("SparderContextCanary numberCount occurs exception, need to restart immediately.", ee);
                    errorAccumulated = Math.max(errorAccumulated + 1, THRESHOLD_TO_RESTART_SPARK);
                } catch (Exception e) {
                    errorAccumulated++;
                    logger.error("SparderContextCanary numberCount occurs exception.", e);
                }
            }

            lastResponseTime = System.currentTimeMillis() - startTime;
            logger.debug("Sparder context errorAccumulated:{}", errorAccumulated);

            if (isError()) {
                sparderRestarting = true;
                try {
                    // Take repair action if error accumulated exceeds threshold
                    logger.warn("Repairing sparder context");
                    SparderContext.restartSpark();
                } catch (Throwable th) {
                    logger.error("Restart sparder context failed.", th);
                }
                sparderRestarting = false;
            }
        } catch (Throwable th) {
            logger.error("Error when monitoring Sparder.", th);
        }
    }

    // for canary
    private static JavaFutureAction<Long> numberCount(JavaSparkContext jsc) {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }

        // use numSlices = 1 to reduce task num
        return jsc.parallelize(list, 1).countAsync();
    }
}
