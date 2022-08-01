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

package org.apache.kylin.common.hystrix;

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_MODEL;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_PROJECT;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.QUERY_RESULT_OBTAIN_FAILED;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class NCircuitBreaker {

    private static final Logger logger = LoggerFactory.getLogger(NCircuitBreaker.class);

    private static final AtomicBoolean breakerStarted = new AtomicBoolean(false);

    private volatile static NBreakerConfig breakerConfig = null;

    private NCircuitBreaker() {
    }

    public static void start(KapConfig verifiableProps) {

        synchronized (breakerStarted) {
            if (!breakerStarted.get()) {
                try {
                    breakerConfig = new NBreakerConfig(verifiableProps);

                    breakerStarted.set(true);

                    logger.info("kap circuit-breaker started");
                } catch (Exception e) {
                    logger.error("kap circuit-breaker start failed", e);
                }
            }
        }
    }

    @VisibleForTesting
    public static void stop() {
        // Only used in test cases!!!
        breakerStarted.set(false);
        logger.info("kap circuit-breaker stopped");
    }

    public static void verifyProjectCreation(int current) {
        if (!isEnabled()) {
            return;
        }

        int threshold = breakerConfig.thresholdOfProject();
        if (threshold < 1 || current < threshold) {
            return;
        }

        throw new KylinException(FAILED_CREATE_PROJECT,
                String.format(Locale.ROOT, MsgPicker.getMsg().getProjectNumOverThreshold(), threshold));
    }

    public static void verifyModelCreation(int current) {
        if (!isEnabled()) {
            return;
        }

        int threshold = breakerConfig.thresholdOfModel();
        if (threshold < 1 || current < threshold) {
            return;
        }

        throw new KylinException(FAILED_CREATE_MODEL,
                String.format(Locale.ROOT, MsgPicker.getMsg().getModelNumOverThreshold(), threshold));
    }

    public static void verifyQueryResultRowCount(long current) {
        if (!isEnabled()) {
            return;
        }

        long threshold = breakerConfig.thresholdOfQueryResultRowCount();
        if (threshold < 1 || current <= threshold) {
            return;
        }

        throw new KylinException(QUERY_RESULT_OBTAIN_FAILED, threshold);
    }

    private static boolean isEnabled() {
        if (!breakerStarted.get()) {
            logger.warn("kap circuit-breaker not started");
            return false;
        }
        return breakerConfig.isBreakerEnabled();
    }
}
