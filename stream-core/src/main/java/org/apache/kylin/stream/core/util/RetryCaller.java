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

package org.apache.kylin.stream.core.util;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryCaller {
    public static final int[] RETRY_BACKOFF = { 1, 2, 3, 5, 7 };
    private static final Logger logger = LoggerFactory.getLogger(RetryCaller.class);
    private int retries;
    private int pauseTimeInMs;

    public RetryCaller(int retries, int pauseTimeInMs) {
        this.retries = retries;
        this.pauseTimeInMs = pauseTimeInMs;
    }

    public <T> T call(RetryCallable<T> callable) throws IOException {
        int tries = 0;
        while (true) {
            try {
                if (tries > 0) {
                    callable.update();
                }
                T result = callable.call();
                if (callable.isResultExpected(result)) {
                    return result;
                } else {
                    throw new UnExpectResultException("unexpected result:" + result.toString());
                }
            } catch (Throwable t) {
                tries++;
                if (tries > retries) {
                    throw new IOException("Fail after " + retries + " retries", t);
                }
                logger.info("call fail because of {}, {} retry", t.getMessage(), tries);
                sleep(tries);
            }
        }
    }

    private void sleep(int retryCnt) {
        int sleepTime = getSleepTime(retryCnt);
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
            logger.error("interrupted", ie);
        }
    }

    private int getSleepTime(int retryCnt) {
        int tries = retryCnt;
        if (retryCnt >= RETRY_BACKOFF.length) {
            tries = RETRY_BACKOFF.length - 1;
        }
        return pauseTimeInMs * RETRY_BACKOFF[tries];
    }

    private static class UnExpectResultException extends RuntimeException {
        public UnExpectResultException(String msg) {
            super(msg);
        }
    }
}
