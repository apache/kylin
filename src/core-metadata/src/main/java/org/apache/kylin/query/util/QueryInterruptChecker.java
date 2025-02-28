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

package org.apache.kylin.query.util;

import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.query.exception.UserStopQueryException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryInterruptChecker {

    private QueryInterruptChecker() {
        // This is Utils.
    }

    /**
     * At most cases, please use {@link this#checkQueryCanceledOrThreadInterrupted(String, String)} instead.
     * The semantic of this method is confused in some scenarios.
     */
    public static void checkThreadInterrupted(String errorMsgLog, String stepInfo) {
        if (Thread.currentThread().isInterrupted()) {
            log.error("{} {}", QueryContext.current().getQueryId(), errorMsgLog);
            if (SlowQueryDetector.getRunningQueries().containsKey(Thread.currentThread())
                    && SlowQueryDetector.getRunningQueries().get(Thread.currentThread()).isStopByUser()) {
                throw new UserStopQueryException("");
            }
            if (QueryContext.current().getQueryTagInfo().isErrInterrupted()) {
                throw new KylinRuntimeException(QueryContext.current().getQueryTagInfo().getInterruptReason());
            }
            QueryContext.current().getQueryTagInfo().setTimeout(true);
            throw new KylinTimeoutException("The query exceeds the set time limit of "
                    + KylinConfig.getInstanceFromEnv().getQueryTimeoutSeconds() + "s. " + stepInfo);
        }
    }

    /**
     * Within the global context, STOP is same to CANCEL, therefore to stop a query is equal to
     * cancel a query.
     * There are some possible reasons to cancel the query recorded in the current thread context.
     * {@link UserStopQueryException} is for stopping the query from the request, see
     * {@link SlowQueryDetector#stopQuery(String)}.
     * {@link KylinTimeoutException} is for run out of the timeout of the query, see
     * {@link SlowQueryDetector::checkTimeout()}.
     * {@link KylinRuntimeException} is risking inconsistent states to stop the query.
     * {@link InterruptedException} is for other interruptions.
     * @param cause the reason of canceling the current query or interrupt the working thread
     * @param step the processing point
     */
    public static void checkQueryCanceledOrThreadInterrupted(String cause, String step) throws InterruptedException {
        SlowQueryDetector.QueryEntry entry = SlowQueryDetector.getRunningQueries().getOrDefault(Thread.currentThread(),
                null);
        if (entry != null) {
            if (entry.isStopByUser() && entry.getPlannerCancelFlag().isCancelRequested()
                    && Thread.currentThread().isInterrupted()) {
                throw new UserStopQueryException(String.format(Locale.ROOT,
                        "Manually stop the query %s. Caused: %s. Step: %s", entry.getQueryId(), cause, step));
            }

            if (entry.getPlannerCancelFlag().isCancelRequested() && entry.isTimeoutStop) {
                QueryContext.current().getQueryTagInfo().setTimeout(true);
                throw new KylinTimeoutException(String.format(Locale.ROOT,
                        "Run out of time of the query %s. Caused: %s. Step: %s", entry.getQueryId(), cause, step));
            }

            if (entry.isStopByUser() || entry.getPlannerCancelFlag().isCancelRequested()) {
                throw new UserStopQueryException(String.format(Locale.ROOT,
                        "You are trying to cancel the query %s with inconsistent states:"
                                + " [isStopByUser=%s, isCancelRequested=%s]! Caused: %s. Step: %s",
                        entry.getQueryId(), entry.isStopByUser(), entry.getPlannerCancelFlag().isCancelRequested(),
                        cause, step));
            }
        }
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException(String.format(Locale.ROOT, "Interrupted on thread %s. Caused: %s. Step: %s",
                    Thread.currentThread().getName(), cause, step));
        }
    }
}
