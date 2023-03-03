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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.query.exception.UserStopQueryException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryInterruptChecker {

    private QueryInterruptChecker() {
        // This is Utils.
    }

    public static void checkThreadInterrupted(String errorMsgLog, String stepInfo) {
        if (Thread.currentThread().isInterrupted()) {
            log.error("{} {}", QueryContext.current().getQueryId(), errorMsgLog);
            if (SlowQueryDetector.getRunningQueries().get(Thread.currentThread()).isStopByUser()) {
                throw new UserStopQueryException("");
            }
            QueryContext.current().getQueryTagInfo().setTimeout(true);
            throw new KylinTimeoutException("The query exceeds the set time limit of "
                    + KylinConfig.getInstanceFromEnv().getQueryTimeoutSeconds() + "s. " + stepInfo);
        }
    }
}
