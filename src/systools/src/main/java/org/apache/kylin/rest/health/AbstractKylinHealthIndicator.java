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
package org.apache.kylin.rest.health;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.HealthIndicator;

public abstract class AbstractKylinHealthIndicator implements HealthIndicator {
    public static final Logger logger = LoggerFactory.getLogger(AbstractKylinHealthIndicator.class);

    protected KylinConfig config;

    protected int warningResponseMs;
    protected int errorResponseMs;

    protected void checkTime(long start, String operation) throws InterruptedException {
        // in case canary was timeout
        if (Thread.interrupted())
            throw new InterruptedException();

        long response = System.currentTimeMillis() - start;
        logger.trace("{} took {} ms", operation, response);

        if (response > errorResponseMs) {
            throw new RuntimeException("check time is time out");
        } else if (response > warningResponseMs) {
            logger.warn("found warning, {} took {} ms", operation, response);
        }
    }
}
