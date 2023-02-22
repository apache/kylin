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

package org.apache.kylin.metadata.cube.planner.algorithm;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractRecommendAlgorithm implements LayoutRecommendAlgorithm {

    protected final LayoutStats layoutStats;
    protected final BenefitPolicy benefitPolicy;

    private final AtomicBoolean cancelRequested = new AtomicBoolean(false);
    private final AtomicBoolean canceled = new AtomicBoolean(false);

    private final long timeoutMillis;

    public AbstractRecommendAlgorithm(final long timeout, BenefitPolicy benefitPolicy, LayoutStats layoutStats) {
        if (timeout <= 0) {
            this.timeoutMillis = Long.MAX_VALUE;
        } else {
            this.timeoutMillis = timeout;
        }
        this.layoutStats = layoutStats;
        this.benefitPolicy = benefitPolicy;
    }

    @Override
    public List<BigInteger> recommend(double expansionRate) {
        double spaceLimit = layoutStats.getBaseLayoutSize() * expansionRate;
        log.info("space limit for the algorithm is {} with expansion rate {}", spaceLimit, expansionRate);
        return start(spaceLimit);
    }

    @Override
    public void cancel() {
        cancelRequested.set(true);
    }

    /**
     * Checks whether the algorithm has been canceled or timed out.
     */
    protected boolean shouldCancel() {
        if (canceled.get()) {
            return true;
        }
        if (cancelRequested.get()) {
            canceled.set(true);
            cancelRequested.set(false);
            log.warn("Algorithm is canceled.");
            return true;
        }
        final long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis > timeoutMillis) {
            canceled.set(true);
            log.warn("Algorithm exceeds time limit.");
            return true;
        }
        return false;
    }
}
