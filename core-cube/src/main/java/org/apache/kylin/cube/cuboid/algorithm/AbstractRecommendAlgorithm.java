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

package org.apache.kylin.cube.cuboid.algorithm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractRecommendAlgorithm implements CuboidRecommendAlgorithm {
    private static final Logger logger = LoggerFactory.getLogger(AbstractRecommendAlgorithm.class);

    protected final CuboidStats cuboidStats;
    protected final BenefitPolicy benefitPolicy;

    private AtomicBoolean cancelRequested = new AtomicBoolean(false);
    private AtomicBoolean canceled = new AtomicBoolean(false);

    private long timeoutMillis;

    public AbstractRecommendAlgorithm(final long timeout, BenefitPolicy benefitPolicy, CuboidStats cuboidStats) {
        if (timeout <= 0) {
            this.timeoutMillis = Long.MAX_VALUE;
        } else {
            this.timeoutMillis = timeout;
        }
        this.cuboidStats = cuboidStats;
        this.benefitPolicy = benefitPolicy;
    }

    @Override
    public List<Long> recommend(double expansionRate) {
        double spaceLimit = cuboidStats.getBaseCuboidSize() * expansionRate;
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
            logger.warn("Algorithm is canceled.");
            return true;
        }
        final long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis > timeoutMillis) {
            canceled.set(true);
            logger.warn("Algorithm exceeds time limit.");
            return true;
        }
        return false;
    }
}
