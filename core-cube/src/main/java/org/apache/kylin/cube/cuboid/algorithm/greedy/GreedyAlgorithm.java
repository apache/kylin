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

package org.apache.kylin.cube.cuboid.algorithm.greedy;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kylin.cube.cuboid.algorithm.AbstractRecommendAlgorithm;
import org.apache.kylin.cube.cuboid.algorithm.BenefitPolicy;
import org.apache.kylin.cube.cuboid.algorithm.CuboidBenefitModel;
import org.apache.kylin.cube.cuboid.algorithm.CuboidStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.kylin.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A simple implementation of the Greedy Algorithm , it chooses the cuboids which give
 * the greatest benefit based on expansion rate and time limitation.
 */
public class GreedyAlgorithm extends AbstractRecommendAlgorithm {
    private static final Logger logger = LoggerFactory.getLogger(GreedyAlgorithm.class);

    private static final int THREAD_NUM = 8;
    private ExecutorService executor;

    private Set<Long> selected = Sets.newLinkedHashSet();
    private List<Long> remaining = Lists.newLinkedList();

    public GreedyAlgorithm(final long timeout, BenefitPolicy benefitPolicy, CuboidStats cuboidStats) {
        super(timeout, benefitPolicy, cuboidStats);
    }

    @Override
    public List<Long> start(double spaceLimit) {
        logger.info("Greedy Algorithm started.");
        executor = Executors.newFixedThreadPool(THREAD_NUM,
                new ThreadFactoryBuilder().setNameFormat("greedy-algorithm-benefit-calculator-pool-%d").build());

        //Initial mandatory cuboids
        selected.clear();
        double remainingSpace = spaceLimit;
        for (Long mandatoryOne : cuboidStats.getAllCuboidsForMandatory()) {
            selected.add(mandatoryOne);
            if (cuboidStats.getCuboidSize(mandatoryOne) != null) {
                remainingSpace -= cuboidStats.getCuboidSize(mandatoryOne);
            }
        }
        //Initial remaining cuboid set
        remaining.clear();
        remaining.addAll(cuboidStats.getAllCuboidsForSelection());

        long round = 0;
        boolean doesRemainSpace = true;
        while (!shouldCancel() && doesRemainSpace) {
            // Choose one cuboId having the maximum benefit per unit space in all available list
            CuboidBenefitModel best = recommendBestOne();

            // If return null, then we should finish the process and return
            // If we finally find the cuboid selected does not meet a minimum threshold of benefit (for
            // example, a cuboid with 0.99M roll up from a parent cuboid with 1M
            // rows), then we should finish the process and return
            if (best != null && benefitPolicy.ifEfficient(best)) {
                remainingSpace -= cuboidStats.getCuboidSize(best.getCuboidId());

                // If we finally find there is no remaining space,  then we should finish the process and return
                if (remainingSpace > 0) {
                    selected.add(best.getCuboidId());
                    remaining.remove(best.getCuboidId());
                    benefitPolicy.propagateAggregationCost(best.getCuboidId(), selected);
                    round++;
                    if (logger.isDebugEnabled()) {
                        logger.debug("Recommend in round {} : {}", round, best);
                    }
                } else {
                    doesRemainSpace = false;
                }
            } else {
                doesRemainSpace = false;
            }
        }

        executor.shutdown();

        List<Long> excluded = Lists.newArrayList(remaining);
        remaining.retainAll(selected);
        Preconditions.checkArgument(remaining.isEmpty(),
                "There should be no intersection between excluded list and selected list.");
        logger.info("Greedy Algorithm finished.");

        if (logger.isDebugEnabled()) {
            logger.debug("Excluded cuboidId size: {}", excluded.size());
            logger.debug("Excluded cuboidId detail:");
            for (Long cuboid : excluded) {
                logger.debug("cuboidId {} and Cost: {} and Space: {}", cuboid,
                        cuboidStats.getCuboidQueryCost(cuboid), cuboidStats.getCuboidSize(cuboid));
            }
            logger.debug("Total Space: {}", spaceLimit - remainingSpace);
            logger.debug("Space Expansion Rate: {}", (spaceLimit - remainingSpace) / cuboidStats.getBaseCuboidSize());
        }
        return Lists.newArrayList(selected);
    }

    private CuboidBenefitModel recommendBestOne() {
        final int selectedSize = selected.size();
        final AtomicReference<CuboidBenefitModel> best = new AtomicReference<>();

        final CountDownLatch counter = new CountDownLatch(remaining.size());
        for (final Long cuboid : remaining) {
            executor.submit(() -> {
                    CuboidBenefitModel currentBest = best.get();
                    assert (selected.size() == selectedSize);
                    CuboidBenefitModel.BenefitModel benefitModel = benefitPolicy.calculateBenefit(cuboid, selected);
                    if (benefitModel != null && (currentBest == null || currentBest.getBenefit() == null
                            || benefitModel.benefit > currentBest.getBenefit())) {
                        while (!best.compareAndSet(currentBest,
                                new CuboidBenefitModel(cuboidStats.getCuboidModel(cuboid), benefitModel))) {
                            currentBest = best.get();
                            if (benefitModel.benefit <= currentBest.getBenefit()) {
                                break;
                            }
                        }
                    }
                    counter.countDown();
                });
        }

        try {
            counter.await();
        } catch (InterruptedException e) {
        }
        return best.get();
    }
}
