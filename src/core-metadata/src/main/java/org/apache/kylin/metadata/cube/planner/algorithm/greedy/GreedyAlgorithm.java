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

package org.apache.kylin.metadata.cube.planner.algorithm.greedy;

import java.math.BigInteger;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kylin.metadata.cube.planner.algorithm.AbstractRecommendAlgorithm;
import org.apache.kylin.metadata.cube.planner.algorithm.BenefitPolicy;
import org.apache.kylin.metadata.cube.planner.algorithm.LayoutBenefitModel;
import org.apache.kylin.metadata.cube.planner.algorithm.LayoutStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A simple implementation of the Greedy Algorithm , it chooses the layouts which give
 * the greatest benefit based on expansion rate and time limitation.
 */
public class GreedyAlgorithm extends AbstractRecommendAlgorithm {
    private static final Logger logger = LoggerFactory.getLogger(GreedyAlgorithm.class);

    private static final int THREAD_NUM = 8;
    private ExecutorService executor;

    private final Set<BigInteger> selected = Sets.newLinkedHashSet();
    private final List<BigInteger> remaining = Lists.newLinkedList();

    public GreedyAlgorithm(final long timeout, BenefitPolicy benefitPolicy, LayoutStats layoutStats) {
        super(timeout, benefitPolicy, layoutStats);
    }

    @Override
    public List<BigInteger> start(double spaceLimit) {
        logger.info("Greedy Algorithm started.");
        executor = Executors.newFixedThreadPool(THREAD_NUM,
                new ThreadFactoryBuilder().setNameFormat("greedy-algorithm-benefit-calculator-pool-%d").build());

        //Initial mandatory layouts
        selected.clear();
        double remainingSpace = spaceLimit;
        for (BigInteger mandatoryOne : layoutStats.getAllLayoutsForMandatory()) {
            selected.add(mandatoryOne);
            if (layoutStats.getLayoutSize(mandatoryOne) != null) {
                remainingSpace -= layoutStats.getLayoutSize(mandatoryOne);
            }
        }
        //Initial remaining layout set
        remaining.clear();
        remaining.addAll(layoutStats.getAllLayoutsForSelection());

        long round = 0;
        while (!shouldCancel()) {
            // Choose one layout having the maximum benefit per unit space in all available list
            LayoutBenefitModel best = recommendBestOne();
            // If return null, then we should finish the process and return
            if (best == null) {
                logger.info("Greedy algorithm ends due to cannot find next best one");
                break;
            }
            // If we finally find the layout selected does not meet a minimum threshold of benefit (for
            // example, a layout with 0.99M roll up from a parent layout with 1M
            // rows), then we should finish the process and return
            if (!benefitPolicy.ifEfficient(best)) {
                logger.info("Greedy algorithm ends due to the benefit of the best one is not efficient {}",
                        best.getBenefit());
                break;
            }

            remainingSpace -= layoutStats.getLayoutSize(best.getLayoutId());
            // If we finally find there is no remaining space,  then we should finish the process and return
            if (remainingSpace <= 0) {
                logger.info("Greedy algorithm ends due to there's no remaining space");
                break;
            }
            selected.add(best.getLayoutId());
            remaining.remove(best.getLayoutId());
            benefitPolicy.propagateAggregationCost(best.getLayoutId(), selected);
            round++;
            if (logger.isTraceEnabled()) {
                logger.trace(String.format(Locale.ROOT, "Recommend in round %d : %s", round, best));
            }
        }

        executor.shutdown();

        List<BigInteger> excluded = Lists.newArrayList(remaining);
        remaining.retainAll(selected);
        Preconditions.checkArgument(remaining.isEmpty(),
                "There should be no intersection between excluded list and selected list.");
        logger.info("Greedy Algorithm finished.");

        if (logger.isTraceEnabled()) {
            logger.trace("Excluded layoutId size: {}", excluded.size());
            logger.trace("Excluded layoutId detail:");
            for (BigInteger layout : excluded) {
                logger.trace("layoutId {} and Cost: {} and Space: {}", layout, layoutStats.getLayoutQueryCost(layout),
                        layoutStats.getLayoutSize(layout));
            }
            logger.trace("Total Space: {}", spaceLimit - remainingSpace);
            logger.trace("Space Expansion Rate: {}", (spaceLimit - remainingSpace) / layoutStats.getBaseLayoutSize());
        }
        return Lists.newArrayList(selected);
    }

    private LayoutBenefitModel recommendBestOne() {
        final int selectedSize = selected.size();
        final AtomicReference<LayoutBenefitModel> best = new AtomicReference<>();

        final CountDownLatch counter = new CountDownLatch(remaining.size());
        for (final BigInteger layout : remaining) {
            executor.submit(() -> {
                LayoutBenefitModel currentBest = best.get();
                assert (selected.size() == selectedSize);
                LayoutBenefitModel.BenefitModel benefitModel = benefitPolicy.calculateBenefit(layout, selected);
                if (benefitModel != null && (currentBest == null || currentBest.getBenefit() == null
                        || benefitModel.benefit > currentBest.getBenefit())) {
                    while (!best.compareAndSet(currentBest,
                            new LayoutBenefitModel(layoutStats.getLayoutModel(layout), benefitModel))) {
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
        } catch (InterruptedException ignored) {
        }
        return best.get();
    }
}
