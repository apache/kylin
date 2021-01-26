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

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.cuboid.algorithm.generic.GeneticAlgorithm;
import org.apache.kylin.cube.cuboid.algorithm.greedy.GreedyAlgorithm;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.cache.Cache;
import org.apache.kylin.shaded.com.google.common.cache.CacheBuilder;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class CuboidRecommender {
    private static final Logger logger = LoggerFactory.getLogger(CuboidRecommender.class);

    private static Cache<String, Map<Long, Long>> cuboidRecommendCache = CacheBuilder.newBuilder()
            .removalListener(notification ->
                logger.info("Recommended cuboids for cube " + notification.getKey() + " is removed due to "
                        + notification.getCause())
            ).maximumSize(KylinConfig.getInstanceFromEnv().getCubePlannerRecommendCuboidCacheMaxSize())
            .expireAfterWrite(1, TimeUnit.DAYS).build();

    private class CuboidRecommenderSyncListener extends Broadcaster.Listener {
        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            cuboidRecommendCache.invalidateAll();
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            cuboidRecommendCache.invalidate(cacheKey);
        }
    }

    public CuboidRecommender() {
        Broadcaster.getInstance(KylinConfig.getInstanceFromEnv())
                .registerStaticListener(new CuboidRecommenderSyncListener(), "cube", "cube_desc");
    }

    private static CuboidRecommender instance = new CuboidRecommender();

    public static CuboidRecommender getInstance() {
        return instance;
    }

    /**
     * Get recommend cuboids with their row count stats with cache
     */
    public Map<Long, Long> getRecommendCuboidList(final CuboidStats cuboidStats, final KylinConfig kylinConfig) {
        if (cuboidStats == null) {
            return null;
        }
        final String key = cuboidStats.getKey();
        Map<Long, Long> results = cuboidRecommendCache.getIfPresent(key);
        if (results == null) {
            try {
                results = cuboidRecommendCache.get(key, new Callable<Map<Long, Long>>() {
                    @Override
                    public Map<Long, Long> call() throws Exception {
                        // recommending flag
                        Map<Long, Long> emptyMap = Maps.newHashMap();
                        cuboidRecommendCache.put(key, emptyMap);
                        try {
                            Map<Long, Long> recommendCuboid = getRecommendCuboidList(cuboidStats, kylinConfig, true);

                            if (recommendCuboid != null) {
                                logger.info(String.format(Locale.ROOT, "Add recommend cuboids for %s to cache", key));
                                cuboidRecommendCache.put(key, recommendCuboid);
                            }

                            return recommendCuboid;
                        } catch (Exception e) {
                            cuboidRecommendCache.invalidate(key);
                            logger.error(
                                    String.format(Locale.ROOT, "Failed to get recommend cuboids for %s in cache", key),
                                    e);
                            throw e;
                        }
                    }
                });
            } catch (ExecutionException e) {
                logger.error(String.format(Locale.ROOT, "Failed to get recommend cuboids for %s", key));
            }
        }
        return results;
    }

    /**
     * Get recommend cuboids with their row count stats without cache
     */
    public Map<Long, Long> getRecommendCuboidList(CuboidStats cuboidStats, KylinConfig kylinConf,
            boolean ifForceRecommend) {
        long threshold1 = 1L << kylinConf.getCubePlannerAgreedyAlgorithmAutoThreshold();
        long threshold2 = 1L << kylinConf.getCubePlannerGeneticAlgorithmAutoThreshold();
        if (threshold1 >= threshold2) {
            logger.error("Invalid Cube Planner Algorithm configuration");
            return null;
        }

        int allCuboidCount = cuboidStats.getAllCuboidsForMandatory().size()
                + cuboidStats.getAllCuboidsForSelection().size();

        if (!ifForceRecommend && allCuboidCount <= threshold1) {
            return null;
        }

        BenefitPolicy benefitPolicy = new PBPUSCalculator(cuboidStats);
        CuboidRecommendAlgorithm algorithm = null;

        if (allCuboidCount <= threshold2) {
            algorithm = new GreedyAlgorithm(-1, benefitPolicy, cuboidStats);
        } else {
            algorithm = new GeneticAlgorithm(-1, benefitPolicy, cuboidStats);
        }

        long startTime = System.currentTimeMillis();
        logger.info("Cube Planner Algorithm started at {}", startTime);
        List<Long> recommendCuboidList = algorithm.recommend(
                kylinConf.getCubePlannerExpansionRateThreshold() / kylinConf.getStorageCompressionRatio());
        logger.info("Cube Planner Algorithm ended at {}", System.currentTimeMillis() - startTime);

        if (recommendCuboidList.size() < allCuboidCount) {
            logger.info("Cube Planner Algorithm chooses {} most effective cuboids to build among of all {} cuboids.",
                    recommendCuboidList.size(), allCuboidCount);
        }

        Map<Long, Long> recommendCuboidsWithStats = Maps.newLinkedHashMap();
        for (Long cuboid : recommendCuboidList) {
            if (cuboid == 0L) {
                // for zero cuboid, just simply recommend the cheapest cuboid.
                handleCuboidZeroRecommend(cuboidStats, recommendCuboidsWithStats);
            } else {
                recommendCuboidsWithStats.put(cuboid, cuboidStats.getCuboidCount(cuboid));
            }
        }

        return recommendCuboidsWithStats;
    }

    private void handleCuboidZeroRecommend(CuboidStats cuboidStats, Map<Long, Long> recommendCuboidsWithStats) {
        Map<Long, Long> statistics = cuboidStats.getStatistics();
        Long cheapestCuboid = null;
        Long cheapestCuboidCount = Long.MAX_VALUE;
        for (Map.Entry<Long, Long> cuboidStatsEntry : statistics.entrySet()) {
            if (cuboidStatsEntry.getValue() < cheapestCuboidCount) {
                cheapestCuboid = cuboidStatsEntry.getKey();
                cheapestCuboidCount = cuboidStatsEntry.getValue();
            }
        }
        if (cheapestCuboid != null) {
            logger.info("recommend cuboid:{} instead of cuboid zero", cheapestCuboid);
            recommendCuboidsWithStats.put(cheapestCuboid, cheapestCuboidCount);
        }
    }
}
