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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Maps;

public class CuboidRecommender {
    private static final Logger logger = LoggerFactory.getLogger(CuboidRecommender.class);

    private static Cache<String, Map<Long, Long>> cuboidRecommendCache = CacheBuilder.newBuilder()
            .removalListener(new RemovalListener<String, Map<Long, Long>>() {
                @Override
                public void onRemoval(RemovalNotification<String, Map<Long, Long>> notification) {
                    logger.info("Dict with resource path " + notification.getKey() + " is removed due to "
                            + notification.getCause());
                }
            }).maximumSize(KylinConfig.getInstanceFromEnv().getCubePlannerRecommendCuboidCacheMaxSize())
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
        Broadcaster.getInstance(KylinConfig.getInstanceFromEnv()).registerListener(new CuboidRecommenderSyncListener(),
                "cube");
    }

    private static CuboidRecommender instance = new CuboidRecommender();

    public static CuboidRecommender getInstance() {
        return instance;
    }

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
                            Map<Long, Long> recommendCuboid = getRecommendCuboidList(cuboidStats, kylinConfig,
                                    true);

                            if (recommendCuboid != null) {
                                logger.info("Add recommend cuboids for " + key + " to cache");
                                cuboidRecommendCache.put(key, recommendCuboid);
                            }

                            return recommendCuboid;
                        } catch (Exception e) {
                            cuboidRecommendCache.invalidate(key);
                            logger.error("Failed to get recommend cuboids for " + key + " in cache", e);
                            throw e;
                        }
                    }
                });
            } catch (ExecutionException e) {
                logger.error("Failed to get recommend cuboids for " + key);
            }
        }
        return results;
    }

    public Map<Long, Long> getRecommendCuboidList(CuboidStats cuboidStats, KylinConfig kylinConf,
            boolean ifForceRecommend) {
        long Threshold1 = 1L << kylinConf.getCubePlannerAgreedyAlgorithmAutoThreshold();
        long Threshold2 = 1L << kylinConf.getCubePlannerGeneticAlgorithmAutoThreshold();
        if (Threshold1 >= Threshold2) {
            logger.error("Invalid Cube Planner Algorithm configuration");
            return null;
        }

        int allCuboidCount = cuboidStats.getAllCuboidsForMandatory().size()
                + cuboidStats.getAllCuboidsForSelection().size();

        BenefitPolicy benefitPolicy = new PBPUSCalculator(cuboidStats);
        CuboidRecommendAlgorithm algorithm = null;

        if (allCuboidCount <= Threshold2) {
            algorithm = new GreedyAlgorithm(-1, benefitPolicy, cuboidStats);
        } else {
            algorithm = new GeneticAlgorithm(-1, benefitPolicy, cuboidStats);
        }

        long startTime = System.currentTimeMillis();
        logger.info("Cube Planner Algorithm started at " + startTime);
        List<Long> recommendCuboidList = algorithm.recommend(kylinConf.getCubePlannerExpansionRateThreshold());
        logger.info("Cube Planner Algorithm ended at " + (System.currentTimeMillis() - startTime));

        if (recommendCuboidList.size() < allCuboidCount) {
            logger.info("Cube Planner Algorithm chooses " + recommendCuboidList.size()
                    + " most effective cuboids to build among of all " + allCuboidCount + " cuboids.");
        }

        Map<Long, Long> recommendCuboidsWithStats = Maps.newLinkedHashMap();
        for (Long cuboid : recommendCuboidList) {
            if (cuboid.equals(cuboidStats.getBaseCuboid())) {
                recommendCuboidsWithStats.put(cuboid, cuboidStats.getCuboidCount(cuboid));
            } else if (cuboidStats.getAllCuboidsForSelection().contains(cuboid)) {
                recommendCuboidsWithStats.put(cuboid, cuboidStats.getCuboidCount(cuboid));
            } else {
                recommendCuboidsWithStats.put(cuboid, -1L);
            }
        }

        if (!ifForceRecommend && allCuboidCount <= Threshold1) {
            return null;
        }
        return recommendCuboidsWithStats;
    }
}
