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

package org.apache.kylin.metrics.lib.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metrics.QuerySparkMetrics;
import org.apache.kylin.metrics.lib.ActiveReservoir;
import org.apache.kylin.metrics.lib.ActiveReservoirRecordFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

public class MetricsSystem extends MetricRegistry {
    public static final MetricsSystem Metrics = new MetricsSystem();
    private static final Logger logger = LoggerFactory.getLogger(MetricsSystem.class);
    private final ConcurrentHashMap<String, ActiveReservoir> activeReservoirs;

    private MetricsSystem() {
        activeReservoirs = new ConcurrentHashMap<>();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Closing Metrics System");
                try {
                    shutdown();
                } catch (IOException e) {
                    logger.error("error during shutdown activeReservoirs and listeners", e);
                }
                logger.info("Closed Metrics System");
            }
        });
    }

    public void shutdown() throws IOException {
        QuerySparkMetrics.getInstance().getQueryExecutionMetricsMap().invalidateAll();
        for (ActiveReservoir entry : activeReservoirs.values()) {
            entry.close();
        }
    }

    public ActiveReservoir activeReservoir(String name) {
        return getOrAddActiveReservoir(name);
    }

    public ActiveReservoir register(String name, ActiveReservoir activeReservoir) {
        if (name == null || activeReservoir == null) {
            throw new IllegalArgumentException("neither of name or ActiveReservoir can be null");
        }
        final ActiveReservoir existingReservoir = activeReservoirs.putIfAbsent(name, activeReservoir);
        if (existingReservoir == null) {
            onActiveReservoirAdded(activeReservoir);
        } else {
            throw new IllegalArgumentException("An active reservoir named " + name + " already exists");
        }

        return activeReservoir;
    }

    /**
     * Removes the active reservoir with the given name.
     *
     * @param name the name of the active reservoir
     * @return whether or not the active reservoir was removed
     */
    public boolean removeActiveReservoir(String name) {
        final ActiveReservoir recordReservoir = activeReservoirs.remove(name);
        if (recordReservoir != null) {
            onActiveReservoirRemoved(recordReservoir);
            return true;
        }
        return false;
    }

    /**
     * Removes all active reservoirs which match the given filter.
     *
     * @param filter a filter
     */
    public void removeActiveReservoirMatching(ActiveReservoirRecordFilter filter) {
        for (Map.Entry<String, ActiveReservoir> entry : activeReservoirs.entrySet()) {
            if (filter.matches(entry.getKey(), entry.getValue())) {
                removeActiveReservoir(entry.getKey());
            }
        }
    }

    private void onActiveReservoirAdded(ActiveReservoir activeReservoir) {
        activeReservoir.start();
    }

    private void onActiveReservoirRemoved(ActiveReservoir activeReservoir) {
        try {
            activeReservoir.close();
        } catch (IOException e) {
        }
    }

    /**
     * Returns a map of all the active reservoirs in the metrics system and their names.
     *
     * @return all the active reservoirs in the metrics system
     */
    public SortedMap<String, ActiveReservoir> getActiveReservoirs() {
        return getActiveReservoirs(ActiveReservoirRecordFilter.ALL);
    }

    /**
     * Returns a map of all the active reservoirs in the metrics system and their names which match the given filter.
     *
     * @param filter    the active reservoir filter to match
     * @return all the active reservoirs in the metrics system
     */
    public SortedMap<String, ActiveReservoir> getActiveReservoirs(ActiveReservoirRecordFilter filter) {
        final TreeMap<String, ActiveReservoir> reservoirs = new TreeMap<>();
        for (Map.Entry<String, ActiveReservoir> entry : activeReservoirs.entrySet()) {
            if (filter.matches(entry.getKey(), entry.getValue())) {
                reservoirs.put(entry.getKey(), entry.getValue());
            }
        }
        return Collections.unmodifiableSortedMap(reservoirs);
    }

    private ActiveReservoir getOrAddActiveReservoir(String name) {
        ActiveReservoir activeReservoir = activeReservoirs.get(name);
        if (activeReservoir != null) {
            return activeReservoir;
        } else {
            String defaultActiveReservoirClass = KylinConfig.getInstanceFromEnv()
                    .getKylinMetricsActiveReservoirDefaultClass();
            try {
                activeReservoir = (ActiveReservoir) Class.forName(defaultActiveReservoirClass).getConstructor()
                        .newInstance();
            } catch (Exception e) {
                logger.warn(
                        "Failed to initialize the " + defaultActiveReservoirClass + ". The StubReservoir will be used");
                activeReservoir = new StubReservoir();
            }
            return register(name, activeReservoir);
        }
    }

}
