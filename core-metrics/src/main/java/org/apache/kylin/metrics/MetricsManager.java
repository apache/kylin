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

package org.apache.kylin.metrics;

import static org.apache.kylin.metrics.lib.impl.MetricsSystem.Metrics;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metrics.lib.ActiveReservoir;
import org.apache.kylin.metrics.lib.ActiveReservoirReporter;
import org.apache.kylin.metrics.lib.Record;
import org.apache.kylin.metrics.lib.Sink;
import org.apache.kylin.metrics.lib.impl.MetricsSystem;
import org.apache.kylin.metrics.lib.impl.ReporterBuilder;
import org.apache.kylin.metrics.lib.impl.StubSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

/**
 * A metric system using a system cube to store/analyze metric information.
 */
public class MetricsManager {

    public static final String SYSTEM_PROJECT = "KYLIN_SYSTEM";
    private static final Logger logger = LoggerFactory.getLogger(MetricsManager.class);
    private static final MetricsManager instance = new MetricsManager();
    private static final String METHOD_FOR_REGISTRY = "forRegistry";
    private static Map<ActiveReservoir, List<Pair<Class<? extends ActiveReservoirReporter>, Properties>>> sourceReporterBindProps = Maps
            .newHashMap();
    private static Sink scSink;
    private final Set<String> activeReservoirPointers;

    private MetricsManager() {
        activeReservoirPointers = Sets.newHashSet();
    }

    public static MetricsManager getInstance() {
        return instance;
    }

    /**
     * This method is called by Spring Framework at kylinMetrics.xml
     */
    public static void initMetricsManager(Sink systemCubeSink,
            Map<ActiveReservoir, List<Pair<String, Properties>>> sourceReporterBindProperties) {
        setSystemCubeSink(systemCubeSink);
        setSourceReporterBindProps(sourceReporterBindProperties);
        instance.init();
    }

    private static void setSystemCubeSink(Sink systemCubeSink) {
        if (systemCubeSink == null) {
            logger.warn("SystemCubeSink is not set and the default one will be chosen");
            try {
                Class clz = Class.forName(KylinConfig.getInstanceFromEnv().getKylinSystemCubeSinkDefaultClass());
                systemCubeSink = (Sink) clz.getConstructor().newInstance();
            } catch (Exception e) {
                logger.warn("Failed to initialize the "
                        + KylinConfig.getInstanceFromEnv().getKylinSystemCubeSinkDefaultClass()
                        + ". The StubSink will be used");
                systemCubeSink = new StubSink();
            }
        }
        scSink = systemCubeSink;
        System.gc();
    }

    private static void setSourceReporterBindProps(
            Map<ActiveReservoir, List<Pair<String, Properties>>> sourceReporterBindProperties) {
        sourceReporterBindProps = Maps.newHashMapWithExpectedSize(sourceReporterBindProperties.size());
        for (ActiveReservoir activeReservoir : sourceReporterBindProperties.keySet()) {
            List<Pair<Class<? extends ActiveReservoirReporter>, Properties>> values = Lists
                    .newArrayListWithExpectedSize(sourceReporterBindProperties.get(activeReservoir).size());
            sourceReporterBindProps.put(activeReservoir, values);
            for (Pair<String, Properties> entry : sourceReporterBindProperties.get(activeReservoir)) {
                try {
                    Class clz = Class.forName(entry.getFirst());
                    if (ActiveReservoirReporter.class.isAssignableFrom(clz)) {
                        values.add(new Pair(clz, entry.getSecond()));
                    } else {
                        logger.warn("The class {} is not a sub class of {}.", clz, ActiveReservoir.class);
                    }
                } catch (ClassNotFoundException e) {
                    logger.warn("Cannot find class {}", entry.getFirst());
                }
            }
        }
    }

    private void init() {
        if (KylinConfig.getInstanceFromEnv().isKylinMetricsMonitorEnabled()) {
            logger.info("Kylin metrics monitor is enabled.");
            int nameIdx = 0;
            for (ActiveReservoir activeReservoir : sourceReporterBindProps.keySet()) {
                String registerName = MetricsSystem.name(MetricsManager.class,
                        "-" + nameIdx + "-" + activeReservoir.toString());
                activeReservoirPointers.add(registerName);
                List<Pair<Class<? extends ActiveReservoirReporter>, Properties>> reportProps = sourceReporterBindProps
                        .get(activeReservoir);
                for (Pair<Class<? extends ActiveReservoirReporter>, Properties> subEntry : reportProps) {
                    try {
                        Method method = subEntry.getFirst().getMethod(METHOD_FOR_REGISTRY, ActiveReservoir.class);
                        ((ReporterBuilder) method.invoke(null, activeReservoir)).setConfig(subEntry.getSecond()).build()
                                .start();
                    } catch (Exception e) {
                        logger.warn("Cannot initialize ActiveReservoirReporter: Builder class - " + subEntry.getFirst()
                                + ", Properties - " + subEntry.getSecond(), e);
                    }
                }
                Metrics.register(registerName, activeReservoir);
            }
            Preconditions.checkArgument(activeReservoirPointers.size() == sourceReporterBindProps.keySet().size(),
                    "Duplicate register names exist!!!");
        } else {
            logger.info("Kylin metrics monitor is not enabled");
        }
    }

    public void update(Record record) {
        for (String registerName : activeReservoirPointers) {
            Metrics.activeReservoir(registerName).update(record);
        }
    }

    public static String getSystemTableFromSubject(String subject) {
        return scSink.getTableFromSubject(subject);
    }
}