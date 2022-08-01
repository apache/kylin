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
package org.apache.kylin.tool.daemon;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.util.Unsafe;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.tool.daemon.handler.DownGradeStateHandler;
import org.apache.kylin.tool.daemon.handler.NormalStateHandler;
import org.apache.kylin.tool.daemon.handler.RestartStateHandler;
import org.apache.kylin.tool.daemon.handler.SuicideStateHandler;
import org.apache.kylin.tool.daemon.handler.UpGradeStateHandler;
import org.apache.kylin.tool.daemon.handler.WarnStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class KapGuardian {
    private static final Logger logger = LoggerFactory.getLogger(KapGuardian.class);

    private List<HealthChecker> healthCheckers = Lists.newArrayList();

    private Map<CheckStateEnum, List<CheckStateHandler>> checkStateHandlersMap = new EnumMap<>(CheckStateEnum.class);

    private KylinConfig config = KylinConfig.getInstanceFromEnv();

    private ScheduledExecutorService executor;

    /**
     * uuid = KYLIN_HOME & server port
     */
    public KapGuardian() {
        if (!config.isGuardianEnabled()) {
            logger.warn("Do not enable to start Guardian Process, exit 0!");
            Unsafe.systemExit(0);
        }

        executor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("ke-guardian-process").build());

        this.loadCheckers();
        this.initCheckStateHandler();
    }

    public static void main(String[] args) {
        logger.info("Guardian Process starting...");

        try {
            KapGuardian kapGuardian = new KapGuardian();
            kapGuardian.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Guardian Process of KE instance port[{}] KYLIN_HOME[{}] stopped",
                        kapGuardian.getServerPort(), kapGuardian.getKylinHome());
                kapGuardian.stop();
            }));
        } catch (Exception e) {
            logger.info("Guardian Process start failed", e);
            Unsafe.systemExit(1);
        }

        logger.info("Guardian Process started...");
    }

    public void start() {
        executor.scheduleWithFixedDelay(() -> {
            try {
                logger.info("Guardian Process: start to run health checkers ...");
                for (HealthChecker healthChecker : healthCheckers) {
                    CheckResult checkResult = healthChecker.check();
                    List<CheckStateHandler> handlers = checkStateHandlersMap.get(checkResult.getCheckState());

                    if (CollectionUtils.isEmpty(handlers)) {
                        continue;
                    }

                    for (CheckStateHandler handler : handlers) {
                        HandleResult handleResult = handler.handle(checkResult);
                        if (HandleStateEnum.STOP_CHECK == handleResult.getHandleState()) {
                            logger.info("Guardian Process: state [{}] found, stop check!", HandleStateEnum.STOP_CHECK);
                            return;
                        }
                    }
                }

                logger.info("Guardian Process: health check finished ...");
            } catch (Exception e) {
                logger.info("Guardian Process: failed to run health check!", e);
            }
        }, config.getGuardianCheckInitDelay(), config.getGuardianCheckInterval(), TimeUnit.SECONDS);
    }

    public void stop() {
        if (null != executor && !executor.isShutdown()) {
            ExecutorServiceUtil.forceShutdown(executor);
        }
    }

    /**
     * optimize check config
     */
    public void loadCheckers() {
        String healthCheckersStr = config.getGuardianHealthCheckers();
        if (StringUtils.isNotBlank(healthCheckersStr)) {
            Map<Integer, List<HealthChecker>> checkerMap = new TreeMap<>();
            for (String healthCheckerClassName : healthCheckersStr.split(",")) {
                if (StringUtils.isNotBlank(healthCheckerClassName.trim())) {
                    HealthChecker healthChecker = (HealthChecker) ClassUtil.newInstance(healthCheckerClassName);
                    if (!checkerMap.containsKey(healthChecker.getPriority())) {
                        checkerMap.put(healthChecker.getPriority(), Lists.newArrayList());
                    }
                    checkerMap.get(healthChecker.getPriority()).add(healthChecker);
                }
            }

            healthCheckers.addAll(checkerMap.values().stream().flatMap(List::stream).collect(Collectors.toList()));
        }
    }

    public void initCheckStateHandler() {
        checkStateHandlersMap.put(CheckStateEnum.RESTART, Lists.newArrayList(new RestartStateHandler()));
        checkStateHandlersMap.put(CheckStateEnum.NORMAL, Lists.newArrayList(new NormalStateHandler()));
        checkStateHandlersMap.put(CheckStateEnum.WARN, Lists.newArrayList(new WarnStateHandler()));
        checkStateHandlersMap.put(CheckStateEnum.SUICIDE, Lists.newArrayList(new SuicideStateHandler()));
        checkStateHandlersMap.put(CheckStateEnum.QUERY_UPGRADE, Lists.newArrayList(new UpGradeStateHandler()));
        checkStateHandlersMap.put(CheckStateEnum.QUERY_DOWNGRADE, Lists.newArrayList(new DownGradeStateHandler()));
        checkStateHandlersMap.put(CheckStateEnum.OTHER, Lists.newArrayList());
    }

    public String getServerPort() {
        return config.getServerPort();
    }

    public String getKylinHome() {
        return KylinConfig.getKylinHome();
    }

}
