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

package org.apache.kylin.common.scheduler;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.persistence.transaction.BroadcastEventReadyNotifier;

import io.kyligence.kap.guava20.shaded.common.annotations.VisibleForTesting;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.guava20.shaded.common.eventbus.AsyncEventBus;
import io.kyligence.kap.guava20.shaded.common.eventbus.EventBus;
import io.kyligence.kap.guava20.shaded.common.eventbus.SyncThrowExceptionEventBus;
import io.kyligence.kap.guava20.shaded.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventBusFactory {
    private final KylinConfig kylinConfig;

    private EventBus asyncEventBus;
    private EventBus syncEventBus;
    private EventBus broadcastEventBus;
    private EventBus serviceEventBus;

    private ThreadPoolExecutor eventExecutor;
    private ExecutorService broadcastExecutor;

    private final Map<String, RateLimiter> rateLimiters = Maps.newConcurrentMap();

    public static EventBusFactory getInstance() {
        return Singletons.getInstance(EventBusFactory.class);
    }

    private EventBusFactory() {
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
        init();
    }

    private void init() {
        eventExecutor = new ThreadPoolExecutor(kylinConfig.getEventBusHandleThreadCount(),
                kylinConfig.getEventBusHandleThreadCount(), 300L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                new NamedThreadFactory("SchedulerEventBus"));
        broadcastExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("BroadcastEventBus"));
        eventExecutor.allowCoreThreadTimeOut(true);
        asyncEventBus = new AsyncEventBus(eventExecutor);
        syncEventBus = new SyncThrowExceptionEventBus();
        broadcastEventBus = new AsyncEventBus(broadcastExecutor);
        serviceEventBus = new SyncThrowExceptionEventBus();
    }

    public void registerBroadcast(Object broadcastListener) {
        broadcastEventBus.register(broadcastListener);
    }

    public void register(Object listener, boolean isSync) {
        if (isSync) {
            syncEventBus.register(listener);
        } else {
            asyncEventBus.register(listener);
        }
    }

    public void registerService(Object listener) {
        serviceEventBus.register(listener);
    }

    public void unregister(Object listener) {
        try {
            asyncEventBus.unregister(listener);
        } catch (IllegalArgumentException ignore) {
            // ignore it
        }
        try {
            syncEventBus.unregister(listener);
        } catch (IllegalArgumentException ignore) {
            // ignore it
        }
        try {
            broadcastEventBus.unregister(listener);
        } catch (IllegalArgumentException ignore) {
            // ignore it
        }
    }

    public void unregisterService(Object listener) {
        try {
            serviceEventBus.unregister(listener);
        } catch (IllegalArgumentException ignore) {
            // ignore it
        }
    }

    public void postWithLimit(SchedulerEventNotifier event) {
        rateLimiters.putIfAbsent(event.toString(), RateLimiter.create(kylinConfig.getSchedulerLimitPerMinute() / 60.0));
        RateLimiter rateLimiter = rateLimiters.get(event.toString());

        if (rateLimiter.tryAcquire()) {
            postAsync(event);
        }
    }

    public void postAsync(SchedulerEventNotifier event) {
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.SCHEDULE_CATEGORY)) {
            log.debug("Post event {} async", event);
        }
        if (event instanceof BroadcastEventReadyNotifier) {
            broadcastEventBus.post(event);
        } else {
            asyncEventBus.post(event);
        }
    }

    public void postSync(Object event) {
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.SCHEDULE_CATEGORY)) {
            log.debug("Post event {} sync", event);
        }
        syncEventBus.post(event);
    }

    public void callService(Object event) {
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.SCHEDULE_CATEGORY)) {
            log.debug("Post Service event {} sync", event);
        }
        serviceEventBus.post(event);
    }

    @VisibleForTesting
    public void restart() {
        stopThreadPool(eventExecutor);
        stopThreadPool(broadcastExecutor);
        init();
    }

    private void stopThreadPool(ExecutorService executor) {
        executor.shutdown();
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.SCHEDULE_CATEGORY)) {
            if (!executor.awaitTermination(6000, TimeUnit.SECONDS)) {
                ExecutorServiceUtil.forceShutdown(executor);
            }
        } catch (InterruptedException ex) {
            ExecutorServiceUtil.forceShutdown(executor);
            Thread.currentThread().interrupt();
        }
    }
}
