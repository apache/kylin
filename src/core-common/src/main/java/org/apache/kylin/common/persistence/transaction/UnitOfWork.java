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
package org.apache.kylin.common.persistence.transaction;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.code.ErrorCodeSystem;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ThreadViewResourceStore;
import org.apache.kylin.common.persistence.TombRawResource;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.EndUnit;
import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.event.ResourceDeleteEvent;
import org.apache.kylin.common.persistence.event.ResourceRelatedEvent;
import org.apache.kylin.common.persistence.event.StartUnit;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.Unsafe;

import com.google.common.base.Preconditions;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class UnitOfWork {
    public static final String GLOBAL_UNIT = "_global";

    public static final long DEFAULT_EPOCH_ID = -1L;
    public static final int DEFAULT_MAX_RETRY = 3;

    private static EventBusFactory factory;

    static {
        factory = EventBusFactory.getInstance();
    }

    static ThreadLocal<Boolean> replaying = new ThreadLocal<>();
    private static ThreadLocal<UnitOfWorkContext> threadLocals = new ThreadLocal<>();

    public static <T> T doInTransactionWithRetry(Callback<T> f, String unitName) {
        return doInTransactionWithRetry(f, unitName, 3);
    }

    public static <T> T doInTransactionWithRetry(Callback<T> f, String unitName, int maxRetry) {
        return doInTransactionWithRetry(
                UnitOfWorkParams.<T> builder().processor(f).unitName(unitName).maxRetry(maxRetry).build());
    }

    public static <T> T doInTransactionWithRetry(UnitOfWorkParams<T> params) {
        val maxRetry = params.getMaxRetry();
        val f = params.getProcessor();
        // reused transaction, won't retry
        if (isAlreadyInTransaction()) {
            val unitOfWork = UnitOfWork.get();
            unitOfWork.checkReentrant(params);
            try {
                checkEpoch(params);
                f.preProcess();
                return f.process();
            } catch (Throwable throwable) {
                f.onProcessError(throwable);
                throw new TransactionException("transaction failed due to inconsistent state", throwable);
            }
        }

        // new independent transaction with retry
        int retry = 0;
        val traceId = RandomUtil.randomUUIDStr();
        while (retry++ < maxRetry) {
            if (retry > 1) {
                Map<String, String> tags = MetricsGroup.getHostTagMap(params.getUnitName());

                if (!GLOBAL_UNIT.equals(params.getUnitName())) {
                    MetricsGroup.counterInc(MetricsName.TRANSACTION_RETRY_COUNTER, MetricsCategory.PROJECT,
                            params.getUnitName(), tags);
                } else {
                    MetricsGroup.counterInc(MetricsName.TRANSACTION_RETRY_COUNTER, MetricsCategory.GLOBAL, "global",
                            tags);
                }
            }

            val ret = doTransaction(params, retry, traceId);
            if (ret.getSecond()) {
                return ret.getFirst();
            }
        }
        throw new IllegalStateException("Unexpected doInTransactionWithRetry end");
    }

    private static <T> Pair<T, Boolean> doTransaction(UnitOfWorkParams<T> params, int retry, String traceId) {
        Pair<T, Boolean> result = Pair.newPair(null, false);
        UnitOfWorkContext context = null;
        try {
            T ret;

            if (retry != 1) {
                log.debug("UnitOfWork {} in project {} is retrying for {}th time", traceId, params.getUnitName(),
                        retry);
            } else {
                log.debug("UnitOfWork {} started on project {}", traceId, params.getUnitName());
            }

            long startTime = System.currentTimeMillis();
            params.getProcessor().preProcess();
            context = UnitOfWork.startTransaction(params);
            long startTransactionTime = System.currentTimeMillis();
            val waitForLockTime = startTransactionTime - startTime;
            if (waitForLockTime > 3000) {
                log.warn("UnitOfWork {} takes too long time {}ms to start", traceId, waitForLockTime);
            }

            ret = params.getProcessor().process();
            UnitOfWork.endTransaction(traceId, params);
            long duration = System.currentTimeMillis() - startTransactionTime;
            logIfLongTransaction(duration, traceId);

            MetricsGroup.hostTagHistogramUpdate(MetricsName.TRANSACTION_LATENCY, MetricsCategory.PROJECT,
                    !GLOBAL_UNIT.equals(params.getUnitName()) ? params.getUnitName() : "global", duration);

            result = Pair.newPair(ret, true);
        } catch (Throwable throwable) {
            handleError(throwable, params, retry, traceId);
        } finally {
            if (isAlreadyInTransaction()) {
                try {
                    val unitOfWork = UnitOfWork.get();
                    unitOfWork.getCurrentLock().unlock();
                    unitOfWork.cleanResource();
                    if (params.getTempLockName() != null && !params.getTempLockName().equals(params.getUnitName())) {
                        TransactionLock.removeLock(params.getTempLockName());
                    }
                } catch (IllegalStateException e) {
                    //has not hold the lock yet, it's ok
                    log.warn(e.getMessage());
                } catch (Exception e) {
                    log.error("Failed to close UnitOfWork", e);
                }
                threadLocals.remove();
            }
        }

        if (result.getSecond() && context != null) {
            context.onUnitFinished();
        }
        return result;
    }

    private static void logIfLongTransaction(long duration, String traceId) {
        if (duration > 3000) {
            log.warn("UnitOfWork {} takes too long time {}ms to complete", traceId, duration);
            if (duration > 10000) {
                log.warn("current stack: ", new Throwable());
            }
        } else {
            log.debug("UnitOfWork {} takes {}ms to complete", traceId, duration);
        }
    }

    static <T> UnitOfWorkContext startTransaction(UnitOfWorkParams<T> params) throws Exception {
        val project = params.getUnitName();
        val readonly = params.isReadonly();
        checkEpoch(params);
        val lock = params.getTempLockName() == null ? TransactionLock.getLock(project, readonly)
                : TransactionLock.getLock(params.getTempLockName(), readonly);

        log.trace("get lock for project {}, lock is held by current thread: {}", project, lock.isHeldByCurrentThread());
        //re-entry is not encouraged (because it indicates complex handling logic, bad smell), let's abandon it first
        Preconditions.checkState(!lock.isHeldByCurrentThread());
        lock.lock();

        val unitOfWork = new UnitOfWorkContext(project);
        unitOfWork.setCurrentLock(lock);
        unitOfWork.setParams(params);
        threadLocals.set(unitOfWork);

        if (readonly || !params.isUseSandbox()) {
            unitOfWork.setLocalConfig(null);
            return unitOfWork;
        }

        //put a sandbox meta store on top of base meta store for isolation
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStore underlying = ResourceStore.getKylinMetaStore(config);
        KylinConfig configCopy = KylinConfig.createKylinConfig(config);
        //TODO check underlying rs is never changed since here
        ThreadViewResourceStore rs = new ThreadViewResourceStore((InMemResourceStore) underlying, configCopy);
        ResourceStore.setRS(configCopy, rs);
        unitOfWork.setLocalConfig(KylinConfig.setAndUnsetThreadLocalConfig(configCopy));

        log.trace("sandbox RS {} now takes place for main RS {}", rs, underlying);

        return unitOfWork;
    }

    private static <T> void checkEpoch(UnitOfWorkParams<T> params) throws Exception {
        val checker = params.getEpochChecker();
        if (checker != null && !params.isReadonly()) {
            checker.process();
        }
    }

    public static UnitOfWorkContext get() {
        val temp = threadLocals.get();
        Preconditions.checkNotNull(temp, "current thread is not accompanied by a UnitOfWork");
        temp.checkLockStatus();
        return temp;
    }

    static <T> void endTransaction(String traceId, UnitOfWorkParams<T> params) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val work = get();
        if (work.isReadonly() || !work.isUseSandbox()) {
            work.cleanResource();
            return;
        }
        val threadViewRS = (ThreadViewResourceStore) ResourceStore.getKylinMetaStore(config);
        List<RawResource> data = threadViewRS.getResources();
        val eventList = data.stream().map(x -> {
            if (x instanceof TombRawResource) {
                return new ResourceDeleteEvent(x.getResPath());
            } else {
                return new ResourceCreateOrUpdateEvent(x);
            }
        }).collect(Collectors.<Event> toList());

        //clean rs and config
        work.cleanResource();

        val originConfig = KylinConfig.getInstanceFromEnv();
        // publish events here
        val metadataStore = ResourceStore.getKylinMetaStore(originConfig).getMetadataStore();
        val writeInterceptor = params.getWriteInterceptor();
        val unitMessages = packageEvents(eventList, get().getProject(), traceId, writeInterceptor);
        long entitiesSize = unitMessages.getMessages().stream().filter(event -> event instanceof ResourceRelatedEvent)
                .count();
        log.debug("transaction {} updates {} metadata items", traceId, entitiesSize);
        checkEpoch(params);
        val unitName = params.getUnitName();
        metadataStore.batchUpdate(unitMessages, get().getParams().isSkipAuditLog(), unitName, params.getEpochId());
        if (entitiesSize != 0 && !params.isReadonly() && !params.isSkipAuditLog() && !config.isUTEnv()) {
            factory.postAsync(new AuditLogBroadcastEventNotifier());
        }
        try {
            // replayInTransaction in leader before release lock
            val replayer = MessageSynchronization.getInstance(originConfig);
            replayer.replayInTransaction(unitMessages);
        } catch (Exception e) {
            // in theory, this should not happen
            log.error("Unexpected error happened! Aborting right now.", e);
            Unsafe.systemExit(1);
        }
    }

    private static void handleError(Throwable throwable, UnitOfWorkParams<?> params, int retry, String traceId) {
        if (throwable instanceof KylinException && Objects.nonNull(((KylinException) throwable).getErrorCodeProducer())
                && ((KylinException) throwable).getErrorCodeProducer().getErrorCode()
                        .equals(ErrorCodeSystem.MAINTENANCE_MODE_WRITE_FAILED.getErrorCode())) {
            retry = params.getMaxRetry();
        }
        if (throwable instanceof QuitTxnRightNow) {
            retry = params.getMaxRetry();
        }

        if (retry >= params.getMaxRetry()) {
            params.getProcessor().onProcessError(throwable);
            throw new TransactionException(
                    "exhausted max retry times, transaction failed due to inconsistent state, traceId:" + traceId,
                    throwable);
        }

        if (retry == 1) {
            log.warn("transaction failed at first time, traceId:" + traceId, throwable);
        }
    }

    private static UnitMessages packageEvents(List<Event> events, String project, String uuid,
            Consumer<ResourceRelatedEvent> writeInterceptor) {
        for (Event e : events) {
            if (!(e instanceof ResourceRelatedEvent)) {
                continue;
            }
            val event = (ResourceRelatedEvent) e;
            if (!(event.getResPath().startsWith("/" + project) || event.getResPath().endsWith("/" + project + ".json")
                    || get().getParams().isAll())) {
                throw new IllegalStateException("some event are not in project " + project);
            }
            if (writeInterceptor != null) {
                writeInterceptor.accept(event);
            }
        }
        events.add(0, new StartUnit(uuid));
        events.add(new EndUnit(uuid));
        events.forEach(e -> e.setKey(get().getProject()));
        return new UnitMessages(events);
    }

    public static boolean isAlreadyInTransaction() {
        return threadLocals.get() != null;
    }

    public static boolean isReplaying() {
        return Objects.equals(true, replaying.get());

    }

    public static boolean isReadonly() {
        return UnitOfWork.get().isReadonly();
    }

    public interface Callback<T> {
        /**
         * Pre-process stage (before transaction)
         */
        default void preProcess() {
        }

        /**
         * Process stage (within transaction)
         */
        T process() throws Exception;

        /**
         * Handle error of process stage
         * @param throwable
         */
        default void onProcessError(Throwable throwable) {
        }
    }
}
