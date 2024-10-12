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

import static org.apache.kylin.common.persistence.lock.TransactionDeadLockHandler.THREAD_NAME_PREFIX;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.code.ErrorCodeSystem;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.TombRawResource;
import org.apache.kylin.common.persistence.TransparentResourceStore;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.EndUnit;
import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.event.ResourceDeleteEvent;
import org.apache.kylin.common.persistence.event.ResourceRelatedEvent;
import org.apache.kylin.common.persistence.event.StartUnit;
import org.apache.kylin.common.persistence.lock.DeadLockException;
import org.apache.kylin.common.persistence.lock.LockInterruptException;
import org.apache.kylin.common.persistence.lock.TransactionLock;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.springframework.transaction.TransactionStatus;

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

    private static EventBusFactory getFactory() {
        if (factory == null) {
            factory = EventBusFactory.getInstance();
        }
        return factory;
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
        try (SetThreadName ignore = new SetThreadName(THREAD_NAME_PREFIX)) {
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
            if (params.isRetryMoreTimeForDeadLockException()) {
                KylinConfig config = KylinConfig.getInstanceFromEnv();
                params.setRetryUntil(System.currentTimeMillis() + config.getMaxSecondsForDeadLockRetry() * 1000);
            }
            while (retry++ < params.getMaxRetry()) {

                val ret = doTransaction(params, retry, traceId);
                if (ret.getSecond()) {
                    return ret.getFirst();
                }
            }
            throw new IllegalStateException("Unexpected doInTransactionWithRetry end");
        }
    }

    private static <T> Pair<T, Boolean> doTransaction(UnitOfWorkParams<T> params, int retry, String traceId) {
        Pair<T, Boolean> result = Pair.newPair(null, false);
        UnitOfWorkContext context = null;
        try {
            T ret;

            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                if (retry != 1) {
                    log.debug("UnitOfWork {} in project {} is retrying for {}th time", traceId, params.getUnitName(),
                            retry);
                } else {
                    log.debug("UnitOfWork {} started on project {}", traceId, params.getUnitName());
                }
            }

            long startTime = System.currentTimeMillis();
            params.getProcessor().preProcess();
            context = UnitOfWork.startTransaction(params);
            long startTransactionTime = System.currentTimeMillis();
            val waitForLockTime = startTransactionTime - startTime;
            if (waitForLockTime > 3000) {
                try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                    log.warn("UnitOfWork {} takes too long time {}ms to start", traceId, waitForLockTime);
                }
            }

            ret = params.getProcessor().process();
            UnitOfWork.endTransaction(traceId, params);
            long duration = System.currentTimeMillis() - startTransactionTime;
            logIfLongTransaction(duration, traceId);

            result = Pair.newPair(ret, true);
        } catch (Throwable throwable) {
            handleError(throwable, params, retry, traceId);
        } finally {
            if (isAlreadyInTransaction()) {
                try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                    val unitOfWork = UnitOfWork.get();
                    unitOfWork.cleanResource();
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
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
            if (duration > 3000) {
                log.warn("UnitOfWork {} takes too long time {}ms to complete", traceId, duration);
                if (duration > 10000) {
                    log.warn("current stack: ", new Throwable());
                }
            } else {
                log.debug("UnitOfWork {} takes {}ms to complete", traceId, duration);
            }
        }
    }

    static <T> UnitOfWorkContext startTransaction(UnitOfWorkParams<T> params) throws Exception {
        val unitName = params.getUnitName();
        checkEpoch(params);

        val unitOfWork = new UnitOfWorkContext(unitName);
        unitOfWork.setParams(params);
        threadLocals.set(unitOfWork);

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStore underlying = ResourceStore.getKylinMetaStore(config);
        MetadataStore metadataStore = underlying.getMetadataStore();

        KylinConfig configCopy = KylinConfig.createKylinConfig(config);
        TransparentResourceStore rs = new TransparentResourceStore((InMemResourceStore) underlying, configCopy);
        ResourceStore.setRS(configCopy, rs);
        unitOfWork.setLocalConfig(KylinConfig.setAndUnsetThreadLocalConfig(configCopy));

        try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
            log.trace("Transparent RS {} now takes place for main RS {}", rs, underlying);
        }

        // start transaction via metadata store.
        TransactionStatus status = metadataStore.getTransaction();
        assert status != null;
        unitOfWork.setTransactionStatus(status);
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
        val transparentRS = (TransparentResourceStore) ResourceStore.getKylinMetaStore(config);
        List<RawResource> data = transparentRS.getResources();
        Set<String> copyForWriteResources = UnitOfWork.get().getCopyForWriteItems();
        val eventList = data.stream().map(x -> {
            String resPath = x.generateKeyWithType();
            if (x.getContent() != null && !copyForWriteResources.contains(resPath)) {
                throw new IllegalStateException(
                        "Transaction try to modify a resource without copyForWrite: " + x.getMetaKey());
            }
            if (x instanceof TombRawResource) {
                return new ResourceDeleteEvent(resPath);
            } else {
                return new ResourceCreateOrUpdateEvent(resPath, x);
            }
        }).collect(Collectors.<Event> toList());

        UnitMessages unitMessages = null;
        long entitiesSize = 0;
        KylinConfig originConfig = work.getOriginConfig();
        boolean isUT = config.isUTEnv();
        val metadataStore = ResourceStore.getKylinMetaStore(originConfig).getMetadataStore();
        try {
            // publish events here
            val writeInterceptor = params.getWriteInterceptor();
            unitMessages = packageEvents(eventList, traceId, writeInterceptor);
            entitiesSize = unitMessages.getMessages().stream().filter(event -> event instanceof ResourceRelatedEvent)
                    .count();
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                log.debug("transaction {} updates {} metadata items", traceId, entitiesSize);
            }
            if (!get().getParams().isSkipAuditLog()) {
                metadataStore.getAuditLogStore().save(unitMessages);
            }
            UnitOfWork.get().onUnitUpdated();
            transparentRS.getMetadataStore().commit(threadLocals.get().getTransactionStatus());
            threadLocals.get().setTransactionStatus(null);
        } finally {
            //clean rs and config
            work.cleanResource();
        }

        if (entitiesSize != 0 && !params.isReadonly() && !params.isSkipAuditLog() && !config.isUTEnv()) {
            getFactory().postAsync(new AuditLogBroadcastEventNotifier());
        }

        long startTime = System.currentTimeMillis();
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
            transparentRS.getAuditLogStore().catchupWithMaxTimeout();
            long endTime = System.currentTimeMillis();
            if (endTime - startTime > 1500) {
                log.warn("UnitOfWork {} takes too long time {}ms to catchup audit log", traceId, endTime - startTime);
            }
        }
    }

    public static void recordLocks(String resPath, TransactionLock lock, boolean readOnly) {
        Preconditions.checkState(lock.isHeldByCurrentThread());
        UnitOfWork.get().getCurrentLock().add(lock);
        if (readOnly) {
            UnitOfWork.get().getReadLockPath().add(resPath);
        } else {
            UnitOfWork.get().getCopyForWriteItems().add(resPath);
        }
    }

    private static void handleError(Throwable throwable, UnitOfWorkParams<?> params, int retry, String traceId) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        TransactionStatus status = threadLocals.get().getTransactionStatus();
        if (status != null) {
            ResourceStore.getKylinMetaStore(config).getMetadataStore().rollback(status);
            threadLocals.get().setTransactionStatus(null);
        }
        if (throwable instanceof KylinException && Objects.nonNull(((KylinException) throwable).getErrorCodeProducer())
                && ((KylinException) throwable).getErrorCodeProducer().getErrorCode()
                        .equals(ErrorCodeSystem.MAINTENANCE_MODE_WRITE_FAILED.getErrorCode())) {
            retry = params.getMaxRetry();
        }
        if (throwable instanceof DeadLockException) {
            log.debug("DeadLock found in this transaction, will retry");
            if (params.isRetryMoreTimeForDeadLockException() && System.currentTimeMillis() < params.getRetryUntil()) {
                params.setMaxRetry(params.getMaxRetry() + 1);
            }
        }
        if (throwable instanceof LockInterruptException) {
            // Just remove the interrupted flag.
            Thread.interrupted();
            log.debug("DeadLock is found by TransactionDeadLockHandler, will retry");
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
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                log.warn("transaction failed at first time, traceId:" + traceId, throwable);
            }
        }
    }

    private static UnitMessages packageEvents(List<Event> events, String uuid,
            Consumer<ResourceRelatedEvent> writeInterceptor) {
        for (Event e : events) {
            if (!(e instanceof ResourceRelatedEvent)) {
                continue;
            }
            val event = (ResourceRelatedEvent) e;
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

    public static void doAfterUpdate(UnitOfWorkContext.UnitTask task) {
        if (isAlreadyInTransaction()) {
            get().doAfterUpdate(task);
            return;
        }
        try {
            task.run();
        } catch (KylinException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Execute 'doAfterUpdate' failed.", e);
        }
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
