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

package org.apache.kylin.metadata.epoch;

import static org.apache.kylin.common.util.AddressUtil.MAINTAIN_MODE_MOCK_PORT;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.persistence.metadata.Epoch;
import org.apache.kylin.common.persistence.metadata.EpochStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.scheduler.EpochStartedNotifier;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.scheduler.ProjectControlledNotifier;
import org.apache.kylin.common.scheduler.ProjectEscapedNotifier;
import org.apache.kylin.common.scheduler.ProjectSerialEventBus;
import org.apache.kylin.common.scheduler.SourceUsageVerifyNotifier;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import org.apache.kylin.guava30.shaded.common.collect.Sets;
import lombok.Getter;
import lombok.Synchronized;
import lombok.val;

public class EpochManager {
    private static final Logger logger = LoggerFactory.getLogger(LogConstant.METADATA_CATEGORY);

    public static EpochManager getInstance() {
        return Singletons.getInstance(EpochManager.class, clz -> {
            try {
                return new EpochManager();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        });
    }

    public static final String GLOBAL = UnitOfWork.GLOBAL_UNIT;

    private static final String MAINTAIN_OWNER;

    private final EpochStore epochStore;

    private final KylinConfig config;
    private String identity;
    private final EventBusFactory eventBusFactory;
    private final ProjectSerialEventBus projectSerialEventBus;
    private final String serverMode;
    private final boolean epochCheckEnabled;
    private final long epochExpiredTime;
    private final int epochRenewTimeout;

    @Getter
    private final EpochUpdateManager epochUpdateManager;

    static {
        MAINTAIN_OWNER = AddressUtil.getMockPortAddress() + "|" + Long.MAX_VALUE;
    }

    public EpochManager() throws Exception {
        this.config = KylinConfig.readSystemKylinConfig();
        this.identity = EpochOrchestrator.getOwnerIdentity();
        this.eventBusFactory = EventBusFactory.getInstance();
        this.projectSerialEventBus = ProjectSerialEventBus.getInstance();
        this.epochStore = EpochStore.getEpochStore(config);
        this.serverMode = config.getServerMode();
        this.epochCheckEnabled = config.getEpochCheckerEnabled();
        this.epochExpiredTime = config.getEpochExpireTimeSecond();
        this.epochRenewTimeout = getEpochRenewTimeout();
        this.epochUpdateManager = new EpochUpdateManager();
    }

    private int getEpochRenewTimeout() {
        double timeoutRate = config.getEpochRenewTimeoutRate() <= 0 ? 1 : config.getEpochRenewTimeoutRate();
        return (int) (epochExpiredTime * timeoutRate);
    }

    public class EpochUpdateManager {
        private final AtomicBoolean updateStarted;

        private final ExecutorService renewExecutor;

        private final Object renewLock = new Object();
        private final Object updateLock = new Object();

        private final int renewWorkerSize = config.getRenewEpochWorkerPoolSize();
        private final int renewBatchSize = config.getRenewEpochBatchSize();

        private final Set<String> lastRenewEpochSet = Sets.newConcurrentHashSet();

        EpochUpdateManager() {
            Preconditions.checkArgument(renewWorkerSize > 0, "illegal renew worker size %s", renewWorkerSize);
            Preconditions.checkArgument(renewBatchSize > 0, "illegal renew renew batch size %s", renewBatchSize);
            updateStarted = new AtomicBoolean(false);
            renewExecutor = Executors.newFixedThreadPool(renewWorkerSize, new NamedThreadFactory("renew-epoch"));
        }

        private List<Epoch> queryEpochAlreadyOwned() {
            return epochStore.list().stream().filter(EpochManager.this::checkEpochOwnerOnly)
                    .collect(Collectors.toList());
        }

        private Pair<HashSet<Epoch>, List<String>> checkAndGetProjectEpoch(boolean removeOutdatedEpoch) {
            if (checkInMaintenanceMode()) {
                return null;
            }
            val oriEpochs = Sets.newHashSet(queryEpochAlreadyOwned());
            val projects = listProjectWithPermission();

            //sometimes it may update epoch that is outdated because metadata is outdated
            if (removeOutdatedEpoch) {
                removeOutdatedOwnedEpoch(oriEpochs, new HashSet<>(projects));
            }

            return new Pair<>(oriEpochs, projects);
        }

        private void removeOutdatedOwnedEpoch(final Set<Epoch> alreadyOwnedSets, final Set<String> projectSets) {
            if (CollectionUtils.isEmpty(alreadyOwnedSets)) {
                return;
            }

            val epochTargetList = alreadyOwnedSets.stream().map(Epoch::getEpochTarget).collect(Collectors.toSet());

            val outdatedProjects = new HashSet<>(Sets.difference(epochTargetList, projectSets));

            if (CollectionUtils.isNotEmpty(outdatedProjects)) {
                outdatedProjects.forEach(EpochManager.this::deleteEpoch);
                notifierEscapedProject(outdatedProjects);
                try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                    logger.warn("remove outdated epoch list :{}", String.join(",", outdatedProjects));
                }
            }
        }

        @Synchronized("renewLock")
        public void tryRenewOwnedEpochs() {
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                logger.debug("Start renew owned epoch.........");
            }
            long startTime = System.currentTimeMillis();

            //1.check and get project
            val epochSetProjectListPair = checkAndGetProjectEpoch(true);
            if (Objects.isNull(epochSetProjectListPair)) {
                return;
            }
            val oriOwnedEpochSet = epochSetProjectListPair.getFirst();
            val projects = epochSetProjectListPair.getSecond();

            //2.only retain the project that is legal
            if (CollectionUtils.isNotEmpty(oriOwnedEpochSet) && CollectionUtils.isNotEmpty(projects)) {
                oriOwnedEpochSet.removeIf(epoch -> !projects.contains(epoch.getEpochTarget()));
            }

            if (CollectionUtils.isEmpty(oriOwnedEpochSet)) {
                try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                    logger.info("current node own none project, end renew...");
                }
                return;
            }

            //3.concurrent to update
            final Set<String> afterRenewEpochSets = innerRenewEpochWithRetry(
                    Collections.unmodifiableSet(oriOwnedEpochSet));

            notifierAfterUpdatedEpoch("renew", lastRenewEpochSet, afterRenewEpochSets);
            lastRenewEpochSet.clear();
            lastRenewEpochSet.addAll(afterRenewEpochSets);
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                logger.debug("End renew owned epoch,cost:{}.........",
                        TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime));
            }
        }

        private Set<String> innerRenewEpochWithRetry(Set<Epoch> oriEpochs) {
            val successRenewEpochSets = Sets.<String> newHashSet();
            int retry = 1;
            Set<Epoch> needRenewSets = oriEpochs;
            while (retry >= 0) {
                retry--;

                // sort by renew time
                val renewEpochList = new ArrayList<>(needRenewSets);
                renewEpochList.sort(Comparator.comparingLong(Epoch::getLastEpochRenewTime));

                Set<String> finishedSets = innerRenewEpoch(renewEpochList);
                successRenewEpochSets.addAll(finishedSets);

                if (successRenewEpochSets.size() == needRenewSets.size()) {
                    break;
                }

                // need retry
                // check epoch owner again
                val curOwnedEpochTargetSet = queryEpochAlreadyOwned().stream().map(Epoch::getEpochTarget)
                        .collect(Collectors.toSet());
                needRenewSets = oriEpochs.stream().filter(e -> !finishedSets.contains(e.getEpochTarget())
                        && curOwnedEpochTargetSet.contains(e.getEpochTarget())).collect(Collectors.toSet());
            }

            return successRenewEpochSets;
        }

        private Set<String> innerRenewEpoch(List<Epoch> oriEpochs) {
            val newRenewEpochSets = Sets.<String> newConcurrentHashSet();

            val totalTask = Lists.partition(oriEpochs, renewBatchSize);

            CountDownLatch countDownLatch = new CountDownLatch(totalTask.size());

            totalTask.forEach(taskEpochList -> {
                val epochTargetList = taskEpochList.stream().map(Epoch::getEpochTarget).collect(Collectors.toList());
                renewExecutor.submit(() -> {
                    try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                        if (CollectionUtils.isNotEmpty(epochTargetList)) {
                            batchRenewEpoch(taskEpochList);
                            newRenewEpochSets.addAll(epochTargetList);
                        }
                    } catch (Exception e) {
                        logger.error("renew task error,", e);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            });

            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                if (!countDownLatch.await(epochRenewTimeout, TimeUnit.SECONDS)) {
                    logger.error("renew not finished,{}/{}...", newRenewEpochSets.size(), oriEpochs.size());
                }
            } catch (InterruptedException e) {
                logger.error("renew timeout...", e);
            }
            return newRenewEpochSets;
        }

        @Synchronized("updateLock")
        public void tryUpdateAllEpochs() {
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                logger.debug("Start update Epochs.........");
            }
            long startTime = System.currentTimeMillis();

            //1.check and get project
            val epochSetProjectListPair = checkAndGetProjectEpoch(false);
            if (Objects.isNull(epochSetProjectListPair)) {
                return;
            }
            val oriEpochs = epochSetProjectListPair.getFirst().stream().map(Epoch::getEpochTarget)
                    .collect(Collectors.toSet());
            val projects = epochSetProjectListPair.getSecond();

            //2.if update owned epoch only, remove all already owned project
            if (CollectionUtils.isNotEmpty(oriEpochs)) {
                projects.removeAll(oriEpochs);
            }

            if (CollectionUtils.isEmpty(projects)) {
                try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                    logger.debug("don't have more new project, end update...");
                }
                return;
            }

            //3.update one by one
            Set<String> updatedMewEpochs = tryUpdateEpochByProjects(projects);

            notifierAfterUpdatedEpoch("update", Collections.emptySet(), updatedMewEpochs);

            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                logger.debug("End update Epochs,cost:{}:.........",
                        TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime));
            }
        }

        private Set<String> tryUpdateEpochByProjects(final List<String> projects) {
            Set<String> newEpochs = new HashSet<>();

            if (CollectionUtils.isEmpty(projects)) {
                return newEpochs;
            }

            //random order
            Collections.shuffle(projects);

            projects.forEach(project -> {
                EpochUpdateLockManager.executeEpochWithLock(project, () -> {
                    if (updateEpochByProject(project)) {
                        newEpochs.add(project);
                    }
                    return null;
                });
            });

            return newEpochs;
        }

        private boolean updateEpochByProject(String project) {
            return EpochUpdateLockManager.executeEpochWithLock(project, () -> {
                boolean success = tryUpdateEpoch(project, false);
                return success && checkEpochOwner(project);
            });
        }

        private void notifierEscapedProject(final Collection<String> escapedProjects) {
            if (CollectionUtils.isEmpty(escapedProjects)) {
                return;
            }

            for (String project : escapedProjects) {
                projectSerialEventBus.postAsync(new ProjectEscapedNotifier(project));
            }

            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                logger.warn("notifier escaped project:{}", String.join(",", escapedProjects));
            }
        }

        private void notifierAfterUpdatedEpoch(String updateTypeName, Set<String> oriEpochs, Set<String> newEpochs) {
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                logger.debug("after {} new epoch size:{}, Project {} owned by {}", updateTypeName, newEpochs.size(),
                    String.join(",", newEpochs), identity);
            }

            if (CollectionUtils.isNotEmpty(newEpochs)) {
                Collection<String> newControlledProjects = new HashSet<>(Sets.difference(newEpochs, oriEpochs));
                if (CollectionUtils.isNotEmpty(newControlledProjects)) {
                    try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                        logger.debug("after {} controlled projects: {}", updateTypeName,
                            String.join(",", newControlledProjects));
                    }
                    newControlledProjects.forEach(p -> projectSerialEventBus.postAsync(new ProjectControlledNotifier(p)));
                }
            }

            if (CollectionUtils.isNotEmpty(oriEpochs)) {
                Collection<String> escapedProjects = new HashSet<>(Sets.difference(oriEpochs, newEpochs));
                if (CollectionUtils.isNotEmpty(escapedProjects)) {
                    try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                        logger.debug("after {} escaped projects: {}", updateTypeName, String.join(",", escapedProjects));
                    }
                    notifierEscapedProject(escapedProjects);
                }
            }

            if (updateStarted.compareAndSet(false, true)) {
                eventBusFactory.postAsync(new EpochStartedNotifier());
                eventBusFactory.postAsync(new SourceUsageVerifyNotifier());
            }
        }
    }

    private List<String> listProjectWithPermission() {
        List<String> projects = epochCheckEnabled ? getProjectsToMarkOwner()
                : NProjectManager.getInstance(config).listAllProjects().stream().map(ProjectInstance::getName)
                        .collect(Collectors.toList());
        projects.add(GLOBAL);
        return projects;
    }

    //for test
    public Epoch getGlobalEpoch() {
        return epochStore.getGlobalEpoch();
    }

    public boolean checkExpectedIsMaintenance(boolean expectedIsMaintenance) {
        return isMaintenanceMode() == expectedIsMaintenance;
    }

    private boolean switchMaintenanceMode(boolean expectedIsMaintenance, Consumer<Epoch> updateConsumer) {
        return updateEpochBatchTransaction(expectedIsMaintenance, () -> epochStore.list(), updateConsumer);
    }

    public boolean updateEpochBatchTransaction(boolean expectedIsMaintenance,
            @Nonnull Supplier<List<Epoch>> epochSupplier, @Nullable Consumer<Epoch> updateConsumer) {
        return epochStore.executeWithTransaction(() -> {
            if (!checkExpectedIsMaintenance(expectedIsMaintenance)) {
                return false;
            }

            val epochs = epochSupplier.get();

            if (Objects.nonNull(updateConsumer)) {
                epochs.forEach(updateConsumer);
            }

            epochStore.updateBatch(epochs);
            return true;
        });
    }

    public Boolean setMaintenanceMode(String reason) {

        return switchMaintenanceMode(false, epoch -> {
            epoch.setCurrentEpochOwner(MAINTAIN_OWNER);
            epoch.setLastEpochRenewTime(Long.MAX_VALUE);
            epoch.setMaintenanceModeReason(reason);
        });

    }

    public Boolean unsetMaintenanceMode(String reason) {

        return switchMaintenanceMode(true, epoch -> {
            epoch.setCurrentEpochOwner("");
            epoch.setLastEpochRenewTime(-1L);
            epoch.setMaintenanceModeReason(reason);
        });
    }

    private List<String> getProjectsToMarkOwner() {
        return NProjectManager.getInstance(config).listAllProjects().stream()
                .filter(p -> currentInstanceHasPermissionToOwn(p.getName(), false)).map(ProjectInstance::getName)
                .collect(Collectors.toList());
    }

    public void batchRenewEpoch(Collection<Epoch> epochList) {
        List<Epoch> needUpdateEpochList = Lists.newArrayList();
        epochList.forEach(epoch -> {
            Pair<Epoch, Epoch> pair = oldEpoch2NewEpoch(epoch, epoch.getEpochTarget(), true, null);
            if (Objects.nonNull(pair)) {
                needUpdateEpochList.add(pair.getSecond());
            }
        });

        epochStore.executeWithTransaction(() -> {
            if (CollectionUtils.isNotEmpty(needUpdateEpochList)) {
                epochStore.updateBatch(needUpdateEpochList);
            }
            return null;
        }, epochRenewTimeout);
    }

    /**
     * the method only update epoch'meta,
     * will not post ProjectControlledNotifier event
     * so it can be safely used by tool
     *
     * @param projects              projects need to be updated or inserted
     * @param skipCheckMaintMode    if true, should not check maintenance mode status
     * @param maintenanceModeReason
     * @param expectedIsMaintenance the expected maintenance mode
     * @return
     */
    public boolean tryForceInsertOrUpdateEpochBatchTransaction(Collection<String> projects, boolean skipCheckMaintMode,
            String maintenanceModeReason, boolean expectedIsMaintenance) {
        if ((!skipCheckMaintMode && !checkExpectedIsMaintenance(expectedIsMaintenance))
                || CollectionUtils.isEmpty(projects)) {
            return false;
        }
        val epochList = epochStore.list();

        //epochs need to be updated
        val needUpdateProjectSet = epochList.stream().map(Epoch::getEpochTarget).collect(Collectors.toSet());
        List<Epoch> needUpdateEpochList = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(needUpdateProjectSet)) {
            epochList.forEach(epoch -> {
                Pair<Epoch, Epoch> pair = oldEpoch2NewEpoch(epoch, epoch.getEpochTarget(), true, maintenanceModeReason);
                if (Objects.nonNull(pair)) {
                    needUpdateEpochList.add(pair.getSecond());
                }
            });
        }

        //epoch need to be inserted
        val needInsertProjectSet = Sets.difference(new HashSet<>(projects), needUpdateProjectSet);
        List<Epoch> needInsertEpochList = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(needInsertProjectSet)) {
            needInsertProjectSet.forEach(project -> {
                Pair<Epoch, Epoch> pair = oldEpoch2NewEpoch(null, project, true, maintenanceModeReason);
                if (Objects.nonNull(pair)) {
                    needInsertEpochList.add(pair.getSecond());
                }
            });
        }

        if (CollectionUtils.isNotEmpty(needUpdateEpochList) || CollectionUtils.isNotEmpty(needInsertEpochList)) {
            epochStore.executeWithTransaction(() -> {
                if (CollectionUtils.isNotEmpty(needUpdateEpochList)) {
                    epochStore.updateBatch(needUpdateEpochList);
                }
                if (CollectionUtils.isNotEmpty(needInsertEpochList)) {
                    epochStore.insertBatch(needInsertEpochList);
                }
                return true;
            });
        }
        return true;
    }

    @Nullable
    private Pair<Epoch, Epoch> oldEpoch2NewEpoch(@Nullable Epoch oldEpoch, @Nonnull String epochTarget, boolean force,
            String maintenanceModeReason) {
        Epoch finalEpoch = getNewEpoch(oldEpoch, force, epochTarget);
        if (finalEpoch == null) {
            return null;
        }

        finalEpoch.setMaintenanceModeReason(maintenanceModeReason);
        return new Pair<>(oldEpoch, finalEpoch);
    }

    public boolean tryUpdateEpoch(String epochTarget, boolean force) {
        if (!force && checkInMaintenanceMode()) {
            return false;
        }
        return EpochUpdateLockManager.executeEpochWithLock(epochTarget, () -> {
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                Epoch epoch = epochStore.getEpoch(epochTarget);
                Pair<Epoch, Epoch> oldNewEpochPair = oldEpoch2NewEpoch(epoch, epochTarget, force, null);

                //current epoch already has owner and not to force
                if (Objects.isNull(oldNewEpochPair)) {
                    return false;
                }

                if (!checkEpochValid(epochTarget)) {
                    logger.warn("epoch target {} is invalid, skip to update it ", epochTarget);
                    return false;
                }

                insertOrUpdateEpoch(oldNewEpochPair.getSecond());

                if (Objects.nonNull(oldNewEpochPair.getFirst())
                        && !Objects.equals(oldNewEpochPair.getFirst().getCurrentEpochOwner(),
                                oldNewEpochPair.getSecond().getCurrentEpochOwner())) {
                    logger.debug("Epoch {} changed from {} to {}", epochTarget,
                            oldNewEpochPair.getFirst().getCurrentEpochOwner(),
                            oldNewEpochPair.getSecond().getCurrentEpochOwner());
                }

                return true;
            } catch (Exception e) {
                logger.error("Update " + epochTarget + " epoch failed.", e);
                return false;
            }
        });
    }

    /**
     * if epoch's target is not in meta data,insert new one,
     * otherwise update it
     *
     * @param epoch
     */
    private void insertOrUpdateEpoch(Epoch epoch) {
        if (Objects.isNull(epoch)) {
            return;
        }
        epochStore.executeWithTransaction(() -> {

            if (Objects.isNull(getEpoch(epoch.getEpochTarget()))) {
                epochStore.insert(epoch);
            } else {
                epochStore.update(epoch);
            }

            return null;
        });

    }

    private Epoch getNewEpoch(@Nullable Epoch epoch, boolean force, @Nonnull String epochTarget) {
        if (!epochCheckEnabled || (isMaintenanceIdentity(identity) && epoch == null)) {
            Epoch newEpoch = new Epoch(1L, epochTarget, identity, Long.MAX_VALUE, serverMode, null, 0L);
            newEpoch.setMvcc(epoch == null ? 0 : epoch.getMvcc());
            return newEpoch;
        }

        if (isMaintenanceIdentity(identity)) {
            return new Epoch(epoch.getEpochId() + 1, epochTarget, identity, Long.MAX_VALUE, serverMode, null,
                    epoch.getMvcc());
        }

        if (!currentInstanceHasPermissionToOwn(epochTarget, force)) {
            return null;
        }
        if (epoch == null) {
            epoch = new Epoch(1L, epochTarget, identity, System.currentTimeMillis(), serverMode, null, 0L);
        } else {
            if (!checkEpochOwnerOnly(epoch)) {
                if (isEpochLegal(epoch) && !force) {
                    return null;
                }
                epoch.setEpochId(epoch.getEpochId() + 1);
            }
            epoch.setServerMode(serverMode);
            epoch.setLastEpochRenewTime(System.currentTimeMillis());
            epoch.setCurrentEpochOwner(identity);
        }
        return epoch;
    }

    public synchronized void updateAllEpochs() {
        epochUpdateManager.tryRenewOwnedEpochs();
        epochUpdateManager.tryUpdateAllEpochs();
    }

    /**
     * 1.get epoch by epochTarget
     * 2.check epoch is legal
     * 3.check epoch owner
     *
     * @param epochTarget
     * @return
     */
    public boolean checkEpochOwner(@Nonnull String epochTarget) {
        Epoch epoch = getEpochOwnerEpoch(epochTarget);

        return Objects.nonNull(epoch) && checkEpochOwnerOnly(epoch);
    }

    /**
     * only check epoch owner
     * don't check legal or not
     *
     * @param epoch
     * @return
     */
    public boolean checkEpochOwnerOnly(@Nonnull Epoch epoch) {
        Preconditions.checkNotNull(epoch, "epoch is null");

        return epoch.getCurrentEpochOwner().equals(identity);
    }

    public boolean checkEpochValid(@Nonnull String epochTarget) {
        return listProjectWithPermission().contains(epochTarget);
    }

    public void updateEpochWithNotifier(String epochTarget, boolean force) {
        EpochUpdateLockManager.executeEpochWithLock(epochTarget, () -> {
            if (tryUpdateEpoch(epochTarget, force)) {
                projectSerialEventBus.postAsync(new ProjectControlledNotifier(epochTarget));
            }
            return null;
        });
    }

    private boolean currentInstanceHasPermissionToOwn(String epochTarget, boolean force) {
        // if force, no need to check resource group, eg: switch maintenance mode.
        if (force) {
            return true;
        }
        return currentInstanceHasPermissionToOwn(epochTarget, AddressUtil.getLocalInstance());
    }

    private boolean currentInstanceHasPermissionToOwn(String epochTarget, String epochServer) {
        if (isMaintenanceMode()) {
            return true;
        }
        ResourceGroupManager rgManager = ResourceGroupManager.getInstance(config);
        return rgManager.instanceHasPermissionToOwnEpochTarget(epochTarget, epochServer);
    }

    private boolean isEpochLegal(Epoch epoch) {
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
            if (epoch == null) {
                logger.debug("Get null epoch");
                return false;
            } else if (StringUtils.isEmpty(epoch.getCurrentEpochOwner())) {
                logger.debug("Epoch {}'s owner is empty", epoch);
                return false;
            } else if (System.currentTimeMillis() - epoch.getLastEpochRenewTime() > epochExpiredTime * 1000) {
                logger.warn("Epoch {}'s last renew time is expired. Current time is {}, expiredTime is {}", epoch,
                        System.currentTimeMillis(), epochExpiredTime);
                return false;
            }

            String epochServer = getHostAndPort(epoch.getCurrentEpochOwner());
            if (!currentInstanceHasPermissionToOwn(epoch.getEpochTarget(), epochServer)) {
                logger.debug("Epoch {}'s owner is not in build request type resource group.", epoch);
                return false;
            }
        }
        return true;
    }

    public String getEpochOwner(String epochTarget) {
        val ownerEpoch = getEpochOwnerEpoch(epochTarget);

        if (Objects.isNull(ownerEpoch)) {
            return null;
        }

        return getHostAndPort(ownerEpoch.getCurrentEpochOwner());

    }

    private Epoch getEpochOwnerEpoch(String epochTarget) {
        checkEpochTarget(epochTarget);

        String epochTargetTemp = epochTarget;

        //get origin project name
        if (!isGlobalProject(epochTargetTemp)) {
            val targetProjectInstance = NProjectManager.getInstance(config).getProject(epochTargetTemp);
            if (Objects.isNull(targetProjectInstance)) {
                try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                    logger.warn("get epoch failed, because the project:{} dose not exist", epochTargetTemp);
                }
                return null;
            }

            epochTargetTemp = targetProjectInstance.getName();
        }

        Epoch epoch = epochStore.getEpoch(epochTargetTemp);

        return isEpochLegal(epoch) ? epoch : null;

    }

    private String getHostAndPort(String owner) {
        return owner.split("\\|")[0];
    }

    //ensure only one epochTarget thread running
    public boolean checkEpochId(long epochId, String epochTarget) {
        return getEpochId(epochTarget) == epochId;
    }

    public long getEpochId(String epochTarget) {
        checkEpochTarget(epochTarget);
        Epoch epoch = epochStore.getEpoch(epochTarget);
        if (epoch == null) {
            throw new IllegalStateException(
                    String.format(Locale.ROOT, "Epoch of project %s does not exist", epochTarget));
        }
        return epoch.getEpochId();
    }

    private void checkEpochTarget(String epochTarget) {
        if (StringUtils.isEmpty(epochTarget)) {
            throw new IllegalStateException("Project should not be empty");
        }
    }

    public Epoch getEpoch(String epochTarget) {
        return epochStore.getEpoch(epochTarget);
    }

    public void setIdentity(String newIdentity) {
        this.identity = newIdentity;
    }

    public void deleteEpoch(String epochTarget) {
        EpochUpdateLockManager.executeEpochWithLock(epochTarget, () -> {
            epochStore.delete(epochTarget);
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                logger.debug("delete epoch:{}", epochTarget);
            }
            return null;
        });
    }

    public Pair<Boolean, String> getMaintenanceModeDetail() {
        return getMaintenanceModeDetail(GLOBAL);
    }

    public Pair<Boolean, String> getMaintenanceModeDetail(String epochTarget) {
        Epoch epoch = epochStore.getEpoch(epochTarget);
        if (epoch != null && isMaintenanceIdentity(epoch.getCurrentEpochOwner())) {
            return Pair.newPair(true, epoch.getMaintenanceModeReason());
        }

        return Pair.newPair(false, null);
    }

    public static boolean isMaintenanceIdentity(String identity) {
        return StringUtils.contains(identity, ":" + MAINTAIN_MODE_MOCK_PORT);
    }

    public boolean isMaintenanceMode() {
        return getMaintenanceModeDetail().getFirst();
    }

    private boolean checkInMaintenanceMode() {
        if (isMaintenanceMode()) {
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                logger.debug("System is currently undergoing maintenance. Abort updating Epochs");
            }
            return true;
        }
        return false;
    }

    private boolean isGlobalProject(@Nullable String project) {
        return StringUtils.equals(GLOBAL, project);
    }

    // when shutdown or meta data is inconsistent
    public void releaseOwnedEpochs() {
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
            logger.info("Release owned epochs");
        }
        epochStore.executeWithTransaction(() -> {
            val epochs = epochStore.list().stream().filter(this::checkEpochOwnerOnly).collect(Collectors.toList());
            epochs.forEach(epoch -> {
                epoch.setCurrentEpochOwner("");
                epoch.setLastEpochRenewTime(-1L);
            });

            epochStore.updateBatch(epochs);
            return null;
        });
    }

    public List<Epoch> getOwnedEpochs() {
        return epochStore.list().stream().filter(this::checkEpochOwnerOnly).collect(Collectors.toList());
    }

}
