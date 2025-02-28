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

package org.apache.kylin.rest.service;

import static org.apache.commons.lang3.time.DateUtils.MILLIS_PER_DAY;
import static org.apache.kylin.common.persistence.ResourceStore.GLOBAL_PROJECT;
import static org.apache.kylin.metadata.favorite.AsyncTaskManager.ASYNC_ACCELERATION_TASK;
import static org.apache.kylin.metadata.favorite.AsyncTaskManager.getInstance;

import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.favorite.AsyncAccelerationTask;
import org.apache.kylin.metadata.favorite.AsyncTaskManager;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Component("topRecsUpdateScheduler")
@Slf4j
public class TopRecsUpdateScheduler {

    @Autowired(required = false)
    private ProjectSmartSupporter rawRecService;

    private final ScheduledThreadPoolExecutor taskScheduler;

    private final Map<String, Future> needUpdateProjects = Maps.newConcurrentMap();

    public TopRecsUpdateScheduler() {
        taskScheduler = new ScheduledThreadPoolExecutor(10, new NamedThreadFactory("recommendation-update-topn"));
        taskScheduler.setKeepAliveTime(1, TimeUnit.MINUTES);
        taskScheduler.allowCoreThreadTimeOut(true);
    }

    public void checkProject() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        taskScheduler.schedule(this::checkProject, 60, TimeUnit.MINUTES);
        List<String> allProjects = NProjectManager.getInstance(config).listAllProjects().stream()
                .map(ProjectInstance::getName).collect(Collectors.toList());
        log.info("Start to check projects for RecommendationTopNUpdateScheduler.");
        for (String project : allProjects) {
            if (!needUpdateProjects.containsKey(project)) {
                addProject(project);
                log.info("Add project {} to needUpdateProjects.", project);
            }
        }
        for (String project : needUpdateProjects.keySet()) {
            if (!allProjects.contains(project)) {
                removeProject(project);
                log.info("Remove project {} from needUpdateProjects.", project);
            }
        }
    }

    public synchronized void reScheduleProject(String project) {
        removeProject(project);
        addProject(project);
    }

    public synchronized void addProject(String project) {
        if (!needUpdateProjects.containsKey(project)) {
            scheduleNextTask(project, true);
        }
    }

    public synchronized void removeProject(String project) {
        Future task = needUpdateProjects.get(project);
        if (task != null) {
            log.debug("cancel {} future task", project);
            task.cancel(false);
        }
        needUpdateProjects.remove(project);
    }

    private synchronized boolean scheduleNextTask(String project, boolean isFirstSchedule) {
        if (!isFirstSchedule && !needUpdateProjects.containsKey(project)) {
            return false;
        }

        boolean needToSkip = false;
        try {
            if (!isFirstSchedule) {
                needToSkip = !saveTaskTime(project);
            }
        } catch (Exception e) {
            needToSkip = true;
            log.warn("{} task cancel, due to exception ", project, e);
        }

        long nextMilliSeconds = needToSkip ? computeNextTaskTimeGap(System.currentTimeMillis(), project)
                : computeNextTaskTimeGap(project);
        needUpdateProjects.put(project,
                taskScheduler.schedule(() -> work(project), nextMilliSeconds, TimeUnit.MILLISECONDS));

        return !needToSkip;
    }

    private void work(String project) {
        if (!scheduleNextTask(project, false)) {
            log.debug("{} task can't run, skip this time", project);
            return;
        }
        MetricsGroup.hostTagCounterInc(MetricsName.METADATA_OPS_CRON, MetricsCategory.GLOBAL, GLOBAL_PROJECT);
        try (SetThreadName ignored = new SetThreadName("UpdateTopNRecommendationsWorker")) {
            log.info("Routine task to update {} cost and topN recommendations", project);
            rawRecService.updateCostsAndTopNCandidates(project);
            log.info("Updating {} cost and topN recommendations finished.", project);
        }

        MetricsGroup.hostTagCounterInc(MetricsName.METADATA_OPS_CRON_SUCCESS, MetricsCategory.GLOBAL, GLOBAL_PROJECT);
    }

    private long computeNextTaskTimeGap(long lastTaskTime, String project) {
        long nextTaskTime = computeNextTaskTime(lastTaskTime, project);
        log.debug("project {} next task time is {}", project, nextTaskTime);
        return nextTaskTime - System.currentTimeMillis();
    }

    @VisibleForTesting
    protected long computeNextTaskTimeGap(String project) {
        long lastTaskTime = getLastTaskTime(project);
        return computeNextTaskTimeGap(lastTaskTime, project);
    }

    private long getLastTaskTime(String project) {
        AsyncAccelerationTask task = (AsyncAccelerationTask) getInstance(project).get(ASYNC_ACCELERATION_TASK);
        return task.getLastUpdateTonNTime() == 0 ? System.currentTimeMillis() : task.getLastUpdateTonNTime();
    }

    protected boolean saveTaskTime(String project) {
        long currentTime = System.currentTimeMillis();
        AsyncTaskManager manager = getInstance(project);
        return JdbcUtil.withTxAndRetry(manager.getTransactionManager(), () -> {
            AsyncAccelerationTask asyncAcceleration = (AsyncAccelerationTask) manager.get(ASYNC_ACCELERATION_TASK);
            AsyncAccelerationTask copied = manager.copyForWrite(asyncAcceleration);
            long lastUpdateTime = copied.getLastUpdateTonNTime();
            if (computeNextTaskTime(lastUpdateTime, project) > currentTime) {
                return false;
            }
            copied.setLastUpdateTonNTime(currentTime);
            manager.save(copied);
            return true;
        });
    }

    private long computeNextTaskTime(long lastTaskTime, String project) {
        KylinConfig config = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project)
                .getConfig();
        if (!config.getUsingUpdateFrequencyRule()) {
            return lastTaskTime + config.getUpdateTopNTimeGap();
        }

        long lastTaskDayStart = getDateInMillis(lastTaskTime);
        int days = Integer.parseInt(FavoriteRuleManager.getInstance(project).getValue(FavoriteRule.UPDATE_FREQUENCY));
        long taskStartInDay = LocalTime.parse(config.getUpdateTopNTime()).toSecondOfDay() * 1000L;
        return lastTaskDayStart + MILLIS_PER_DAY * days + taskStartInDay;
    }

    private long getDateInMillis(final long queryTime) {
        return TimeUtil.getDayStart(queryTime);
    }

    public int getTaskCount() {
        return needUpdateProjects.size();
    }

    @SneakyThrows
    public void close() {
        ExecutorServiceUtil.forceShutdown(taskScheduler);
    }
}
