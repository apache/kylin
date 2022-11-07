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
package org.apache.kylin.rest.service.task;

import static org.apache.commons.lang3.time.DateUtils.MILLIS_PER_DAY;
import static org.apache.kylin.metadata.epoch.EpochManager.GLOBAL;
import static org.apache.kylin.metadata.favorite.AsyncTaskManager.ASYNC_ACCELERATION_TASK;
import static org.apache.kylin.metadata.favorite.AsyncTaskManager.getInstance;

import java.time.LocalTime;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.metadata.favorite.AsyncAccelerationTask;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.service.ProjectSmartSupporter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.kyligence.kap.guava20.shaded.common.annotations.VisibleForTesting;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Component("recommendationUpdateScheduler")
@Slf4j
public class RecommendationTopNUpdateScheduler {

    @Autowired(required = false)
    private ProjectSmartSupporter rawRecService;

    private ScheduledThreadPoolExecutor taskScheduler;

    private Map<String, Future> needUpdateProjects = Maps.newConcurrentMap();

    public RecommendationTopNUpdateScheduler() {
        taskScheduler = new ScheduledThreadPoolExecutor(10, new NamedThreadFactory("recommendation-update-topn"));
        taskScheduler.setKeepAliveTime(1, TimeUnit.MINUTES);
        taskScheduler.allowCoreThreadTimeOut(true);
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

        boolean happenException = false;
        try {
            if (!isFirstSchedule) {
                saveTaskTime(project);
            }
        } catch (Exception e) {
            happenException = true;
            log.warn("{} task cancel, due to exception ", project, e);
        }

        long nextMilliSeconds = happenException ? computeNextTaskTimeGap(System.currentTimeMillis(), project)
                : computeNextTaskTimeGap(project);
        needUpdateProjects.put(project,
                taskScheduler.schedule(() -> work(project), nextMilliSeconds, TimeUnit.MILLISECONDS));

        return !happenException;
    }

    private void work(String project) {
        if (!scheduleNextTask(project, false)) {
            log.debug("{} task can't run, skip this time", project);
            return;
        }
        MetricsGroup.hostTagCounterInc(MetricsName.METADATA_OPS_CRON, MetricsCategory.GLOBAL, GLOBAL);
        try (SetThreadName ignored = new SetThreadName("UpdateTopNRecommendationsWorker")) {
            log.info("Routine task to update {} cost and topN recommendations", project);
            rawRecService.updateCostsAndTopNCandidates(project);
            log.info("Updating {} cost and topN recommendations finished.", project);
        }

        MetricsGroup.hostTagCounterInc(MetricsName.METADATA_OPS_CRON_SUCCESS, MetricsCategory.GLOBAL, GLOBAL);
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
        AsyncAccelerationTask task = (AsyncAccelerationTask) getInstance(KylinConfig.getInstanceFromEnv(), project)
                .get(ASYNC_ACCELERATION_TASK);
        return task.getLastUpdateTonNTime() == 0 ? System.currentTimeMillis() : task.getLastUpdateTonNTime();
    }

    public void saveTaskTime(String project) {
        long currentTime = System.currentTimeMillis();
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            AsyncAccelerationTask asyncAcceleration = (AsyncAccelerationTask) getInstance(
                    KylinConfig.getInstanceFromEnv(), project).get(ASYNC_ACCELERATION_TASK);
            asyncAcceleration.setLastUpdateTonNTime(currentTime);
            getInstance(KylinConfig.getInstanceFromEnv(), project).save(asyncAcceleration);
            return null;
        }, project);
    }

    private long computeNextTaskTime(long lastTaskTime, String project) {
        KylinConfig config = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project)
                .getConfig();
        if (!config.getUsingUpdateFrequencyRule()) {
            return lastTaskTime + config.getUpdateTopNTimeGap();
        }

        long lastTaskDayStart = getDateInMillis(lastTaskTime);
        int days = Integer.parseInt(FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getValue(FavoriteRule.UPDATE_FREQUENCY));
        long taskStartInDay = LocalTime.parse(config.getUpdateTopNTime()).toSecondOfDay() * 1000L;
        long nextTaskTime = lastTaskDayStart + MILLIS_PER_DAY * days + taskStartInDay;
        return nextTaskTime;
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
