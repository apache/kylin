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

import static org.apache.kylin.metadata.favorite.QueryHistoryIdOffset.OffsetType.ACCELERATE;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.favorite.AccelerateRuleUtil;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffset;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffsetManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.rest.util.SpringContext;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryHistoryAccelerateScheduler {

    private ScheduledExecutorService taskScheduler;
    @VisibleForTesting
    RDBMSQueryHistoryDAO queryHistoryDAO;
    AccelerateRuleUtil accelerateRuleUtil;
    private QuerySmartSupporter querySmartSupporter;
    private IUserGroupService userGroupService;

    private final QueryHistoryAccelerateRunner queryHistoryAccelerateRunner;

    private static volatile QueryHistoryAccelerateScheduler INSTANCE = null;

    public QueryHistoryAccelerateScheduler() {
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        accelerateRuleUtil = new AccelerateRuleUtil();
        if (userGroupService == null && SpringContext.getApplicationContext() != null) {
            userGroupService = (IUserGroupService) SpringContext.getApplicationContext().getBean("userGroupService");
        }
        queryHistoryAccelerateRunner = new QueryHistoryAccelerateRunner(false);
        if (querySmartSupporter == null && SpringContext.getApplicationContext() != null) {
            querySmartSupporter = SpringContext.getBean(QuerySmartSupporter.class);
        }
        log.debug("New QueryHistoryAccelerateScheduler created.");
    }

    public static QueryHistoryAccelerateScheduler getInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (QueryHistoryAccelerateScheduler.class) {
            if (INSTANCE == null) {
                INSTANCE = new QueryHistoryAccelerateScheduler();
                INSTANCE.init();
            }
            return INSTANCE;
        }
    }

    public void init() {
        taskScheduler = Executors.newScheduledThreadPool(1, new NamedThreadFactory("QueryHistoryWorker"));
        taskScheduler.scheduleWithFixedDelay(queryHistoryAccelerateRunner, 0,
                KylinConfig.getInstanceFromEnv().getQueryHistoryAccelerateInterval(), TimeUnit.MINUTES);

        log.info("Query history task scheduler is started.");
    }

    public void scheduleImmediately(QueryHistoryTask runner) {
        runner.run();
    }

    public static void shutdown() {
        log.info("Shutting down QueryHistoryAccelerateScheduler ....");
        if (INSTANCE != null) {
            ExecutorServiceUtil.forceShutdown(INSTANCE.taskScheduler);
            INSTANCE = null;
        }
    }

    @Getter
    public class QueryHistoryAccelerateRunner extends QueryHistoryTask {
        private final boolean isManual;

        public QueryHistoryAccelerateRunner(boolean isManual, String project) {
            this.isManual = isManual;
            this.project = project;
        }

        public QueryHistoryAccelerateRunner(boolean isManual) {
            this(isManual, null);
        }

        @Override
        protected String name() {
            return "queryAcc";
        }

        @Override
        protected List<QueryHistory> getQueryHistories(int batchSize, String project) {
            QueryHistoryIdOffsetManager qhIdOffsetManager = QueryHistoryIdOffsetManager.getInstance(project);
            return JdbcUtil.withTxAndRetry(qhIdOffsetManager.getTransactionManager(), () -> {
                QueryHistoryIdOffset qhIdOffset = qhIdOffsetManager.get(ACCELERATE);
                List<QueryHistory> queryHistoryList = queryHistoryDAO
                        .queryQueryHistoriesByIdOffset(qhIdOffset.getOffset(), batchSize, project);
                if (!queryHistoryList.isEmpty()) {
                    long maxId = 0;
                    for (QueryHistory queryHistory : queryHistoryList) {
                        if (queryHistory.getId() > maxId) {
                            maxId = queryHistory.getId();
                        }
                    }
                    updateIdOffset(maxId, project);
                }
                return queryHistoryList;
            });
        }

        @Override
        public void work(String project) {
            if (NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project).isExpertMode()) {
                log.info("Skip QueryHistoryAccelerateRunner job, project [{}].", project);
                return;
            }
            log.info("Start QueryHistoryAccelerateRunner job, project [{}].", project);

            int batchSize = KylinConfig.getInstanceFromEnv().getQueryHistoryAccelerateBatchSize();
            int maxSize = isManual() //
                    ? KylinConfig.getInstanceFromEnv().getQueryHistoryAccelerateBatchSize()
                    : KylinConfig.getInstanceFromEnv().getQueryHistoryAccelerateMaxSize();
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            QueryHistoryIdOffsetManager qhIdOffsetManager = QueryHistoryIdOffsetManager.getInstance(project);
            if (System.currentTimeMillis() - qhIdOffsetManager.get(ACCELERATE).getUpdateTime() >= TimeUnit.MINUTES
                    .toMillis(kylinConfig.getQueryHistoryAccelerateInterval()) || isManual) {
                batchHandle(batchSize, maxSize, project, this::accelerateAndUpdateMetadata);
                log.info("End QueryHistoryAccelerateRunner job, project [{}].", project);
            } else {
                log.info("Skip QueryHistoryAccelerateRunner job, project [{}].", project);
            }
        }

        public void accelerateAndUpdateMetadata(Pair<List<QueryHistory>, String> pair) {
            List<QueryHistory> queryHistories = pair.getFirst();
            String project = pair.getSecond();
            if (CollectionUtils.isEmpty(queryHistories)) {
                return;
            }
            // accelerate
            List<Pair<Long, QueryHistoryInfo>> idToQHInfoList = Lists.newArrayList();
            Map<String, Set<String>> submitterToGroups = getUserToGroups(queryHistories);
            List<QueryHistory> matchedCandidate = accelerateRuleUtil.findMatchedCandidate(project, queryHistories,
                    submitterToGroups, idToQHInfoList);
            queryHistoryDAO.batchUpdateQueryHistoriesInfo(idToQHInfoList);
            if (querySmartSupporter != null) {
                querySmartSupporter.onMatchQueryHistory(project, matchedCandidate);
            }
        }

        protected Map<String, Set<String>> getUserToGroups(List<QueryHistory> queryHistories) {
            Map<String, Set<String>> submitterToGroups = new HashMap<>();
            for (QueryHistory qh : queryHistories) {
                QueryHistoryInfo queryHistoryInfo = qh.getQueryHistoryInfo();
                if (queryHistoryInfo == null) {
                    continue;
                }
                String querySubmitter = qh.getQuerySubmitter();
                submitterToGroups.putIfAbsent(querySubmitter, userGroupService.listUserGroups(querySubmitter));
            }
            return submitterToGroups;
        }

        private void updateIdOffset(long maxId, String project) {
            QueryHistoryIdOffsetManager offsetManager = QueryHistoryIdOffsetManager.getInstance(project);
            // update id offset
            JdbcUtil.withTxAndRetry(offsetManager.getTransactionManager(), () -> {
                offsetManager.updateOffset(ACCELERATE, copyForWrite -> copyForWrite.setOffset(maxId));
                return null;
            });
        }
    }

    public abstract static class QueryHistoryTask implements Runnable {

        protected String project;

        protected abstract String name();

        public void batchHandle(int batchSize, int maxSize, String project,
                Consumer<Pair<List<QueryHistory>, String>> consumer) {
            if (!(batchSize > 0 && maxSize >= batchSize)) {
                throw new IllegalArgumentException(String.format(Locale.ROOT,
                        "%s task, batch size: %d , maxsize: %d is illegal", name(), batchSize, maxSize));
            }
            int finishNum = 0;
            while (true) {
                List<QueryHistory> queryHistories = getQueryHistories(batchSize, project);
                finishNum = finishNum + queryHistories.size();
                if (isInterrupted()) {
                    break;
                }
                if (!queryHistories.isEmpty()) {
                    consumer.accept(new Pair<>(queryHistories, project));
                }
                log.debug("{} handled {} query history", name(), queryHistories.size());
                if (queryHistories.size() < batchSize || finishNum >= maxSize) {
                    break;
                }
            }
        }

        protected boolean isInterrupted() {
            return false;
        }

        protected abstract List<QueryHistory> getQueryHistories(int batchSize, String project);

        @Override
        public void run() {
            List<String> projects = null;
            if (project != null) {
                projects = Collections.singletonList(project);
            } else {
                KylinConfig config = KylinConfig.getInstanceFromEnv();
                projects = NProjectManager.getInstance(config).listAllProjects().stream().map(ProjectInstance::getName)
                        .collect(Collectors.toList());
            }
            projects.forEach(projectName -> {
                try {
                    work(projectName);
                } catch (Exception e) {
                    log.warn("QueryHistory {}  process failed of project({})", name(), projectName, e);
                }
            });
        }

        protected abstract void work(String project);

    }
}
