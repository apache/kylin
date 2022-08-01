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

package org.apache.kylin.engine.spark.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildLayoutWithUpdate {
    protected static final Logger logger = LoggerFactory.getLogger(BuildLayoutWithUpdate.class);
    private static final int CHECK_POINT_DELAY_SEC = 5;
    private final ScheduledExecutorService checkPointer;
    private final LinkedBlockingQueue<LayoutPoint> layoutPoints;
    private ExecutorService pool = Executors.newCachedThreadPool();
    private CompletionService<JobResult> completionService = new ExecutorCompletionService<>(pool);
    private int currentLayoutsNum = 0;

    public BuildLayoutWithUpdate() {
        layoutPoints = new LinkedBlockingQueue<>();
        checkPointer = Executors.newSingleThreadScheduledExecutor();
        startCheckPoint();
    }

    public void submit(JobEntity job, KylinConfig config) {
        completionService.submit(new Callable<JobResult>() {
            @Override
            public JobResult call() throws Exception {
                KylinConfig.setAndUnsetThreadLocalConfig(config);
                Thread.currentThread().setName("thread-" + job.getName());
                List<NDataLayout> nDataLayouts = new LinkedList<>();
                Throwable throwable = null;
                try {
                    nDataLayouts = job.build();
                } catch (Throwable t) {
                    logger.error("Error occurred when run " + job.getName(), t);
                    throwable = t;
                }
                return new JobResult(job.getIndexId(), nDataLayouts, throwable);
            }
        });
        currentLayoutsNum++;
    }

    public void updateLayout(NDataSegment seg, KylinConfig config, String project) {
        for (int i = 0; i < currentLayoutsNum; i++) {
            updateSingleLayout(seg, config, project);
        }
        // flush
        doCheckPoint();
        currentLayoutsNum = 0;
    }

    public long updateSingleLayout(NDataSegment seg, KylinConfig config, String project) {
        long indexId = -1l;
        try {
            logger.info("Wait to take job result.");
            JobResult result = completionService.take().get();
            logger.info("Take job result successful.");
            if (result.isFailed()) {
                shutDown();
                throw new RuntimeException(result.getThrowable());
            }
            indexId = result.getIndexId();
            for (NDataLayout layout : result.getLayouts()) {
                logger.info("Update layout {} in dataflow {}, segment {}", layout.getLayoutId(),
                        seg.getDataflow().getUuid(), seg.getId());
                if (!layoutPoints.offer(new LayoutPoint(project, config, seg.getDataflow().getId(), layout))) {
                    throw new IllegalStateException(
                            "[UNLIKELY_THINGS_HAPPENED] Make sure that layoutPoints can offer.");
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            shutDown();
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        // flush
        doCheckPoint();
        return indexId;
    }

    private void startCheckPoint() {
        checkPointer.scheduleWithFixedDelay(this::doCheckPoint, CHECK_POINT_DELAY_SEC, CHECK_POINT_DELAY_SEC,
                TimeUnit.SECONDS);
    }

    private synchronized void doCheckPoint() {
        LayoutPoint lp = layoutPoints.poll();
        if (Objects.isNull(lp)) {
            return;
        }

        if (Objects.isNull(lp.getLayout())) {
            logger.warn("[LESS_LIKELY_THINGS_HAPPENED] layout shouldn't be empty.");
            return;
        }

        final String project = lp.getProject();
        final KylinConfig config = lp.getConfig();
        final String dataFlowId = lp.getDataFlowId();
        final List<NDataLayout> layouts = new ArrayList<>();
        layouts.add(lp.getLayout());
        StringBuilder sb = new StringBuilder("Checkpoint layouts: ").append(lp.getLayout().getLayoutId());

        while (Objects.nonNull(lp = layoutPoints.peek()) && Objects.equals(project, lp.getProject())
                && Objects.equals(dataFlowId, lp.getDataFlowId()) && Objects.nonNull(lp.getLayout())) {
            layouts.add(lp.getLayout());
            sb.append(',').append(lp.getLayout().getLayoutId());
            LayoutPoint temp = layoutPoints.remove();
            if (!Objects.equals(lp, temp)) {
                throw new IllegalStateException(
                        "[UNLIKELY_THINGS_HAPPENED] Make sure that only one thread can execute layoutPoints poll.");
            }
        }

        // add log
        final String layoutsLog = sb.toString();
        logger.info(layoutsLog);

        // persist to storage
        KylinConfig.setAndUnsetThreadLocalConfig(config);
        updateLayouts(config, project, dataFlowId, layouts);

        if (Objects.nonNull(layoutPoints.peek())) {
            // schedule immediately
            doCheckPoint();
        }
    }

    protected void updateLayouts(KylinConfig config, String project, String dataFlowId,
            final List<NDataLayout> layouts) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowUpdate update = new NDataflowUpdate(dataFlowId);
            update.setToAddOrUpdateLayouts(layouts.toArray(new NDataLayout[0]));
            NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateDataflow(update);
            return 0;
        }, project);
    }

    public void shutDown() {
        pool.shutdown();
        try {
            pool.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("Error occurred when shutdown thread pool.", e);
            ExecutorServiceUtil.forceShutdown(pool);
            Thread.currentThread().interrupt();
        }

        checkPointer.shutdown();
        try {
            checkPointer.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("Error occurred when shutdown checkPointer.", e);
            ExecutorServiceUtil.forceShutdown(checkPointer);
            Thread.currentThread().interrupt();
        }
    }

    private static class LayoutPoint {
        private final String project;
        private final KylinConfig config;
        private final String dataFlowId;
        private final NDataLayout layout;

        public LayoutPoint(String project, KylinConfig config, String dataFlowId, NDataLayout layout) {
            this.project = project;
            this.config = config;
            this.dataFlowId = dataFlowId;
            this.layout = layout;
        }

        public String getProject() {
            return project;
        }

        public KylinConfig getConfig() {
            return config;
        }

        public String getDataFlowId() {
            return dataFlowId;
        }

        public NDataLayout getLayout() {
            return layout;
        }
    }

    private static class JobResult {
        private long indexId;
        private List<NDataLayout> layouts;
        private Throwable throwable;

        JobResult(long indexId, List<NDataLayout> layouts, Throwable throwable) {
            this.indexId = indexId;
            this.layouts = layouts;
            this.throwable = throwable;
        }

        boolean isFailed() {
            return throwable != null;
        }

        Throwable getThrowable() {
            return throwable;
        }

        long getIndexId() {
            return indexId;
        }

        List<NDataLayout> getLayouts() {
            return layouts;
        }
    }

    public static abstract class JobEntity {

        public abstract long getIndexId();

        public abstract String getName();

        public abstract List<NDataLayout> build() throws IOException;
    }
}
