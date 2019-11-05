/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.job;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.engine.spark.utils.BuildUtils;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;

public class BuildLayoutWithUpdate {
    protected static final Logger logger = LoggerFactory.getLogger(BuildLayoutWithUpdate.class);
    private ExecutorService pool = Executors.newCachedThreadPool();
    private CompletionService<JobResult> completionService = new ExecutorCompletionService<>(pool);
    private int currentLayoutsNum = 0;

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
                return new JobResult(nDataLayouts, throwable);
            }
        });
        currentLayoutsNum++;
    }

    public void updateLayout(NDataSegment seg, KylinConfig config, String project) {
        for (int i = 0; i < currentLayoutsNum; i++) {
            try {
                logger.info("Wait to take job result.");
                JobResult result = completionService.take().get();
                logger.info("Take job result successful.");
                if (result.isFailed()) {
                    shutDownPool();
                    throw new RuntimeException(result.getThrowable());
                }
                for (NDataLayout layout : result.getLayouts()) {
                    BuildUtils.updateDataFlow(seg, layout, config, project);
                }
            } catch (InterruptedException | ExecutionException e) {
                shutDownPool();
                throw new RuntimeException(e);
            }
        }
        currentLayoutsNum = 0;
    }

    private void shutDownPool() {
        pool.shutdown();
        try {
            pool.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("Error occurred when shutdown thread pool.", e);
            pool.shutdownNow();
        }
    }

    private static class JobResult {
        private List<NDataLayout> layouts;
        private Throwable throwable;

        JobResult(List<NDataLayout> layouts, Throwable throwable) {
            this.layouts = layouts;
            this.throwable = throwable;
        }

        boolean isFailed() {
            return throwable != null;
        }

        Throwable getThrowable() {
            return throwable;
        }

        List<NDataLayout> getLayouts() {
            return layouts;
        }
    }

    static abstract class JobEntity {

        public abstract String getName();

        public abstract List<NDataLayout> build() throws IOException;
    }
}
