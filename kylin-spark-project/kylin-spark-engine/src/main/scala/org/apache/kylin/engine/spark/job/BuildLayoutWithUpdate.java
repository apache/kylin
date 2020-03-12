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
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.metadata.SegmentInfo;
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                LayoutEntity dataLayouts = null;
                Throwable throwable = null;
                try {
                    dataLayouts = job.build();
                } catch (Throwable t) {
                    logger.error("Error occurred when run " + job.getName(), t);
                    throwable = t;
                }
                return new JobResult(dataLayouts, throwable);
            }
        });
        currentLayoutsNum++;
    }

    public void updateLayout(SegmentInfo seg, KylinConfig config) {
        for (int i = 0; i < currentLayoutsNum; i++) {
            try {
                logger.info("Wait to take job result.");
                JobResult result = completionService.take().get();
                logger.info("Take job result successful.");
                if (result.isFailed()) {
                    shutDownPool();
                    throw new RuntimeException(result.getThrowable());
                }
                seg.updateLayout(result.layout);
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
        private LayoutEntity layout;
        private Throwable throwable;

        JobResult(LayoutEntity layout, Throwable throwable) {
            this.layout = layout;
            this.throwable = throwable;
        }

        boolean isFailed() {
            return throwable != null;
        }

        Throwable getThrowable() {
            return throwable;
        }

        LayoutEntity getLayout() {
            return layout;
        }
    }

    static abstract class JobEntity {

        public abstract String getName();

        public abstract LayoutEntity build() throws IOException;
    }
}
