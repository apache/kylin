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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.builder.NBuildSourceInfo;
import org.apache.kylin.engine.spark.metadata.SegmentInfo;
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity;
import org.apache.kylin.engine.spark.metadata.cube.model.SpanningTree;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;

public class BuildLayerWithUpdate {

    protected static final Logger logger = LoggerFactory.getLogger(BuildLayoutWithUpdate.class);
    private ExecutorService pool = Executors.newCachedThreadPool();
    private CompletionService<LayerJobResult> completionService = new ExecutorCompletionService<>(pool);
    private int currentBuildInfoNum = 0;

    public void submit(LayerJobEntity job) {
        completionService.submit(new Callable<LayerJobResult>() {
            @Override
            public LayerJobResult call() throws Exception {
                KylinConfig.setAndUnsetThreadLocalConfig(job.config);
                Thread.currentThread().setName("thread-build-layer" + job.info.getLayoutId());
                try {
                    return job.build();
                } catch (Throwable t) {
                    logger.error("Error occurred when run " + job.info.getLayoutId(), t);
                    return new LayerJobResult( t);
                }
            }
        });
        currentBuildInfoNum ++;
    }

    public void updateLayer() {
        for (int i = 0; i < currentBuildInfoNum; i++) {
            try {
                logger.info("Wait to take layer result.");
                LayerJobResult result = completionService.take().get();
                logger.info("Take job layer successful.");
                if (result.isFailed()) {
                    shutDownPool();
                    throw new RuntimeException(result.getThrowable());
                }
            } catch (InterruptedException | ExecutionException e) {
                shutDownPool();
                throw new RuntimeException(e);
            }
        }
        currentBuildInfoNum = 0;
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

    private static class LayerJobResult {
        private Throwable throwable;

        LayerJobResult(Throwable throwable) {
            this.throwable = throwable;
        }

        boolean isFailed() {
            return throwable != null;
        }

        Throwable getThrowable() {
            return throwable;
        }

    }

    public static class LayerJobEntity {

        NBuildSourceInfo info;
        SpanningTree st;
        SegmentInfo seg;
        CubeBuildJob job;
        KylinConfig config;
        Map<Long, Long> cuboidsRowCount;

        public LayerJobEntity(NBuildSourceInfo info, SpanningTree st, KylinConfig config,
                              CubeBuildJob job, SegmentInfo seg, Map<Long, Long> cuboidsRowCount){
            this.info = info;
            this.config = config;
            this.job = job;
            this.st = st;
            this.seg = seg;
            this.cuboidsRowCount = cuboidsRowCount;
        }

        public LayerJobResult build() {
            Collection<LayoutEntity> toBuildCuboids = info.getToBuildCuboids();
            Dataset<Row> parentDS = info.getParentDS();
            if (toBuildCuboids.size() > 1) {
                parentDS.cache();
            }
            // record the source count of flat table
            if (info.getLayoutId() == ParentSourceChooser.FLAT_TABLE_FLAG()) {
                cuboidsRowCount.putIfAbsent(info.getLayoutId(), parentDS.count());
            }
            BuildLayoutWithUpdate buildLayout = new BuildLayoutWithUpdate();
            for (LayoutEntity index : toBuildCuboids) {
                Preconditions.checkNotNull(parentDS, "Parent dataset is null when building.");
                buildLayout.submit(new BuildLayoutWithUpdate.JobEntity() {
                    @Override
                    public String getName() {
                        return "build-cuboid-" + index.getId();
                    }
                    @Override
                    public LayoutEntity build() throws IOException {
                        return job.buildCuboid(seg, index, parentDS, st, info.getLayoutId());
                    }
                }, config);
            }
            //wait until all cuboid to finish, then unpersist dependent parent dataset
            buildLayout.updateLayout(seg, this.config);
            if (toBuildCuboids.size() > 1)
                parentDS.unpersist();
            return new LayerJobResult(null);
        }
    }
}
