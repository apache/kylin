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
package org.apache.kylin.rest.config.initialize;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.metadata.recommendation.candidate.RawRecManager;
import org.apache.kylin.rest.service.task.QueryHistoryTaskScheduler;
import org.apache.kylin.streaming.manager.StreamingJobManager;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProjectDropListener {

    public void onDelete(String project) {
        log.debug("delete project {}", project);

        val kylinConfig = KylinConfig.getInstanceFromEnv();

        try {
            NExecutableManager.getInstance(kylinConfig, project).destoryAllProcess();
            StreamingJobManager.getInstance(kylinConfig, project).destroyAllProcess();
            RDBMSQueryHistoryDAO.getInstance().dropProjectMeasurement(project);
            RawRecManager.getInstance(project).deleteByProject(project);
            QueryHistoryTaskScheduler.shutdownByProject(project);
            NDefaultScheduler.shutdownByProject(project);

            MetricsGroup.removeProjectMetrics(project);
            if (KylinConfig.getInstanceFromEnv().isPrometheusMetricsEnabled()) {
                MetricsRegistry.deletePrometheusProjectMetrics(project);
            }
            EpochManager epochManager = EpochManager.getInstance();
            epochManager.deleteEpoch(project);
            deleteStorage(kylinConfig, project.split("\\.")[0]);
        } catch (Exception e) {
            log.warn("error when delete " + project + " storage", e);
        }
    }

    private void deleteStorage(KylinConfig config, String project) throws IOException {
        String strPath = config.getHdfsWorkingDirectory(project);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        if (fs.exists(new Path(strPath))) {
            fs.delete(new Path(strPath), true);
        }
    }

}
