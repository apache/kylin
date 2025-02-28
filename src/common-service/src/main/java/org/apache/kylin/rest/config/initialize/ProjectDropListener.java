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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffsetManager;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.metadata.recommendation.candidate.RawRecManager;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.apache.kylin.tool.restclient.RestClient;
import org.springframework.http.HttpHeaders;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProjectDropListener {

    public void onDelete(String project, ClusterManager clusterManager, HttpHeaders headers) {
        log.debug("delete project {}", project);

        val kylinConfig = KylinConfig.getInstanceFromEnv();

        try {
            destroyAllProcess(project, clusterManager, headers);
            ExecutableManager.getInstance(kylinConfig, project).deleteAllJobsOfProject();
            StreamingJobManager.getInstance(kylinConfig, project).destroyAllProcess();
            RDBMSQueryHistoryDAO.getInstance().dropProjectMeasurement(project);
            QueryHistoryIdOffsetManager.getInstance(project).delete();
            RawRecManager.getInstance(project).deleteByProject(project);

            MetricsGroup.removeProjectMetrics(project);
            if (KylinConfig.getInstanceFromEnv().isPrometheusMetricsEnabled()) {
                MetricsRegistry.deletePrometheusProjectMetrics(project);
            }
            deleteStorage(kylinConfig, project.split("\\.")[0]);
        } catch (Exception e) {
            log.warn("error when delete " + project + " storage", e);
        }
    }

    private void destroyAllProcess(String project, ClusterManager clusterManager, HttpHeaders headers)
            throws IOException {
        if (null == clusterManager || null == headers) {
            return;
        }
        List<ServerInfoResponse> serverInfoResponses = clusterManager.getJobServers();
        List<String> jobNodeHosts = serverInfoResponses.stream().map(serverInfoResponse -> serverInfoResponse.getHost())
                .collect(Collectors.toList());
        for (String host : jobNodeHosts) {
            RestClient client = new RestClient(host);
            Map<String, String> form = Maps.newHashMap();
            form.put("project", project);
            client.forwardPostWithUrlEncodedForm("/jobs/destroy_job_process", headers, form);
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
