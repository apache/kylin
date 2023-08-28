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

package org.apache.kylin.job.metrics;

import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RDBMJobMetricsDAO implements JobMetricsDao{

    private static final Logger logger = LoggerFactory.getLogger(RDBMJobMetricsDAO.class);

    private final JdbcJobMetricsStore jdbcJobMetricsStore;

    public RDBMJobMetricsDAO() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        if (!UnitOfWork.isAlreadyInTransaction()) {
            logger.info("Initializing RDBMJobMetricsDAO with KylinConfig Id: {} ", System.identityHashCode(config));
        }
        jdbcJobMetricsStore = new JdbcJobMetricsStore(config);
    }

    public static RDBMJobMetricsDAO getInstance() {
        return Singletons.getInstance(RDBMJobMetricsDAO.class);
    }

    public int insert(JobMetrics jobMetrics) {
        return jdbcJobMetricsStore.insert(jobMetrics);
    }

    @Override
    public JobMetricsStatistics getJobCountAndTotalBuildCost(long startTime, long endTime, String project) {
        List<JobMetricsStatistics> result = jdbcJobMetricsStore.JobCountAndTotalBuildCost(startTime, endTime, project);
        if (CollectionUtils.isEmpty(result)) {
            return new JobMetricsStatistics();
        }
        return result.get(0);
    }

    @Override
    public List<JobMetricsStatistics> getJobCountByModel(long startTime, long endTime, String project) {
        return jdbcJobMetricsStore.jobCountByModel(startTime, endTime, project);
    }

    @Override
    public List<JobMetricsStatistics> getJobCountByTime(long startTime, long endTime, String timeDimension, String project) {
        return jdbcJobMetricsStore.jobCountByTime(startTime, endTime, timeDimension, project);
    }

    @Override
    public List<JobMetricsStatistics> getJobBuildCostByModel(long startTime, long endTime, String project) {
        return jdbcJobMetricsStore.jobBuildCostByModel(startTime, endTime, project);
    }

    @Override
    public List<JobMetricsStatistics> getJobBuildCostByTime(long startTime, long endTime, String timeDimension, String project) {
        return jdbcJobMetricsStore.jobBuildCostByTime(startTime, endTime, timeDimension, project);
    }
}
