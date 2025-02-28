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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.cluster.ClusterManagerFactory;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.metrics.service.JobStatusMonitorMetric;
import org.apache.kylin.common.metrics.service.MonitorDao;
import org.apache.kylin.common.metrics.service.MonitorMetric;
import org.apache.kylin.common.metrics.service.QueryMonitorMetric;
import org.apache.kylin.common.state.StateSwitchConstant;
import org.apache.kylin.common.util.ClusterConstant;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.state.QueryShareStateManager;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.config.initialize.AfterMetadataReadyEvent;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.monitor.AbstractMonitorCollectTask;
import org.apache.kylin.rest.monitor.MonitorReporter;
import org.apache.kylin.rest.monitor.SparkContextCanary;
import org.apache.kylin.rest.request.AlertMessageRequest;
import org.apache.kylin.rest.response.ClusterStatisticStatusResponse;
import org.apache.kylin.rest.response.ClusterStatusResponse;
import org.apache.kylin.rest.response.ClusterStatusResponse.NodeState;
import org.apache.kylin.rest.response.ProjectConfigResponse;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.apache.spark.metrics.SparkPrometheusMetrics;
import org.apache.spark.sql.SparderEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("monitorService")
public class MonitorService extends BasicService implements ApplicationListener<AfterMetadataReadyEvent> {
    private static final Logger logger = LoggerFactory.getLogger(MonitorService.class);

    @Setter
    @Autowired
    private ProjectService projectService;

    @Setter
    @Autowired
    private ClusterManager clusterManager;

    @VisibleForTesting
    public List<ProjectInstance> getReadableProjects() {
        return projectService.getManager(NProjectManager.class).listAllProjects();
    }

    @VisibleForTesting
    public Set<String> getAllYarnQueues() {
        return getReadableProjects().stream().map(ProjectInstance::getName).map(projectService::getProjectConfig0)
                .map(ProjectConfigResponse::getYarnQueue).collect(Collectors.toSet());
    }

    @Override
    public void onApplicationEvent(AfterMetadataReadyEvent event) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        KapConfig kapConfig = KapConfig.wrap(kylinConfig);
        boolean monitorEnabled = kapConfig.isMonitorEnabled();
        MonitorReporter monitorReporter = MonitorReporter.getInstance();
        if (!monitorEnabled) {
            logger.warn("Monitor reporter is not enabled!");
            return;
        }
        try {
            monitorReporter.startReporter();
        } catch (Exception e) {
            log.error("Failed to start monitor reporter!", e);
        }
        monitorReporter.submit(new AbstractMonitorCollectTask(
                Lists.newArrayList(ClusterConstant.ALL, ClusterConstant.QUERY, ClusterConstant.JOB)) {
            @Override
            protected MonitorMetric collect() {
                QueryMonitorMetric queryMonitorMetric = monitorReporter.createQueryMonitorMetric();

                queryMonitorMetric.setLastResponseTime(SparkContextCanary.getInstance().getLastResponseTime());
                queryMonitorMetric.setErrorAccumulated(SparkContextCanary.getInstance().getErrorAccumulated());
                queryMonitorMetric.setSparkRestarting(SparkContextCanary.getInstance().isSparkRestarting());

                return queryMonitorMetric;
            }
        });
        if (!kylinConfig.isJobNode() && !kylinConfig.isDataLoadingNode()) {
            return;
        }
        monitorReporter
                .submit(new AbstractMonitorCollectTask(Lists.newArrayList(ClusterConstant.ALL, ClusterConstant.JOB)) {
                    @Override
                    protected MonitorMetric collect() {
                        return collectJobMetric();
                    }
                });
    }

    private JobStatusMonitorMetric collectJobMetric() {
        List<AbstractExecutable> finishedJobs = new ArrayList<>();
        List<AbstractExecutable> runningJobs = new ArrayList<>();
        List<AbstractExecutable> pendingJobs = new ArrayList<>();
        List<AbstractExecutable> errorJobs = new ArrayList<>();

        for (ProjectInstance project : getReadableProjects()) {
            val executableManager = getManager(ExecutableManager.class, project.getName());

            for (ExecutablePO executablePO : executableManager.getAllJobs()) {
                AbstractExecutable executable = executableManager.fromPO(executablePO);
                if (executable.getStatusInMem().isFinalState()) {
                    finishedJobs.add(executable);
                } else if (ExecutableState.RUNNING == executable.getStatusInMem()) {
                    runningJobs.add(executable);
                } else if (ExecutableState.READY == executable.getStatusInMem()
                        || ExecutableState.PAUSED == executable.getStatusInMem()
                        || ExecutableState.PENDING == executable.getStatusInMem()) {
                    pendingJobs.add(executable);
                } else if (ExecutableState.ERROR == executable.getStatusInMem()) {
                    errorJobs.add(executable);
                }
            }

        }

        List<String> runningOnYarnJobs = ClusterManagerFactory.create(getConfig()).getRunningJobs(getAllYarnQueues());
        val pendingOnYarnJobs = runningJobs.stream()
                .filter(job -> pendingOnYarn(Sets.newHashSet(runningOnYarnJobs), job)).count();

        JobStatusMonitorMetric metric = MonitorReporter.getInstance().createJobStatusMonitorMetric();

        metric.setErrorJobs((long) errorJobs.size());
        metric.setFinishedJobs((long) finishedJobs.size());
        metric.setPendingJobs(pendingJobs.size() + pendingOnYarnJobs);
        metric.setRunningJobs(runningJobs.size() - pendingOnYarnJobs);
        return metric;
    }

    private long floorTime(long time) {
        return floorTime(time, KapConfig.wrap(getConfig()).getMonitorInterval());
    }

    private static long floorTime(long time, long interval) {
        // interval need to <= 60 * 60 * 1000
        return time - time % interval;
    }

    public ClusterStatusResponse currentClusterStatus() {
        return timeClusterStatus(floorTime(System.currentTimeMillis()));
    }

    public MonitorDao getMonitorDao() {
        return MonitorDao.getInstance();
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN //
            + " or hasPermission(#project, 'ADMINISTRATION')" //
            + " or hasPermission(#project, 'MANAGEMENT')" //
            + " or hasPermission(#project, 'OPERATION')")
    public ClusterStatusResponse timeClusterStatus(final long time) {
        val queryServers = clusterManager.getQueryServers().stream().map(ServerInfoResponse::getHost)
                .collect(Collectors.toList());
        val jobServers = clusterManager.getJobServers().stream().map(ServerInfoResponse::getHost)
                .collect(Collectors.toList());

        long jobInterval = KapConfig.wrap(getConfig()).getJobStatisticInterval();
        long interval = KapConfig.wrap(getConfig()).getMonitorInterval();
        val startTime = time - interval;

        val jobMetrics = transferToMap(getMonitorDao().readJobStatusMonitorMetricFromInfluxDB(startTime, time));
        val beforeStartTime1 = startTime - jobInterval;
        val beforeJobMetrics = transferToMap(
                getMonitorDao().readJobStatusMonitorMetricFromInfluxDB(beforeStartTime1, beforeStartTime1 + interval));

        val jobStatus = Maps.<String, NodeState> newHashMap();
        jobMetrics.entrySet().forEach(entry -> {
            NodeState nodeState = calculateNodeState(beforeJobMetrics.getOrDefault(entry.getKey(), null),
                    entry.getValue());
            jobStatus.put(entry.getValue().getInstanceName(), nodeState);
            jobStatus.put(entry.getValue().getIpPort(), nodeState);
        });
        val jobServerResponse = jobServers.stream()
                .collect(Collectors.toMap(s -> s, s -> jobStatus.getOrDefault(s, NodeState.CRASH)));

        val queryMetrics = transferToMap(getMonitorDao().readQueryMonitorMetricFromInfluxDB(startTime, time));

        val queryStatus = Maps.<String, NodeState> newHashMap();
        queryMetrics.values().forEach(monitorMetric -> {
            NodeState nodeState = calculateNodeState(monitorMetric);
            queryStatus.put(monitorMetric.getInstanceName(), nodeState);
            queryStatus.put(monitorMetric.getIpPort(), nodeState);
        });
        val queryServerResponse = queryServers.stream()
                .collect(Collectors.toMap(s -> s, s -> queryStatus.getOrDefault(s, NodeState.CRASH)));

        return clusterStatus(jobServerResponse, queryServerResponse);
    }

    public String fetchAndMergeSparkMetrics() {
        if (!SparderEnv.isSparkAvailable()) {
            return "";
        }
        String executorMetricsInfo = "";
        if (KylinConfig.getInstanceFromEnv().isSpark3ExecutorPrometheusEnabled()) {
            executorMetricsInfo = SparkPrometheusMetrics
                    .fetchExecutorMetricsInfo(SparderEnv.getSparkSession().sparkContext().applicationId());
        }
        String driverMetricsInfo = "";
        if ("org.apache.spark.metrics.sink.PrometheusServlet"
                .equals(KylinConfig.getInstanceFromEnv().getSpark3DriverPrometheusServletClass())
                && "/metrics/prometheus"
                        .equals(KylinConfig.getInstanceFromEnv().getSpark3DriverPrometheusServletPath())) {
            driverMetricsInfo = SparkPrometheusMetrics
                    .fetchDriverMetricsInfo(SparderEnv.getSparkSession().sparkContext().applicationId());
        }

        StringBuilder stringBuilder = new StringBuilder();
        if (StringUtils.isNotBlank(driverMetricsInfo)) {
            stringBuilder.append(driverMetricsInfo).append("\n");
        }
        if (StringUtils.isNotBlank(executorMetricsInfo)) {
            stringBuilder.append(executorMetricsInfo).append("\n");
        }
        return stringBuilder.toString();
    }

    public void handleAlertMessage(AlertMessageRequest request) {
        log.info("handle alert message : {}", request);
        List<AlertMessageRequest.Alerts> relatedQueryLimitAlerts = request.getAlerts().stream()
                .filter(e -> "Spark Utilization Is Too High".equalsIgnoreCase(e.getLabels().getAlertname()))
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(relatedQueryLimitAlerts)) {
            return;
        }
        List<String> needOpenLimitInstanceList = relatedQueryLimitAlerts.stream()
                .filter(e -> !"resolved".equals(e.getStatus())).map(e -> e.getLabels().getInstance()).distinct()
                .collect(Collectors.toList());
        List<String> needCloseLimitInstanceList = relatedQueryLimitAlerts.stream()
                .filter(e -> "resolved".equals(e.getStatus())).map(e -> e.getLabels().getInstance()).distinct()
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(needOpenLimitInstanceList)) {
            QueryShareStateManager.getInstance().setState(needOpenLimitInstanceList,
                    StateSwitchConstant.QUERY_LIMIT_STATE, "true");
        }
        if (CollectionUtils.isNotEmpty(needCloseLimitInstanceList)) {
            QueryShareStateManager.getInstance().setState(needCloseLimitInstanceList,
                    StateSwitchConstant.QUERY_LIMIT_STATE, "false");
        }
    }

    private ClusterStatusResponse clusterStatus(Map<String, NodeState> jobStatus, Map<String, NodeState> queryStatus) {
        val goodJobs = jobStatus.values().stream().filter(s -> s == NodeState.GOOD).count();
        val crashJobs = jobStatus.values().stream().filter(s -> s == NodeState.CRASH).count();
        val goodQueries = queryStatus.values().stream().filter(s -> s == NodeState.GOOD).count();
        val crashQueries = queryStatus.values().stream().filter(s -> s == NodeState.CRASH).count();

        val response = new ClusterStatusResponse();
        setTotalState(goodJobs, crashJobs, jobStatus.size(), response::setJobStatus);
        setTotalState(goodQueries, crashQueries, queryStatus.size(), response::setQueryStatus);
        response.setActiveInstances(jobStatus.size() + queryStatus.size()
                - Sets.intersection(jobStatus.keySet(), queryStatus.keySet()).size());
        response.setJob(transferToNodeStateResponse(jobStatus));
        response.setQuery(transferToNodeStateResponse(queryStatus));
        return response;
    }

    private List<ClusterStatusResponse.NodeStateResponse> transferToNodeStateResponse(Map<String, NodeState> states) {
        return states.entrySet().stream()
                .map(e -> new ClusterStatusResponse.NodeStateResponse(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    private void setTotalState(long goods, long crashes, long total, Consumer<NodeState> consumer) {
        if (goods == total) {
            consumer.accept(NodeState.GOOD);
        } else if (((double) crashes / total) >= KapConfig.wrap(getConfig()).getClusterCrashThreshhold()) {
            consumer.accept(NodeState.CRASH);
        } else {
            consumer.accept(NodeState.WARNING);
        }
    }

    private <T extends MonitorMetric> Map<String, T> transferToMap(List<T> metrics) {
        return metrics.stream().collect(Collectors.groupingBy(MonitorMetric::getInstanceName)).values().stream()
                .map(ts -> {
                    ts.sort(Comparator.comparing(MonitorMetric::getCreateTime).reversed());
                    return ts.iterator().next();
                }).collect(Collectors.toMap(MonitorMetric::getInstanceName, m -> m));
    }

    private boolean pendingOnYarn(Set<String> runningOnYarnJobs, AbstractExecutable executable) {
        val parent = (DefaultExecutable) executable;
        val runningJob = parent.getTasks().stream().filter(e -> e.getStatusInMem() == ExecutableState.RUNNING)
                .findFirst().orElse(null);
        if (runningJob == null) {
            return false;
        }
        return !runningOnYarnJobs.contains("job_step_" + runningJob.getId());
    }

    private NodeState calculateWhenNoFinished(long dP, long dE, long dNF, long dNFMax, long pendingJobs,
            long errorJobs) {
        if (dP < 0) {
            if (dE > 0) {
                return NodeState.WARNING;
            }
        } else if (dP > 0) {
            if (dNF >= dNFMax) {
                return NodeState.CRASH;
            } else if (dNF > 0) {
                return NodeState.WARNING;
            }
        } else {
            if (dE > 0) {
                return NodeState.CRASH;
            } else if (dE < 0) {
                return NodeState.GOOD;
            } else if ((pendingJobs + errorJobs) >= (dNFMax / 2)) {
                return NodeState.WARNING;
            }
        }

        return NodeState.GOOD;
    }

    private NodeState calculatedWhenFinishedError(long dNF, long dNFMax, long pendingJobs, long errorJobs) {
        if (dNF > 0 && dNF < dNFMax) {
            return NodeState.WARNING;
        } else if (dNF >= dNFMax) {
            return NodeState.CRASH;
        } else if (dNF == 0 && (pendingJobs + errorJobs) >= (dNFMax / 2)) {
            return NodeState.WARNING;
        }
        return NodeState.GOOD;
    }

    private NodeState calculateNodeState(JobStatusMonitorMetric m1, JobStatusMonitorMetric m2) {
        if (m2 == null) {
            return NodeState.CRASH;
        }
        if (m1 == null) {
            return NodeState.GOOD;
        }

        long dF = m2.getFinishedJobs() - m1.getFinishedJobs();
        long dE = m2.getErrorJobs() - m1.getErrorJobs();
        long dP = m2.getPendingJobs() - m1.getPendingJobs();
        long dNF = dP + dE;
        long dNFMax = KapConfig.wrap(getConfig()).getMaxPendingErrorJobs();
        double dNFMaxRation = KapConfig.wrap(getConfig()).getMaxPendingErrorJobsRation();
        if (dF > 0) {
            if (((dE + dP) > 0) && ((double) dF) / (dE + dP) <= dNFMaxRation) {
                return NodeState.WARNING;
            }
            return NodeState.GOOD;
        } else if (dF == 0) {
            return calculateWhenNoFinished(dP, dE, dNF, dNFMax, m2.getPendingJobs(), m2.getErrorJobs());
        }
        return calculatedWhenFinishedError(dNF, dNFMax, m2.getPendingJobs(), m2.getErrorJobs());
    }

    private NodeState calculateNodeState(QueryMonitorMetric metric) {
        if (metric == null) {
            return NodeState.CRASH;
        }

        if (metric.getErrorAccumulated() == 0) {
            return NodeState.GOOD;
        } else if (metric.getErrorAccumulated() >= KapConfig.wrap(getConfig()).getThresholdToRestartSpark()) {
            return NodeState.CRASH;
        } else {
            return NodeState.WARNING;
        }
    }

    public ClusterStatisticStatusResponse statisticClusterByFloorTime(final long start, final long end) {
        return statisticCluster(floorTime(start), floorTime(end));
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN //
            + " or hasPermission(#project, 'ADMINISTRATION')" //
            + " or hasPermission(#project, 'MANAGEMENT')" //
            + " or hasPermission(#project, 'OPERATION')")
    // /monitor/status/statistic
    public ClusterStatisticStatusResponse statisticCluster(final long start, final long end) {
        val jobs = clusterManager.getJobServers().stream().map(ServerInfoResponse::getHost)
                .collect(Collectors.toList());
        val queries = clusterManager.getQueryServers().stream().map(ServerInfoResponse::getHost)
                .collect(Collectors.toList());

        long interval = KapConfig.wrap(getConfig()).getMonitorInterval();
        long jobInterval = KapConfig.wrap(getConfig()).getJobStatisticInterval();

        val response = new ClusterStatisticStatusResponse();

        val jobMetrics = groupByInstance(
                getMonitorDao().readJobStatusMonitorMetricFromInfluxDB(start - jobInterval, end));
        val jobStatistics = Maps.<String, ClusterStatisticStatusResponse.NodeStatisticStatusResponse> newHashMap();
        jobMetrics.entrySet().forEach(entry -> {
            val value = toResponse(new JobNodeStatistic(entry.getKey(), entry.getValue(), start - jobInterval, end,
                    interval, start, end, jobInterval));
            jobStatistics.put(entry.getValue().get(0).getInstanceName(), value);
            jobStatistics.put(entry.getValue().get(0).getIpPort(), value);
        });
        response.setJob(jobs.stream().map(s -> jobStatistics.getOrDefault(s, fullCrashed(s, start, end, interval)))
                .collect(Collectors.toList()));

        val queryMetrics = groupByInstance(getMonitorDao().readQueryMonitorMetricFromInfluxDB(start, end));
        val queryStatistics = Maps.<String, ClusterStatisticStatusResponse.NodeStatisticStatusResponse> newHashMap();
        queryMetrics.entrySet().forEach(entry -> {
            val value = toResponse(new QueryNodeStatistic(entry.getKey(), entry.getValue(), start, end, interval));
            queryStatistics.put(entry.getValue().get(0).getInstanceName(), value);
            queryStatistics.put(entry.getValue().get(0).getIpPort(), value);
        });
        response.setQuery(
                queries.stream().map(s -> queryStatistics.getOrDefault(s, fullCrashed(s, start, end, interval)))
                        .collect(Collectors.toList()));

        response.setStart(start);
        response.setEnd(end);
        response.setInterval(interval);
        return response;
    }

    private ClusterStatisticStatusResponse.NodeStatisticStatusResponse fullCrashed(String instance, long start,
            long end, long interval) {
        val response = new ClusterStatisticStatusResponse.NodeStatisticStatusResponse();
        response.setInstance(instance);
        int size = (int) ((end - start) / interval);
        List<ClusterStatisticStatusResponse.NodeTimeState> crashedStates = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            crashedStates.add(new ClusterStatisticStatusResponse.NodeTimeState(start + i * interval, NodeState.CRASH));
        }
        response.setDetails(crashedStates);
        response.setUnavailableCount(size);
        response.setUnavailableTime(size * interval);
        return response;
    }

    private <T extends MonitorMetric> ClusterStatisticStatusResponse.NodeStatisticStatusResponse toResponse(
            NodeStatistic<T> statistic) {
        val response = new ClusterStatisticStatusResponse.NodeStatisticStatusResponse();
        response.setInstance(statistic.getName());
        response.setDetails(statistic.getStates().stream()
                .map(p -> new ClusterStatisticStatusResponse.NodeTimeState(p.getKey(), p.getValue()))
                .collect(Collectors.toList()));
        response.setUnavailableCount(statistic.getUnavailableCount());
        response.setUnavailableTime(statistic.getUnavailableTime());
        return response;
    }

    private <T extends MonitorMetric> Map<String, List<T>> groupByInstance(List<T> metrics) {
        return metrics.stream().sorted(Comparator.comparingLong(MonitorMetric::getCreateTime))
                .collect(Collectors.groupingBy(MonitorMetric::getInstanceName, Collectors.toList()));
    }

    abstract static class NodeStatistic<T extends MonitorMetric> {
        @Getter
        String name;
        List<T> metrics;
        int size;
        long start;
        long end;
        long interval;
        long realStart;
        long realEnd;

        @Getter(lazy = true)
        private final List<Pair<Long, NodeState>> states = statistic();

        NodeStatistic(String name, List<T> metrics, long start, long end, long interval) {
            this(name, metrics, start, end, interval, start, end);
        }

        NodeStatistic(String name, List<T> metrics, long start, long end, long interval, long realStart, long readEnd) {
            this.name = name;
            this.metrics = metrics;
            this.interval = interval;
            this.size = (int) ((floorTime(end, interval) - floorTime(start, interval)) / interval);
            this.start = start;
            this.end = end;
            this.realStart = realStart;
            this.realEnd = readEnd;
        }

        protected abstract T[] createMetricArray();

        protected abstract NodeState[] calculate(T[] ts);

        protected List<Pair<Long, NodeState>> statistic() {
            // to do config, 90d monitor data, OOM warning
            long range = 90L * 24 * 60 * 60 * 1000 / interval;
            if (size > range) {
                throw new IllegalArgumentException("Out of data range, only can calculate 90 days monitor data!");
            }

            T[] fullMetrics = createMetricArray();

            long firstTime = floorTime(start, interval);
            for (T metric : metrics) {
                int length = (int) ((floorTime(metric.getCreateTime(), interval) - firstTime) / interval);
                if (length >= fullMetrics.length || null != fullMetrics[length]) {
                    if (length >= fullMetrics.length) {
                        logger.warn("Monitor metric create_time error, time: {}, end: {}", metric.getCreateTime(), end);
                    } else {
                        logger.debug("Found multi monitor metric in same interval, time: {}", metric.getCreateTime());
                    }
                    continue;
                }

                fullMetrics[length] = metric;
            }

            NodeState[] calculatedStates = calculate(fullMetrics);
            List<Pair<Long, NodeState>> res = new ArrayList<>(calculatedStates.length);
            for (int i = 0; i < calculatedStates.length; i++) {
                res.add(new Pair<>(fullMetrics[i] == null ? start + i * interval : fullMetrics[i].getCreateTime(),
                        calculatedStates[i]));
            }
            return res.stream().filter(p -> p.getKey() >= realStart && p.getKey() < realEnd)
                    .collect(Collectors.toList());
        }

        public int getUnavailableCount() {
            return (int) getStates().stream().filter(e -> e.getValue() == NodeState.CRASH).count();
        }

        public long getUnavailableTime() {
            return getUnavailableCount() * interval;
        }

    }

    class JobNodeStatistic extends NodeStatistic<JobStatusMonitorMetric> {

        long calInterval;

        JobNodeStatistic(String name, List<JobStatusMonitorMetric> metrics, long start, long end, long interval,
                long realStart, long realEnd, long calInterval) {
            super(name, metrics, start, end, interval, realStart, realEnd);
            this.calInterval = calInterval;
        }

        @Override
        protected JobStatusMonitorMetric[] createMetricArray() {
            return new JobStatusMonitorMetric[size];
        }

        @Override
        protected NodeState[] calculate(JobStatusMonitorMetric[] fullMetrics) {
            NodeState[] states = new NodeState[size];
            for (int i = 0; i < fullMetrics.length; i++) {
                if (i == 0) {
                    states[i] = fullMetrics[i] == null ? NodeState.CRASH : NodeState.GOOD;
                } else {
                    states[i] = calculateNodeState(findMetricBefore(fullMetrics, i), fullMetrics[i]);
                }
            }
            return states;
        }

        private JobStatusMonitorMetric findMetricBefore(JobStatusMonitorMetric[] metrics, int i) {
            int pos = i - (int) (calInterval / interval);
            if (pos < 0) {
                return null;
            }
            return metrics[pos];
        }
    }

    class QueryNodeStatistic extends NodeStatistic<QueryMonitorMetric> {

        QueryNodeStatistic(String name, List<QueryMonitorMetric> metrics, long start, long end, long interval) {
            super(name, metrics, start, end, interval);
        }

        @Override
        protected QueryMonitorMetric[] createMetricArray() {
            return new QueryMonitorMetric[size];
        }

        @Override
        protected NodeState[] calculate(QueryMonitorMetric[] fullMetrics) {
            NodeState[] states = new NodeState[size];
            for (int i = 0; i < fullMetrics.length; i++) {
                states[i] = calculateNodeState(fullMetrics[i]);
            }
            return states;
        }

    }
}
