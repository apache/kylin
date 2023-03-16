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
package org.apache.kylin.common.metrics.service;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;

import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;

import org.apache.kylin.shaded.influxdb.org.influxdb.dto.QueryResult;
import org.apache.kylin.shaded.influxdb.org.influxdb.impl.InfluxDBResultMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MonitorDao {

    private final InfluxDBInstance influxDBInstance;
    public static final String QUERY_METRICS_BY_TIME_SQL_FORMAT = "SELECT * FROM %s WHERE create_time >= %d AND create_time < %d";

    private MonitorDao() {
        val kylinConfig = KylinConfig.getInstanceFromEnv();

        val database = generateDatabase(kylinConfig);
        val rp = generateRetentionPolicy(kylinConfig);
        val retentionDuration = KapConfig.wrap(kylinConfig).getMonitorRetentionDuration();
        val shardDuration = KapConfig.wrap(kylinConfig).getMonitorShardDuration();
        val replicationFactor = KapConfig.wrap(kylinConfig).getMonitorReplicationFactor();
        val useDefault = KapConfig.wrap(kylinConfig).isMonitorUserDefault();
        this.influxDBInstance = new InfluxDBInstance(database, rp, retentionDuration, shardDuration, replicationFactor,
                useDefault);
    }

    @VisibleForTesting
    public MonitorDao(InfluxDBInstance influxDBInstance) {
        this.influxDBInstance = influxDBInstance;
    }

    public static MonitorDao getInstance() {
        return Singletons.getInstance(MonitorDao.class, monitorDaoClass -> {
            val dao = new MonitorDao();
            dao.influxDBInstance.init();
            return dao;
        });
    }

    public static String generateDatabase(KylinConfig kylinConfig) {
        return kylinConfig.getMetadataUrlPrefix() + "_" + KapConfig.wrap(kylinConfig).getMonitorDatabase();
    }

    public static String generateRetentionPolicy(KylinConfig kylinConfig) {
        return kylinConfig.getMetadataUrlPrefix() + "_" + KapConfig.wrap(kylinConfig).getMonitorRetentionPolicy();
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public class InfluxDBWriteRequest {
        private String database;
        private String measurement;
        private Map<String, String> tags;
        private Map<String, Object> fields;
        private Long timeStamp;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public class InfluxDBReadRequest {
        private String database;
        private String measurement;
        private Long startTime;
        private Long endTime;
    }

    public InfluxDBWriteRequest convert2InfluxDBWriteRequest(MonitorMetric monitorMetric) {
        return new InfluxDBWriteRequest(influxDBInstance.getDatabase(), monitorMetric.getTable(),
                monitorMetric.getTags(), monitorMetric.getFields(), monitorMetric.getCreateTime());
    }

    public boolean write2InfluxDB(InfluxDBWriteRequest writeRequest) {
        return this.influxDBInstance.write(writeRequest.getMeasurement(), writeRequest.getTags(),
                writeRequest.getFields(), writeRequest.getTimeStamp());
    }

    public List<QueryMonitorMetric> readQueryMonitorMetricFromInfluxDB(Long startTime, Long endTime) {
        QueryResult queryResult = readFromInfluxDBByTime(new InfluxDBReadRequest(this.influxDBInstance.getDatabase(),
                QueryMonitorMetric.QUERY_MONITOR_METRIC_TABLE, startTime, endTime));

        InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
        return resultMapper.toPOJO(queryResult, QueryMonitorMetric.class,
                QueryMonitorMetric.QUERY_MONITOR_METRIC_TABLE);
    }

    public List<JobStatusMonitorMetric> readJobStatusMonitorMetricFromInfluxDB(Long startTime, Long endTime) {
        QueryResult queryResult = readFromInfluxDBByTime(new InfluxDBReadRequest(this.influxDBInstance.getDatabase(),
                JobStatusMonitorMetric.JOB_STATUS_MONITOR_METRIC_TABLE, startTime, endTime));

        InfluxDBResultMapper resultMapper = new InfluxDBResultMapper();
        return resultMapper.toPOJO(queryResult, JobStatusMonitorMetric.class,
                JobStatusMonitorMetric.JOB_STATUS_MONITOR_METRIC_TABLE);
    }

    private QueryResult readFromInfluxDBByTime(InfluxDBReadRequest readRequest) {
        String influxDBSql = String.format(Locale.ROOT, QUERY_METRICS_BY_TIME_SQL_FORMAT, readRequest.getMeasurement(),
                readRequest.getStartTime(), readRequest.getEndTime());

        return this.influxDBInstance.read(influxDBSql);
    }

}
