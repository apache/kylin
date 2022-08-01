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

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.util.InfluxDBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.guava20.shaded.common.base.Throwables;
import io.kyligence.kap.shaded.influxdb.org.influxdb.BatchOptions;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBIOException;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Point;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Pong;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.Query;
import io.kyligence.kap.shaded.influxdb.org.influxdb.dto.QueryResult;
import lombok.Getter;
import lombok.Setter;

@Getter
public class InfluxDBInstance {
    private static final Logger logger = LoggerFactory.getLogger(InfluxDBInstance.class);

    private String database;
    private String retentionPolicyName;
    private String retentionDuration;
    private String shardDuration;
    private int replicationFactor;
    private boolean useDefault;

    private static final String DEFAULT_DATABASE = "KE_MONITOR";
    private static final String DEFAULT_RETENTION_POLICY_NAME = "KE_MONITOR_RP";
    private static final String RETENTION_DURATION = "90d";
    private static final String SHARD_DURATION = "7d";
    private static final int REPLICATION_FACTOR = 1;
    private static final boolean USE_DEFAULT = true;

    @Setter
    private volatile InfluxDB influxDB;

    private ScheduledExecutorService scheduledExecutorService;
    private KapConfig config;

    public InfluxDBInstance(String database, String retentionPolicyName, String retentionDuration, String shardDuration,
            int replicationFactor, boolean useDefault) {
        this.database = database;
        this.retentionPolicyName = retentionPolicyName;
        this.retentionDuration = retentionDuration;
        this.shardDuration = shardDuration;
        this.replicationFactor = replicationFactor;
        this.useDefault = useDefault;

        this.config = KapConfig.wrap(KylinConfig.getInstanceFromEnv());
    }

    public void init() {
        final String addr = config.influxdbAddress();
        if (StringUtils.isEmpty(addr)) {
            logger.info("InfluxDB address is empty, skip it");
            return;
        }
        tryConnectInfluxDB();
        startMonitorInfluxDB();
    }

    private void tryConnectInfluxDB() {
        try {
            if (influxDB == null) {
                final String addr = config.influxdbAddress();
                final String username = config.influxdbUsername();
                final String password = config.influxdbPassword();
                final boolean enableHttps = config.isInfluxdbHttpsEnabled();
                final boolean enableUnsafeSsl = config.isInfluxdbUnsafeSslEnabled();

                logger.info("Init influxDB, address: {}, username: {}", addr, username);

                influxDB = InfluxDBUtils.getInfluxDBInstance(addr, username, password, enableHttps, enableUnsafeSsl);
                influxDB.setDatabase(getDatabase());
                influxDB.setRetentionPolicy(getRetentionPolicyName());

                if (!influxDB.databaseExists(getDatabase())) {
                    logger.info("Create influxDB database {}", getDatabase());
                    influxDB.createDatabase(getDatabase());
                    // create retention policy and use it as the default
                    logger.info("Create influxDB retention policy '{}' on database '{}'", getRetentionPolicyName(),
                            getDatabase());
                    influxDB.createRetentionPolicy(getRetentionPolicyName(), getDatabase(), getRetentionDuration(),
                            getShardDuration(), getReplicationFactor(), isUseDefault());
                }

                // enable async write. max batch size 1000, flush duration 3s.
                // when bufferLimit > actions，#RetryCapableBatchWriter will be used
                influxDB.enableBatch(BatchOptions.DEFAULTS.actions(1000).bufferLimit(10000)
                        .flushDuration(config.getInfluxDBFlushDuration()).jitterDuration(500));

            } else {
                final Pong pong = influxDB.ping();
                MetricsGroup.monitorRegisterMetrics();
                logger.trace("Connected to influxDB successfully. [{}]", pong);
            }
        } catch (Exception ex) {
            if (influxDB != null && influxDB.isBatchEnabled()) {
                influxDB.disableBatch();
            }
            influxDB = null;
            if (Throwables.getCausalChain(ex).stream().anyMatch(t -> t instanceof InfluxDBIOException)) {
                logger.warn("Check influxDB Instance error, database: {}, retentionPolicy: {} ex: {}", getDatabase(),
                        getRetentionPolicyName(), ex.getMessage());
                return;
            }
            logger.error("Unknown exception happened", ex);
        }
    }

    private void startMonitorInfluxDB() {
        logger.info("Start to monitor influxDB Instance, database: {}, retentionPolicy: {}", getDatabase(),
                getRetentionPolicyName());
        scheduledExecutorService = Executors
                .newSingleThreadScheduledExecutor(new NamedThreadFactory("InfluxDBMonitor-" + this.getDatabase()));
        scheduledExecutorService.scheduleWithFixedDelay(this::tryConnectInfluxDB, 60, 600, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            getInfluxDB().close();
            ExecutorServiceUtil.forceShutdown(scheduledExecutorService);
            logger.info("Shutdown InfluxDB Instance, database: {}, retentionPolicy: {}", getDatabase(),
                    getRetentionPolicyName());
        }));
    }

    public boolean write(String measurement, Map<String, String> tags, Map<String, Object> fields, long timestamp) {
        if (influxDB == null) {
            logger.error("InfluxDB is not connected, abort writing.");
            return false;
        }

        Point p = Point.measurement(measurement) //
                .time(timestamp, TimeUnit.MILLISECONDS) //
                .tag(tags) //
                .fields(fields) //
                .build(); //

        getInfluxDB().write(p);
        return true;
    }

    public QueryResult read(String sql) {
        if (influxDB == null) {
            logger.error("InfluxDB is not connected, abort reading.");
            return new QueryResult();
        }
        return getInfluxDB().query(new Query(sql, getDatabase()));
    }
}
