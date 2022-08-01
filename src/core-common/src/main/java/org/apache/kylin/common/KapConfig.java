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

package org.apache.kylin.common;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.common.util.FileUtils;

@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class KapConfig {

    // no need to cache KapConfig as it is so lightweight
    public static KapConfig getInstanceFromEnv() {
        return wrap(KylinConfig.getInstanceFromEnv());
    }

    public static KapConfig wrap(KylinConfig config) {
        return new KapConfig(config);
    }

    public static File getKylinHomeAtBestEffort() {
        String kylinHome = KylinConfig.getKylinHome();
        if (kylinHome != null) {
            return new File(kylinHome).getAbsoluteFile();
        } else {
            File confFile = KylinConfig.getSitePropertiesFile();
            return confFile.getAbsoluteFile().getParentFile().getParentFile();
        }
    }

    public static String getKylinLogDirAtBestEffort() {
        return new File(getKylinHomeAtBestEffort(), "logs").getAbsolutePath();
    }

    public static String getKylinConfDirAtBestEffort() {
        return new File(getKylinHomeAtBestEffort(), "conf").getAbsolutePath();
    }

    // ============================================================================

    private final KylinConfig config;

    private static final String CIRCUIT_BREAKER_THRESHOLD = "30000";

    private static final String HALF_MINUTE_MS = "30000";

    private static final String FALSE = "false";

    private static final String TRUE = "true";

    public static final String FI_PLATFORM = "FI";

    public static final String TDH_PLATFORM = "TDH";

    public static final String CHANNEL_CLOUD = "cloud";

    private KapConfig(KylinConfig config) {
        this.config = config;
    }

    public KylinConfig getKylinConfig() {
        return config;
    }

    public boolean isDevEnv() {
        return config.isDevEnv();
    }

    public String getWriteHdfsWorkingDirectory() {
        return config.getHdfsWorkingDirectory();
    }

    public String getReadHdfsWorkingDirectory() {
        String readFS = getParquetReadFileSystem();
        if (StringUtils.isNotEmpty(readFS)) {
            Path workingDir = new Path(getWriteHdfsWorkingDirectory());
            return new Path(readFS, Path.getPathWithoutSchemeAndAuthority(workingDir)).toString() + "/";
        }

        return getWriteHdfsWorkingDirectory();
    }

    public String getMetadataWorkingDirectory() {
        if (isCloud()) {
            return getWriteHdfsWorkingDirectory();
        }
        return getReadHdfsWorkingDirectory();
    }

    public String getParquetReadFileSystem() {
        String fileSystem = config.getOptional("kylin.storage.columnar.file-system", "");
        String fileSystemBackup = config.getOptional("kylin.storage.columnar.file-system-backup", "");
        return ReadFsSwitch.select(fileSystem, fileSystemBackup);
    }

    public int getParquetReadFileSystemBackupResetSec() {
        return Integer.parseInt(config.getOptional("kylin.storage.columnar.file-system-backup-reset-sec", "1000"));
    }

    public int getParquetSparkExecutorInstance() {
        return Integer.parseInt(
                config.getOptional("kylin.storage.columnar.spark-conf.spark.executor.instances", String.valueOf(1)));
    }

    public int getParquetSparkExecutorCore() {
        return Integer.parseInt(
                config.getOptional("kylin.storage.columnar.spark-conf.spark.executor.cores", String.valueOf(1)));
    }

    /**
     * where is parquet fles stored in hdfs , end with /
     */
    public String getWriteParquetStoragePath(String project) {
        String defaultPath = config.getHdfsWorkingDirectory() + project + "/parquet/";
        return config.getOptional("kylin.storage.columnar.hdfs-dir", defaultPath);
    }

    public String getReadParquetStoragePath(String project) {
        String readFS = getParquetReadFileSystem();
        if (StringUtils.isNotEmpty(readFS)) {
            Path parquetPath = new Path(getWriteParquetStoragePath(project));
            return new Path(readFS, Path.getPathWithoutSchemeAndAuthority(parquetPath)).toString() + "/";
        }

        return getWriteParquetStoragePath(project);
    }

    /**
     * parquet shard size, in MB
     */
    public int getMinBucketsNumber() {
        return Integer.parseInt(config.getOptional("kylin.storage.columnar.bucket-num", "1"));
    }

    public int getParquetStorageShardSizeMB() {
        return Integer.parseInt(config.getOptional("kylin.storage.columnar.shard-size-mb", "128"));
    }

    public long getParquetStorageShardSizeRowCount() {
        return Long.parseLong(config.getOptional("kylin.storage.columnar.shard-rowcount", "2500000"));
    }

    public long getParquetStorageCountDistinctShardSizeRowCount() {
        return Long.parseLong(config.getOptional("kylin.storage.columnar.shard-countdistinct-rowcount", "1000000"));
    }

    public int getParquetStorageRepartitionThresholdSize() {
        return Integer.parseInt(config.getOptional("kylin.storage.columnar.repartition-threshold-size-mb", "128"));
    }

    public boolean isProjectInternalDefaultPermissionGranted() {
        return Boolean
                .parseBoolean(config.getOptional("kylin.acl.project-internal-default-permission-granted", "true"));
    }

    /**
     * Massin
     */
    public String getMassinResourceIdentiferDir() {
        return config.getOptional("kylin.server.massin-resource-dir", "/massin");
    }

    public String getZookeeperConnectString() {
        return config.getZookeeperConnectString();
    }

    public boolean getDBAccessFilterEnable() {
        return Boolean.parseBoolean(config.getOptional("kylin.source.hive.database-access-filter-enabled", "true"));
    }

    /**
     * Diagnosis config
     */
    public long getDiagPackageTimeout() {
        return Long.parseLong(config.getOptional(("kylin.diag.package.timeout-seconds"), "3600"));
    }

    public int getExtractionStartTimeDays() {
        return Integer.parseInt(config.getOptional("kylin.diag.extraction.start-time-days", "3"));
    }

    /**
     * Online service
     */
    public String getKyAccountUsename() {
        return config.getOptional("kylin.kyaccount.username");
    }

    public String getKyAccountPassword() {
        return config.getOptional("kylin.kyaccount.password");
    }

    public String getKyAccountToken() {
        return config.getOptional("kylin.kyaccount.token");
    }

    public String getKyAccountSSOUrl() {
        return config.getOptional("kylin.kyaccount.url", "https://sso.kyligence.com");
    }

    public String getKyAccountSiteUrl() {
        return config.getOptional("kylin.kyaccount.site-url", "https://account.kyligence.io");
    }

    public String getChannelUser() {
        return config.getOptional("kylin.env.channel", "on-premises");
    }

    public boolean isCloud() {
        return getChannelUser().equals(CHANNEL_CLOUD);
    }

    /**
     * Spark configuration
     */
    public String getColumnarSparkEnv(String conf) {
        return config.getPropertiesByPrefix("kylin.storage.columnar.spark-env.").get(conf);
    }

    public String getColumnarSparkConf(String conf) {
        return config.getPropertiesByPrefix("kylin.storage.columnar.spark-conf.").get(conf);
    }

    public Map<String, String> getSparkConf() {
        return config.getPropertiesByPrefix("kylin.storage.columnar.spark-conf.");
    }

    /**
     *  Smart modeling
     */
    public String getSmartModelingConf(String conf) {
        return config.getOptional("kylin.smart.conf." + conf, null);
    }

    /**
     * Query
     */
    public boolean isImplicitComputedColumnConvertEnabled() {
        return Boolean.parseBoolean(config.getOptional("kylin.query.implicit-computed-column-convert", "true"));
    }

    public boolean isAggComputedColumnRewriteEnabled() {
        return Boolean.parseBoolean(config.getOptional("kylin.query.agg-computed-column-rewrite", "true"));
    }

    public int getComputedColumnMaxRecursionTimes() {
        return Integer.parseInt(config.getOptional("kylin.query.computed-column-max-recursion-times", "10"));
    }

    public boolean isJdbcEscapeEnabled() {
        return Boolean.parseBoolean(config.getOptional("kylin.query.jdbc-escape-enabled", "true"));
    }

    public int getListenerBusBusyThreshold() {
        return Integer.parseInt(config.getOptional("kylin.query.engine.spark-listenerbus-busy-threshold", "5000"));
    }

    public int getBlockNumBusyThreshold() {
        return Integer.parseInt(config.getOptional("kylin.query.engine.spark-blocknum-busy-threshold", "5000"));
    }

    // in general, users should not set this, cuz we will auto calculate this num.
    public int getSparkSqlShufflePartitions() {
        return Integer.parseInt(config.getOptional("kylin.query.engine.spark-sql-shuffle-partitions", "-1"));
    }

    /**
     * LDAP filter
     */
    public String getLDAPUserSearchFilter() {
        return config.getOptional("kylin.security.ldap.user-search-filter", "(objectClass=person)");
    }

    public String getLDAPGroupSearchFilter() {
        return config.getOptional("kylin.security.ldap.group-search-filter",
                "(|(objectClass=groupOfNames)(objectClass=group))");
    }

    public String getLDAPGroupMemberSearchFilter() {
        return config.getOptional("kylin.security.ldap.group-member-search-filter",
                "(&(cn={0})(objectClass=groupOfNames))");
    }

    public String getLDAPUserIDAttr() {
        return config.getOptional("kylin.security.ldap.user-identifier-attr", "cn");
    }

    public String getLDAPGroupIDAttr() {
        return config.getOptional("kylin.security.ldap.group-identifier-attr", "cn");
    }

    public String getLDAPGroupMemberAttr() {
        return config.getOptional("kylin.security.ldap.group-member-attr", "member");
    }

    public Integer getLDAPMaxPageSize() {
        return Integer.parseInt(config.getOptional("kylin.security.ldap.max-page-size", "1000"));
    }

    public Integer getLDAPMaxValRange() {
        return Integer.parseInt(config.getOptional("kylin.security.ldap.max-val-range", "1500"));
    }

    /**
     * Metastore
     */
    public boolean needReplaceAggWhenExactlyMatched() {
        return Boolean.parseBoolean(config.getOptional("kylin.query.engine.need-replace-agg", "true"));
    }

    /**
     * health
     */

    public int getMetaStoreHealthWarningResponseMs() {
        return Integer.parseInt(config.getOptional("kylin.health.metastore-warning-response-ms", "300"));
    }

    public int getMetaStoreHealthErrorResponseMs() {
        return Integer.parseInt(config.getOptional("kylin.health.metastore-error-response-ms", "1000"));
    }

    /**
     * Diagnosis graph
     */

    public String influxdbAddress() {
        return config.getOptional("kylin.influxdb.address", "localhost:8086");
    }

    public String influxdbUsername() {
        return config.getOptional("kylin.influxdb.username", "root");
    }

    public String influxdbPassword() {
        String password = config.getOptional("kylin.influxdb.password", "root");
        if (EncryptUtil.isEncrypted(password)) {
            password = EncryptUtil.decryptPassInKylin(password);
        }
        return password;
    }

    public boolean isInfluxdbHttpsEnabled() {
        return Boolean.parseBoolean(config.getOptional("kylin.influxdb.https.enabled", FALSE));
    }

    public boolean isInfluxdbUnsafeSslEnabled() {
        return Boolean.parseBoolean(config.getOptional("kylin.influxdb.https.unsafe-ssl.enabled", TRUE));
    }

    public int getInfluxDBFlushDuration() {
        return Integer.parseInt(config.getOptional("kylin.influxdb.flush-duration", "3000"));
    }

    public String sparderJars() {
        try {
            File storageFile = FileUtils.findFile(KylinConfigBase.getKylinHome() + "/lib", "newten-job.jar");
            String path1 = "";
            if (storageFile != null) {
                path1 = storageFile.getCanonicalPath();
            }

            return config.getOptional("kylin.query.engine.sparder-additional-jars", path1);
        } catch (IOException e) {
            return "";
        }
    }

    /**
     * kap metrics, default: influxDb
     */
    public String getMetricsDbNameWithMetadataUrlPrefix() {
        StringBuilder sb = new StringBuilder(config.getMetadataUrlPrefix());
        sb.append("_");
        sb.append(config.getOptional("kylin.metrics.influx-db", "KE_METRICS"));
        return sb.toString();
    }

    public String getDailyMetricsDbNameWithMetadataUrlPrefix() {
        StringBuilder sb = new StringBuilder(config.getMetadataUrlPrefix());
        sb.append("_");
        sb.append(config.getOptional("kylin.metrics.daily-influx-db", "KE_METRICS_DAILY"));
        return sb.toString();
    }

    public String getMetricsRpcServiceBindAddress() {
        return config.getOptional("kylin.metrics.influx-rpc-service-bind-address", "127.0.0.1:8088");
    }

    public int getMetricsPollingIntervalSecs() {
        return Integer.parseInt(config.getOptional("kylin.metrics.polling-interval-secs", "60"));
    }

    public int getDailyMetricsRunHour() {
        return Integer.parseInt(config.getOptional("kylin.metrics.daily-run-hour", "1"));
    }

    public int getDailyMetricsMaxRetryTimes() {
        return Integer.parseInt(config.getOptional("kylin.metrics.daily-max-retry-times", "3"));
    }

    /**
     * kap monitor
     */
    public Boolean isMonitorEnabled() {
        return Boolean.parseBoolean(config.getOptional("kylin.monitor.enabled", "true"));
    }

    public String getMonitorDatabase() {
        return String.valueOf(config.getOptional("kylin.monitor.db", "KE_MONITOR"));
    }

    public String getMonitorRetentionPolicy() {
        return String.valueOf(config.getOptional("kylin.monitor.retention-policy", "KE_MONITOR_RP"));
    }

    public String getMonitorRetentionDuration() {
        return String.valueOf(config.getOptional("kylin.monitor.retention-duration", "90d"));
    }

    public String getMonitorShardDuration() {
        return String.valueOf(config.getOptional("kylin.monitor.shard-duration", "7d"));
    }

    public Integer getMonitorReplicationFactor() {
        return Integer.parseInt(config.getOptional("kylin.monitor.replication-factor", "1"));
    }

    public Boolean isMonitorUserDefault() {
        return Boolean.parseBoolean(config.getOptional("kylin.monitor.user-default", "true"));
    }

    public Long getMonitorInterval() {
        return Long.parseLong(config.getOptional("kylin.monitor.interval", "60")) * 1000;
    }

    public long getJobStatisticInterval() {
        return Long.parseLong(config.getOptional("kylin.monitor.job-statistic-interval", "3600")) * 1000;
    }

    public long getMaxPendingErrorJobs() {
        return Long.parseLong(config.getOptional("kylin.monitor.job-pending-error-total", "20"));
    }

    public double getMaxPendingErrorJobsRation() {
        double ration = Double.parseDouble(config.getOptional("kylin.monitor.job-pending-error-rate", "0.2"));
        if (ration <= 0 || ration >= 1) {
            return 0.2;
        }
        return ration;
    }

    public double getClusterCrashThreshhold() {
        return Double.parseDouble(config.getOptional("kylin.monitor.cluster-crash-threshold", "0.8"));
    }

    /**
     * kap circuit-breaker
     */
    public Boolean isCircuitBreakerEnabled() {
        return Boolean.parseBoolean(config.getOptional("kylin.circuit-breaker.enabled", "true"));
    }

    public int getCircuitBreakerThresholdOfProject() {
        return Integer.parseInt(config.getOptional("kylin.circuit-breaker.threshold.project", "100"));
    }

    public int getCircuitBreakerThresholdOfModel() {
        return Integer.parseInt(config.getOptional("kylin.circuit-breaker.threshold.model", "100"));
    }

    public int getCircuitBreakerThresholdOfFavoriteQuery() {
        return Integer.parseInt(config.getOptional("kylin.circuit-breaker.threshold.fq", CIRCUIT_BREAKER_THRESHOLD));
    }

    public int getCircuitBreakerThresholdOfSqlPatternToBlacklist() {
        return Integer.parseInt(config.getOptional("kylin.circuit-breaker.threshold.sql-pattern-to-blacklist",
                CIRCUIT_BREAKER_THRESHOLD));
    }

    public long getCircuitBreakerThresholdOfQueryResultRowCount() {
        return Long.parseLong(config.getOptional("kylin.circuit-breaker.threshold.query-result-row-count", "2000000"));
    }

    // log rotate
    public int getMaxKeepLogFileNumber() {
        return Integer.parseInt(config.getOptional("kylin.env.max-keep-log-file-number", "10"));
    }

    public int getMaxKeepLogFileThresholdMB() {
        return Integer.parseInt(config.getOptional("kylin.env.max-keep-log-file-threshold-mb", "256"));
    }

    public String sparderFiles() {
        try {
            File storageFile = new File(getKylinConfig().getLogSparkExecutorPropertiesFile());
            String additionalFiles = storageFile.getCanonicalPath();
            storageFile = new File(getKylinConfig().getLogSparkAppMasterPropertiesFile());
            if (additionalFiles.isEmpty()) {
                additionalFiles = storageFile.getCanonicalPath();
            } else {
                additionalFiles = additionalFiles + "," + storageFile.getCanonicalPath();
            }
            return config.getOptional("kylin.query.engine.sparder-additional-files", additionalFiles);
        } catch (IOException e) {
            return "";
        }
    }

    public String getAsyncResultBaseDir(String project) {
        return config.getOptional("kylin.query.engine.sparder-asyncresult-base-dir",
                KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory(project) + "/async_query_result");
    }

    /**
     * Newten
     */
    public String getCuboidSpanningTree() {
        return config.getOptional("kylin.cube.cuboid-spanning-tree",
                "org.apache.kylin.metadata.cube.cuboid.NForestSpanningTree");
    }

    public String getIntersectCountSeparator() {
        return config.getOptional("kylin.cube.intersect-count-array-separator", "|");
    }

    public boolean enableQueryPattern() {
        return Boolean.parseBoolean(config.getOptional("kylin.query.favorite.collect-as-pattern", "true"));
    }

    public boolean splitGroupSetsIntoUnion() {
        return Boolean.parseBoolean(config.getOptional("kylin.query.engine.split-group-sets-into-union", TRUE));
    }

    public int defaultDecimalScale() {
        return Integer.parseInt(config.getOptional("kylin.query.engine.default-decimal-scale", "0"));
    }

    public boolean enablePushdownPrepareStatementWithParams() {
        return Boolean.parseBoolean(
                config.getOptional("kylin.query.engine.push-down.enable-prepare-statement-with-params", FALSE));
    }

    public boolean enableReplaceDynamicParams() {
        return Boolean.parseBoolean(config.getOptional("kylin.query.replace-dynamic-params-enabled", FALSE));
    }

    public boolean runConstantQueryLocally() {
        return Boolean.parseBoolean(config.getOptional("kylin.query.engine.run-constant-query-locally", TRUE));
    }

    public boolean isRecordSourceUsage() {
        return Boolean.parseBoolean(config.getOptional("kylin.source.record-source-usage-enabled", "true"));
    }

    public boolean isSourceUsageUnwrapComputedColumn() {
        return Boolean
                .parseBoolean(config.getOptional("kylin.metadata.history-source-usage-unwrap-computed-column", TRUE));
    }

    /**
     * Kerberos
     */

    public Boolean isKerberosEnabled() {
        return Boolean.parseBoolean(config.getOptional("kylin.kerberos.enabled", FALSE));
    }

    public String getKerberosKeytab() {
        return config.getOptional("kylin.kerberos.keytab", "");
    }

    public String getKerberosKeytabPath() {
        return KylinConfig.getKylinConfDir() + File.separator + getKerberosKeytab();
    }

    public String getKerberosZKPrincipal() {
        return config.getOptional("kylin.kerberos.zookeeper-server-principal", "zookeeper/hadoop");
    }

    public Long getKerberosTicketRefreshInterval() {
        return Long.parseLong(config.getOptional("kylin.kerberos.ticket-refresh-interval-minutes", "720"));
    }

    public Long getKerberosMonitorInterval() {
        return Long.parseLong(config.getOptional("kylin.kerberos.monitor-interval-minutes", "10"));
    }

    public String getKerberosPlatform() {
        return config.getOptional("kylin.kerberos.platform", "");
    }

    public Boolean getPlatformZKEnable() {
        return Boolean.parseBoolean(config.getOptional("kylin.env.zk-kerberos-enabled",
                config.getOptional("kylin.kerberos.enabled", FALSE)));
    }

    public String getKerberosKrb5Conf() {
        return config.getOptional("kylin.kerberos.krb5-conf", "krb5.conf");
    }

    public String getKerberosKrb5ConfPath() {
        return KylinConfig.getKylinConfDir() + File.separator + getKerberosKrb5Conf();
    }

    public String getKerberosJaasConf() {
        return config.getOptional("kylin.kerberos.jaas-conf", "jaas.conf");
    }

    public String getKerberosJaasConfPath() {
        return KylinConfig.getKylinConfDir() + File.separator + getKerberosJaasConf();
    }

    public String getKafkaJaasConf() {
        return config.getOptional("kylin.kafka-jaas-conf", "kafka_jaas.conf");
    }

    public String getKafkaJaasConfPath() {
        return KylinConfig.getKylinConfDir() + File.separator + getKafkaJaasConf();
    }

    public boolean isKafkaJaasEnabled() {
        return Boolean.parseBoolean(config.getOptional("kylin.kafka-jaas.enabled", FALSE));
    }

    public String getKerberosPrincipal() {
        return config.getOptional("kylin.kerberos.principal");
    }

    public int getThresholdToRestartSpark() {
        return Integer.parseInt(config.getOptional("kylin.canary.sqlcontext-threshold-to-restart-spark", "3"));
    }

    public int getSparkCanaryErrorResponseMs() {
        return Integer.parseInt(config.getOptional("kylin.canary.sqlcontext-error-response-ms", HALF_MINUTE_MS));
    }

    public int getSparkCanaryPeriodMinutes() {
        return Integer.parseInt(config.getOptional("kylin.canary.sqlcontext-period-min", "3"));
    }

    public String getSparkCanaryType() {
        return config.getOptional("kylin.canary.sqlcontext-type", "file");
    }

    public Boolean getSparkCanaryEnable() {
        return Boolean.parseBoolean(config.getOptional("kylin.canary.sqlcontext-enabled", FALSE));
    }

    public double getJoinMemoryFraction() {
        // driver memory that can be used by join(mostly BHJ)
        return Double.parseDouble(config.getOptional("kylin.query.join-memory-fraction", "0.3"));
    }

    public int getMonitorSparkPeriodSeconds() {
        return Integer.parseInt(config.getOptional("kylin.storage.monitor-spark-period-seconds", "30"));
    }

    public boolean isQueryEscapedLiteral() {
        return Boolean.parseBoolean(config.getOptional("kylin.query.parser.escaped-string-literals", FALSE));
    }

    public boolean isAESkewJoinEnabled() {
        return Boolean.parseBoolean(config.getOptional("kylin.build.ae.skew-join-enabled", TRUE));
    }

    public boolean optimizeShardEnabled() {
        return Boolean.parseBoolean(config.getOptional("kylin.build.optimize-shard-enabled", TRUE));
    }

    public String getSwitchBackupFsExceptionAllowString() {
        return config.getOptional("kylin.query.switch-backup-fs-exception-allow-string", "alluxio");
    }

    public boolean isQuerySparkJobTraceEnabled() {
        return Boolean.parseBoolean(config.getOptional("kylin.query.spark-job-trace-enabled", TRUE));
    }

    public boolean isOnlyPlanInSparkEngine() {
        return Boolean.parseBoolean(config.getOptional("kylin.query.only-plan-with-spark-engine", FALSE));
    }

    public long getSparkJobTraceTimeoutMs() {
        return Long.parseLong(config.getOptional("kylin.query.spark-job-trace-timeout-ms", "8000"));
    }

    public int getSparkJobTraceCacheMax() {
        return Integer.parseInt(config.getOptional("kylin.query.spark-job-trace-cache-max", "1000"));
    }

    public int getSparkJobTraceParallelMax() {
        return Integer.parseInt(config.getOptional("kylin.query.spark-job-trace-parallel-max", "50"));
    }

    public long getBigQuerySourceScanRowsThreshold() {
        return Long.parseLong(config.getOptional("kylin.query.big-query-source-scan-rows-threshold", "-1"));
    }

    public String getShareStateSwitchImplement() {
        return config.getOptional("kylin.query.share-state-switch-implement", "close");
    }

    public boolean isQueryLimitEnabled() {
        return Boolean.parseBoolean(config.getOptional("kylin.query.query-limit-enabled", FALSE));
    }

    public boolean isApplyLimitInfoToSourceScanRowsEnabled() {
        return Boolean.parseBoolean(config.getOptional("kylin.query.apply-limit-info-to-source-scan-rows-enabled", FALSE));
    }

    public boolean isAutoAdjustBigQueryRowsThresholdEnabled() {
        return Boolean.parseBoolean(config.getOptional("kylin.query.auto-adjust-big-query-rows-threshold-enabled", FALSE));
    }

    public long getBigQuerySecond() {
        return Long.parseLong(config.getOptional("kylin.query.big-query-second", "10"));
    }

    public long getBigQueryThresholdUpdateIntervalSecond() {
        return Long.parseLong(config.getOptional("kylin.query.big-query-threshold-update-interval-second", "10800"));
    }

}
