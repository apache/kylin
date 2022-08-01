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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

@MetadataInfo(onlyProps = true)
public class KapConfigTest {

    private static final Map<String, PropertiesEntity> map = new HashMap<>();

    {
        map.put("getParquetReadFileSystem", new PropertiesEntity("kylin.storage.columnar.file-system", "", ""));

        map.put("getParquetSparkExecutorInstance", new PropertiesEntity(
                "kylin.storage.columnar.spark-conf.spark.executor.instances", String.valueOf(1), 1));

        map.put("getParquetSparkExecutorCore",
                new PropertiesEntity("kylin.storage.columnar.spark-conf.spark.executor.cores", String.valueOf(1), 1));

        map.put("getMinBucketsNumber", new PropertiesEntity("kylin.storage.columnar.bucket-num", "1", 1));

        map.put("getParquetStorageShardSizeMB",
                new PropertiesEntity("kylin.storage.columnar.shard-size-mb", "128", 128));

        map.put("getParquetStorageShardSizeRowCount",
                new PropertiesEntity("kylin.storage.columnar.shard-rowcount", "2500000", 2500000L));

        map.put("getParquetStorageCountDistinctShardSizeRowCount",
                new PropertiesEntity("kylin.storage.columnar.shard-countdistinct-rowcount", "1000000", 1000000L));

        map.put("getParquetStorageRepartitionThresholdSize",
                new PropertiesEntity("kylin.storage.columnar.repartition-threshold-size-mb", "128", 128));

        map.put("isProjectInternalDefaultPermissionGranted",
                new PropertiesEntity("kylin.acl.project-internal-default-permission-granted", "true", true));

        map.put("getMassinResourceIdentiferDir",
                new PropertiesEntity("kylin.server.massin-resource-dir", "/massin", "/massin"));

        map.put("getDBAccessFilterEnable",
                new PropertiesEntity("kylin.source.hive.database-access-filter-enabled", "true", true));

        map.put("getDiagPackageTimeout", new PropertiesEntity("kylin.diag.package.timeout-seconds", "3600", 3600L));

        map.put("getExtractionStartTimeDays", new PropertiesEntity("kylin.diag.extraction.start-time-days", "3", 3));

        map.put("getKyAccountUsename", new PropertiesEntity("kylin.kyaccount.username", "ADMIN", "ADMIN"));
        map.put("getKyAccountPassword", new PropertiesEntity("kylin.kyaccount.password", "KYLIN", "KYLIN"));
        map.put("getKyAccountToken", new PropertiesEntity("kylin.kyaccount.token", "ADMIN", "ADMIN"));

        map.put("getKyAccountSSOUrl",
                new PropertiesEntity("kylin.kyaccount.url", "https://sso.kyligence.com", "https://sso.kyligence.com"));

        map.put("getKyAccountSiteUrl", new PropertiesEntity("kylin.kyaccount.site-url", "https://account.kyligence.io",
                "https://account.kyligence.io"));

        map.put("getChannelUser", new PropertiesEntity("kylin.env.channel", "on-premises", "on-premises"));

        map.put("isImplicitComputedColumnConvertEnabled",
                new PropertiesEntity("kylin.query.implicit-computed-column-convert", "true", true));

        map.put("isAggComputedColumnRewriteEnabled",
                new PropertiesEntity("kylin.query.agg-computed-column-rewrite", "true", true));

        map.put("getComputedColumnMaxRecursionTimes",
                new PropertiesEntity("kylin.query.computed-column-max-recursion-times", "10", 10));

        map.put("isJdbcEscapeEnabled", new PropertiesEntity("kylin.query.jdbc-escape-enabled", "true", true));

        map.put("getListenerBusBusyThreshold",
                new PropertiesEntity("kylin.query.engine.spark-listenerbus-busy-threshold", "5000", 5000));

        map.put("getBlockNumBusyThreshold",
                new PropertiesEntity("kylin.query.engine.spark-blocknum-busy-threshold", "5000", 5000));

        map.put("getSparkSqlShufflePartitions",
                new PropertiesEntity("kylin.query.engine.spark-sql-shuffle-partitions", "-1", -1));

        map.put("getLDAPUserSearchFilter", new PropertiesEntity("kylin.security.ldap.user-search-filter",
                "(objectClass=person)", "(objectClass=person)"));

        map.put("getLDAPGroupSearchFilter",
                new PropertiesEntity("kylin.security.ldap.group-search-filter",
                        "(|(objectClass=groupOfNames)(objectClass=group))",
                        "(|(objectClass=groupOfNames)(objectClass=group))"));

        map.put("getLDAPGroupMemberSearchFilter", new PropertiesEntity("kylin.security.ldap.group-member-search-filter",
                "(&(cn={0})(objectClass=groupOfNames))", "(&(cn={0})(objectClass=groupOfNames))"));

        map.put("getLDAPMaxPageSize", new PropertiesEntity("kylin.security.ldap.max-page-size", "1000", 1000));

        map.put("getLDAPMaxValRange", new PropertiesEntity("kylin.security.ldap.max-val-range", "1500", 1500));

        map.put("getLDAPUserIDAttr", new PropertiesEntity("kylin.security.ldap.user-identifier-attr", "cn", "cn"));

        map.put("getLDAPGroupIDAttr", new PropertiesEntity("kylin.security.ldap.group-identifier-attr", "cn", "cn"));

        map.put("getLDAPGroupMemberAttr",
                new PropertiesEntity("kylin.security.ldap.group-member-attr", "member", "member"));

        map.put("needReplaceAggWhenExactlyMatched",
                new PropertiesEntity("kylin.query.engine.need-replace-agg", "true", true));

        map.put("getMetaStoreHealthWarningResponseMs",
                new PropertiesEntity("kylin.health.metastore-warning-response-ms", "300", 300));

        map.put("getMetaStoreHealthErrorResponseMs",
                new PropertiesEntity("kylin.health.metastore-error-response-ms", "1000", 1000));

        map.put("influxdbAddress", new PropertiesEntity("kylin.influxdb.address", "localhost:8086", "localhost:8086"));

        map.put("influxdbUsername", new PropertiesEntity("kylin.influxdb.username", "root", "root"));

        map.put("influxdbPassword", new PropertiesEntity("kylin.influxdb.password", "root", "root"));

        map.put("getInfluxDBFlushDuration", new PropertiesEntity("kylin.influxdb.flush-duration", "3000", 3000));

        map.put("getMetricsRpcServiceBindAddress", new PropertiesEntity("kylin.metrics.influx-rpc-service-bind-address",
                "127.0.0.1:8088", "127.0.0.1:8088"));

        map.put("getMetricsPollingIntervalSecs", new PropertiesEntity("kylin.metrics.polling-interval-secs", "60", 60));

        map.put("getDailyMetricsRunHour", new PropertiesEntity("kylin.metrics.daily-run-hour", "1", 1));

        map.put("getDailyMetricsMaxRetryTimes", new PropertiesEntity("kylin.metrics.daily-max-retry-times", "3", 3));

        map.put("isMonitorEnabled", new PropertiesEntity("kylin.monitor.enabled", "true", true));

        map.put("getMonitorDatabase", new PropertiesEntity("kylin.monitor.db", "KE_MONITOR", "KE_MONITOR"));

        map.put("getMonitorRetentionPolicy",
                new PropertiesEntity("kylin.monitor.retention-policy", "KE_MONITOR_RP", "KE_MONITOR_RP"));

        map.put("getMonitorRetentionDuration", new PropertiesEntity("kylin.monitor.retention-duration", "90d", "90d"));

        map.put("getMonitorShardDuration", new PropertiesEntity("kylin.monitor.shard-duration", "7d", "7d"));

        map.put("getMonitorReplicationFactor", new PropertiesEntity("kylin.monitor.replication-factor", "1", 1));

        map.put("isMonitorUserDefault", new PropertiesEntity("kylin.monitor.user-default", "true", true));

        map.put("getMonitorInterval", new PropertiesEntity("kylin.monitor.interval", "60", 60 * 1000L));

        map.put("getJobStatisticInterval",
                new PropertiesEntity("kylin.monitor.job-statistic-interval", "3600", 3600 * 1000L));

        map.put("getMaxPendingErrorJobs", new PropertiesEntity("kylin.monitor.job-pending-error-total", "20", 20L));

        map.put("getMaxPendingErrorJobsRation",
                new PropertiesEntity("kylin.monitor.job-pending-error-rate", "0.2", 0.2));

        map.put("getClusterCrashThreshhold", new PropertiesEntity("kylin.monitor.cluster-crash-threshold", "0.8", 0.8));

        map.put("isCircuitBreakerEnabled", new PropertiesEntity("kylin.circuit-breaker.enabled", "true", true));

        map.put("getCircuitBreakerThresholdOfProject",
                new PropertiesEntity("kylin.circuit-breaker.threshold.project", "100", 100));

        map.put("getCircuitBreakerThresholdOfModel",
                new PropertiesEntity("kylin.circuit-breaker.threshold.model", "100", 100));

        map.put("getCircuitBreakerThresholdOfFavoriteQuery",
                new PropertiesEntity("kylin.circuit-breaker.threshold.fq", "30000", 30000));

        map.put("getCircuitBreakerThresholdOfSqlPatternToBlacklist",
                new PropertiesEntity("kylin.circuit-breaker.threshold.sql-pattern-to-blacklist", "30000", 30000));

        map.put("getCircuitBreakerThresholdOfQueryResultRowCount",
                new PropertiesEntity("kylin.circuit-breaker.threshold.query-result-row-count", "2000000", 2000000L));

        map.put("getMaxKeepLogFileNumber", new PropertiesEntity("kylin.env.max-keep-log-file-number", "10", 10));

        map.put("getMaxKeepLogFileThresholdMB",
                new PropertiesEntity("kylin.env.max-keep-log-file-threshold-mb", "256", 256));

        map.put("getCuboidSpanningTree",
                new PropertiesEntity("kylin.cube.cuboid-spanning-tree",
                        "org.apache.kylin.metadata.cube.cuboid.NForestSpanningTree",
                        "org.apache.kylin.metadata.cube.cuboid.NForestSpanningTree"));

        map.put("getIntersectCountSeparator",
                new PropertiesEntity("kylin.cube.intersect-count-array-separator", "|", "|"));

        map.put("enableQueryPattern", new PropertiesEntity("kylin.query.favorite.collect-as-pattern", "true", true));

        map.put("splitGroupSetsIntoUnion",
                new PropertiesEntity("kylin.query.engine.split-group-sets-into-union", "true", true));

        map.put("defaultDecimalScale", new PropertiesEntity("kylin.query.engine.default-decimal-scale", "0", 0));

        map.put("enablePushdownPrepareStatementWithParams", new PropertiesEntity(
                "kylin.query.engine.push-down.enable-prepare-statement-with-params", "false", false));

        map.put("runConstantQueryLocally",
                new PropertiesEntity("kylin.query.engine.run-constant-query-locally", "true", true));

        map.put("isKerberosEnabled", new PropertiesEntity("kylin.kerberos.enabled", "false", false));

        map.put("getKerberosKeytab", new PropertiesEntity("kylin.kerberos.keytab", "", ""));

        map.put("getKerberosZKPrincipal", new PropertiesEntity("kylin.kerberos.zookeeper-server-principal",
                "zookeeper/hadoop", "zookeeper/hadoop"));

        map.put("getKerberosTicketRefreshInterval",
                new PropertiesEntity("kylin.kerberos.ticket-refresh-interval-minutes", "720", 720L));

        map.put("getKerberosMonitorInterval",
                new PropertiesEntity("kylin.kerberos.monitor-interval-minutes", "10", 10L));

        map.put("getKerberosPlatform", new PropertiesEntity("kylin.kerberos.platform", "", ""));

        map.put("getPlatformZKEnable", new PropertiesEntity("kylin.env.zk-kerberos-enabled", "false", false));

        map.put("getKerberosKrb5Conf", new PropertiesEntity("kylin.kerberos.krb5-conf", "krb5.conf", "krb5.conf"));

        map.put("getKerberosJaasConf", new PropertiesEntity("kylin.kerberos.jaas-conf", "jaas.conf", "jaas.conf"));

        map.put("getKerberosPrincipal", new PropertiesEntity("kylin.kerberos.principal", "kylin", "kylin"));

        map.put("getThresholdToRestartSpark",
                new PropertiesEntity("kylin.canary.sqlcontext-threshold-to-restart-spark", "3", 3));

        map.put("getSparkCanaryErrorResponseMs",
                new PropertiesEntity("kylin.canary.sqlcontext-error-response-ms", "30000", 30000));

        map.put("getSparkCanaryPeriodMinutes", new PropertiesEntity("kylin.canary.sqlcontext-period-min", "3", 3));

        map.put("getJoinMemoryFraction", new PropertiesEntity("kylin.query.join-memory-fraction", "0.3", 0.3));

        map.put("getMonitorSparkPeriodSeconds",
                new PropertiesEntity("kylin.storage.monitor-spark-period-seconds", "30", 30));
        map.put("isRecordSourceUsage", new PropertiesEntity("kylin.source.record-source-usage-enabled", "true", true));

        map.put("isInfluxdbHttpsEnabled", new PropertiesEntity("kylin.influxdb.https.enabled", "false", false));
        map.put("isInfluxdbUnsafeSslEnabled",
                new PropertiesEntity("kylin.influxdb.https.unsafe-ssl.enabled", "true", true));
        map.put("isOnlyPlanInSparkEngine",
                new PropertiesEntity("kylin.query.only-plan-with-spark-engine", "false", false));
        map.put("getSparkJobTraceTimeoutMs",
                new PropertiesEntity("kylin.query.spark-job-trace-timeout-ms", "8000", 8000L));
        map.put("getSparkJobTraceCacheMax",
                new PropertiesEntity("kylin.query.spark-job-trace-cache-max", "1000", 1000));
        map.put("getSparkJobTraceParallelMax",
                new PropertiesEntity("kylin.query.spark-job-trace-parallel-max", "50", 50));
        map.put("isSourceUsageUnwrapComputedColumn",
                new PropertiesEntity("kylin.metadata.history-source-usage-unwrap-computed-column", "true", true));
    }

    @Test
    public void test() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        Class<? extends KapConfig> configClass = kapConfig.getClass();

        for (Map.Entry<String, PropertiesEntity> entry : map.entrySet()) {
            String func = entry.getKey();
            PropertiesEntity propertiesEntity = entry.getValue();
            Method method = configClass.getDeclaredMethod(func);
            Assert.assertNotNull(method);
            kapConfig.getKylinConfig().setProperty(propertiesEntity.getKey(), propertiesEntity.getValue());
            Object invoke = method.invoke(kapConfig);
            if (invoke != null && invoke.getClass().isArray()) {
                Class<?> componentType = invoke.getClass().getComponentType();
                if (componentType.isPrimitive()) {
                    switch (componentType.getName()) {
                    case "int":
                        Assert.assertArrayEquals((int[]) propertiesEntity.getExpectValue(), (int[]) invoke);
                        break;
                    default:
                        /// just implement it
                        Assert.fail();
                    }
                } else {
                    Assert.assertArrayEquals((Object[]) propertiesEntity.getExpectValue(), (Object[]) invoke);
                }
            } else {
                Assert.assertEquals(propertiesEntity.getExpectValue(), invoke);
            }
        }
    }

}
