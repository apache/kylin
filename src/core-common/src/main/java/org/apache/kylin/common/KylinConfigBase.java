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

import static java.lang.Math.toIntExact;
import static org.apache.kylin.common.constant.AsyncProfilerConstants.ASYNC_PROFILER_LIB_LINUX_ARM64;
import static org.apache.kylin.common.constant.AsyncProfilerConstants.ASYNC_PROFILER_LIB_LINUX_X64;
import static org.apache.kylin.common.constant.Constants.KYLIN_SOURCE_JDBC_CONNECTION_URL_KEY;
import static org.apache.kylin.common.constant.Constants.KYLIN_SOURCE_JDBC_DRIVER_KEY;
import static org.apache.kylin.common.constant.Constants.KYLIN_SOURCE_JDBC_PASS_KEY;
import static org.apache.kylin.common.constant.Constants.KYLIN_SOURCE_JDBC_SOURCE_ENABLE_KEY;
import static org.apache.kylin.common.constant.Constants.KYLIN_SOURCE_JDBC_SOURCE_NAME_KEY;
import static org.apache.kylin.common.constant.Constants.KYLIN_SOURCE_JDBC_USER_KEY;
import static org.apache.kylin.common.constant.Constants.SNAPSHOT_AUTO_REFRESH;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.annotation.ThirdPartyDependencies;
import org.apache.kylin.common.constant.NonCustomProjectLevelConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.lock.DistributedLockFactory;
import org.apache.kylin.common.persistence.metadata.HDFSMetadataStore;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.ByteUnit;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.ClusterConstant;
import org.apache.kylin.common.util.CompositeMapView;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.common.util.FileUtils;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.SizeConvertUtil;
import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.config.core.loader.IExternalConfigLoader;
import lombok.val;

/**
 * An abstract class to encapsulate access to a set of 'properties'.
 * Subclass can override methods in this class to extend the content of the 'properties',
 * with some override values for example.
 */
public abstract class KylinConfigBase implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(KylinConfigBase.class);

    protected static final String WORKING_DIR_PROP = "kylin.env.hdfs-working-dir";
    protected static final String DATA_WORKING_DIR_PROP = "kylin.env.hdfs-data-working-dir";
    protected static final String KYLIN_ROOT = "/kylin";
    public static final String WRITING_CLUSTER_WORKING_DIR = "kylin.env.hdfs-write-working-dir";

    public static final long REJECT_SIMILARITY_THRESHOLD = 100_000_000L;
    public static final double SIMILARITY_THRESHOLD = 0.9;

    public static final long MINUTE = 60;

    private static final String ONE_HUNDRED_THOUSAND = "100000";

    public static final String DEFAULT = "default";
    public static final String TRUE = "true";
    public static final String FALSE = "false";
    public static final String QUERY_NODE = "query";
    public static final String PATH_DELIMITER = "/";

    public static final String DIAG_ID_PREFIX = "front_";

    public static final String POWER_BI_CONVERTER = "org.apache.kylin.query.util.PowerBIConverter";
    public static final String KYLIN_STREAMING_STATS_URL = "kylin.streaming.stats.url";
    public static final String KYLIN_QUERY_HISTORY_URL = "kylin.query.queryhistory.url";
    public static final String KYLIN_METADATA_DISTRIBUTED_LOCK_JDBC_URL = "kylin.metadata.distributed-lock.jdbc.url";

    private static final String METRICS = "_metrics/";

    protected static final Map<String, String> STATIC_SYSTEM_ENV = new ConcurrentHashMap<>(System.getenv());

    /*
     * DON'T DEFINE CONSTANTS FOR PROPERTY KEYS!
     *
     * For 1), no external need to access property keys, all accesses are by public methods.
     * For 2), it's cumbersome to maintain constants at top and code at bottom.
     * For 3), key literals usually appear only once.
     */

    public static String getKylinHome() {
        String kylinHome = getKylinHomeWithoutWarn();
        if (StringUtils.isEmpty(kylinHome)) {
            logger.warn("KYLIN_HOME was not set");
        }
        return kylinHome;
    }

    public static String getKylinHomeWithoutWarn() {
        String kylinHome = System.getenv("KYLIN_HOME");
        if (StringUtils.isEmpty(kylinHome)) {
            kylinHome = SystemPropertiesCache.getProperty("KYLIN_HOME");
        }
        return kylinHome;
    }

    public static String getKylinConfHome() {
        String confHome = System.getenv("KYLIN_CONF");
        if (StringUtils.isEmpty(confHome)) {
            confHome = SystemPropertiesCache.getProperty("KYLIN_CONF");
        }
        return confHome;
    }

    public static String getSparkHome() {
        String sparkHome = System.getenv("SPARK_HOME");
        if (StringUtils.isNotEmpty(sparkHome)) {
            return sparkHome;
        }

        return getKylinHome() + File.separator + "spark";
    }

    public Map<String, String> getReadonlyProperties() {
        val subStitutorTmp = getSubstitutor();
        HashMap<String, String> config = Maps.newHashMap();
        for (Entry<Object, Object> entry : this.properties.entrySet()) {
            config.put((String) entry.getKey(), subStitutorTmp.replace((String) entry.getValue()));
        }
        return config;
    }

    // backward compatibility check happens when properties is loaded or updated
    static BackwardCompatibilityConfig BCC = new BackwardCompatibilityConfig();

    // ============================================================================

    /**
     * only reload properties
     */
    final PropertiesDelegate properties;
    final transient StrSubstitutor substitutor;

    protected KylinConfigBase(IExternalConfigLoader configLoader) {
        this(new Properties(), configLoader);
    }

    protected KylinConfigBase(Properties props, IExternalConfigLoader configLoader) {
        this(props, false, configLoader);
    }

    @SuppressWarnings("rawtypes")
    protected KylinConfigBase(Properties props, boolean force, IExternalConfigLoader configLoader) {
        if (props instanceof PropertiesDelegate) {
            this.properties = (PropertiesDelegate) props;
        } else {
            this.properties = force ? new PropertiesDelegate(props, configLoader)
                    : new PropertiesDelegate(BCC.check(props), configLoader);
        }
        // env > properties
        this.substitutor = new StrSubstitutor(new CompositeMapView(this.properties, STATIC_SYSTEM_ENV));
    }

    protected final String getOptional(String prop) {
        return getOptional(prop, null);
    }

    protected String getOptional(String prop, String dft) {
        final String property = SystemPropertiesCache.getProperty(prop);
        return property != null ? getSubstitutor().replace(property)
                : getSubstitutor().replace(properties.getProperty(prop, dft));
    }

    protected Properties getAllProperties() {
        return getProperties(null);
    }

    /**
     * @param propertyKeys the collection of the properties; if null will return all properties
     * @return
     */
    protected Properties getProperties(Collection<String> propertyKeys) {
        val subStitutorTmp = getSubstitutor();

        Properties result = new Properties();
        for (Entry<Object, Object> entry : this.properties.entrySet()) {
            if (propertyKeys == null || propertyKeys.contains(entry.getKey())) {
                result.put(entry.getKey(), subStitutorTmp.replace((String) entry.getValue()));
            }
        }

        return result;
    }

    protected StrSubstitutor getSubstitutor() {
        return substitutor;
    }

    protected Properties getRawAllProperties() {
        return properties;
    }

    protected final Map<String, String> getPropertiesByPrefix(String prefix) {
        Map<String, String> result = Maps.newLinkedHashMap();
        for (Entry<Object, Object> entry : getAllProperties().entrySet()) {
            String key = (String) entry.getKey();
            if (key.startsWith(prefix)) {
                result.put(key.substring(prefix.length()), (String) entry.getValue());
            }
        }
        for (Entry<Object, Object> entry : SystemPropertiesCache.getProperties().entrySet()) {
            String key = (String) entry.getKey();
            if (key.startsWith(prefix)) {
                result.put(key.substring(prefix.length()), (String) entry.getValue());
            }
        }
        return result;
    }

    protected final String[] getOptionalStringArray(String prop, String[] dft) {
        final String property = getOptional(prop);
        if (!StringUtils.isBlank(property)) {
            return Arrays.stream(property.split(",")).map(String::trim).toArray(String[]::new);
        } else {
            return dft;
        }
    }

    protected final String[] getSystemStringArray(String prop, String[] dft) {
        final String property = SystemPropertiesCache.getProperty(prop);
        if (!StringUtils.isBlank(property)) {
            return Arrays.stream(property.split(",")).map(String::trim).toArray(String[]::new);
        } else {
            return dft;
        }
    }

    protected final int[] getOptionalIntArray(String prop, String[] dft) {
        String[] strArray = getOptionalStringArray(prop, dft);
        int[] intArray = new int[strArray.length];
        for (int i = 0; i < strArray.length; i++) {
            intArray[i] = Integer.parseInt(strArray[i]);
        }
        return intArray;
    }

    protected final String getRequired(String prop) {
        String r = getOptional(prop);
        if (StringUtils.isEmpty(r)) {
            throw new IllegalArgumentException("missing '" + prop + "' in conf/kylin.properties");
        }
        return r;
    }

    /**
     * Use with care, properties should be read-only. This is for testing only.
     */
    public final void setProperty(String key, String value) {
        logger.trace("KylinConfig was updated with {}={}", key, value);
        properties.setProperty(BCC.check(key), value);
    }

    protected final void reloadKylinConfig(Properties properties) {
        this.properties.reloadProperties(BCC.check(properties));
        setProperty("kylin.metadata.url.identifier", getMetadataUrlPrefix());
        setProperty("kylin.metadata.url.unique-id", getMetadataUrlUniqueId());
        setProperty("kylin.log.spark-executor-properties-file", getLogSparkExecutorPropertiesFile());
        setProperty("kylin.log.spark-driver-properties-file", getLogSparkDriverPropertiesFile());
        setProperty("kylin.log.spark-appmaster-properties-file", getLogSparkAppMasterPropertiesFile());

        // https://github.com/kyligence/kap/issues/12654
        this.properties.put(WORKING_DIR_PROP,
                makeQualified(new Path(this.properties.getProperty(WORKING_DIR_PROP, KYLIN_ROOT))).toString());
        if (this.properties.getProperty(DATA_WORKING_DIR_PROP) != null) {
            this.properties.put(DATA_WORKING_DIR_PROP,
                    makeQualified(new Path(this.properties.getProperty(DATA_WORKING_DIR_PROP))).toString());
        }
        if (this.properties.getProperty(WRITING_CLUSTER_WORKING_DIR) != null) {
            this.properties.put(WRITING_CLUSTER_WORKING_DIR,
                    makeQualified(new Path(this.properties.getProperty(WRITING_CLUSTER_WORKING_DIR))).toString());
        }
    }

    private Map<Integer, String> convertKeyToInteger(Map<String, String> map) {
        Map<Integer, String> result = Maps.newLinkedHashMap();
        for (Entry<String, String> entry : map.entrySet()) {
            result.put(Integer.parseInt(entry.getKey()), entry.getValue());
        }
        return result;
    }

    // ============================================================================
    // ENV
    // ============================================================================

    public boolean isDevOrUT() {
        return isUTEnv() || isDevEnv();
    }

    public boolean isUTEnv() {
        return "UT".equals(getDeployEnv());
    }

    public boolean isDevEnv() {
        return "DEV".equals(getDeployEnv());
    }

    public String getDeployEnv() {
        return getOptional("kylin.env", "PROD");
    }

    protected String cachedHdfsWorkingDirectory;

    public String getHdfsWorkingDirectoryWithoutScheme() {
        return HadoopUtil.getPathWithoutScheme(getHdfsWorkingDirectory());
    }

    protected Path makeQualified(Path path) {
        try {
            FileSystem fs = path.getFileSystem(HadoopUtil.getCurrentConfiguration());
            return fs.makeQualified(path);
        } catch (IOException e) {
            throw new KylinRuntimeException(e);
        }
    }

    public String getHdfsWorkingDirectory() {
        if (cachedHdfsWorkingDirectory != null) {
            return cachedHdfsWorkingDirectory;
        }

        String root = getOptional(DATA_WORKING_DIR_PROP, null);
        boolean compriseMetaId = false;

        if (root == null) {
            root = getOptional(WORKING_DIR_PROP, KYLIN_ROOT);
            compriseMetaId = true;
        }

        Path path = new Path(root);
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException("kylin.env.hdfs-working-dir must be absolute, but got " + root);
        }

        // make sure path is qualified
        path = makeQualified(path);

        if (compriseMetaId) {
            // if configuration WORKING_DIR_PROP_V2 dose not exist, append metadata-url prefix
            String metaId = getMetadataUrlPrefix().replace(':', '-').replace('/', '-');
            path = new Path(path, metaId);
        }

        root = path.toString();
        if (!root.endsWith("/")) {
            root += "/";
        }

        cachedHdfsWorkingDirectory = root;
        if (cachedHdfsWorkingDirectory.startsWith("file:")) {
            cachedHdfsWorkingDirectory = cachedHdfsWorkingDirectory.replace("file:", "file://");
        } else if (cachedHdfsWorkingDirectory.startsWith("maprfs:")) {
            cachedHdfsWorkingDirectory = cachedHdfsWorkingDirectory.replace("maprfs:", "maprfs://");
        }
        logger.info("Hdfs data working dir is setting to {}", cachedHdfsWorkingDirectory);
        return cachedHdfsWorkingDirectory;
    }

    public String getKylinMetricsPrefix() {
        return getOptional("kylin.metrics.prefix", "KYLIN").toUpperCase(Locale.ROOT);
    }

    public String getFirstDayOfWeek() {
        return getOptional("kylin.metadata.first-day-of-week", "monday");
    }

    public String getKylinMetricsActiveReservoirDefaultClass() {
        return getOptional("kylin.metrics.active-reservoir-default-class",
                "org.apache.kylin.metrics.lib.impl.StubReservoir");
    }

    public String getKylinSystemCubeSinkDefaultClass() {
        return getOptional("kylin.metrics.system-cube-sink-default-class",
                "org.apache.kylin.metrics.lib.impl.hive.HiveSink");
    }

    public String getEngineSparkHome() {
        return getOptional("kylin.engine.spark-home", null);
    }

    public boolean isKylinMetricsMonitorEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metrics.monitor-enabled", FALSE));
    }

    public String getZookeeperBasePath() {
        return getOptional("kylin.env.zookeeper-base-path", KYLIN_ROOT);
    }

    public String getClusterName() {
        return this.getOptional("kylin.server.cluster-name", getMetadataUrlUniqueId());
    }

    public int getZKBaseSleepTimeMs() {
        long sleepTimeMs = TimeUtil.timeStringAs(getOptional("kylin.env.zookeeper-base-sleep-time", "3s"),
                TimeUnit.MILLISECONDS);
        return toIntExact(sleepTimeMs);
    }

    public int getZKMaxRetries() {
        return Integer.parseInt(getOptional("kylin.env.zookeeper-max-retries", "3"));
    }

    /**
     * A comma separated list of host:port pairs, each corresponding to a ZooKeeper server
     */
    public String getZookeeperConnectString() {
        String str = getOptional("kylin.env.zookeeper-connect-string");
        if (!StringUtils.isEmpty(str)) {
            return str;
        }

        throw new KylinRuntimeException("Please set 'kylin.env.zookeeper-connect-string' in kylin.properties");
    }

    public long geZookeeperClientSessionTimeoutThreshold() {
        return TimeUtil.timeStringAs(getOptional("kylin.env.zookeeper-session-client-timeout-threshold", "120000ms"),
                TimeUnit.MILLISECONDS);
    }

    public long geZookeeperClientConnectionTimeoutThreshold() {
        return TimeUtil.timeStringAs(getOptional("kylin.env.zookeeper-connection-client-timeout-threshold", "15000ms"),
                TimeUnit.MILLISECONDS);
    }

    public boolean isZookeeperAclEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.env.zookeeper-acl-enabled", FALSE));
    }

    public String getZKAuths() {
        return EncryptUtil.getDecryptedValue(getOptional("kylin.env.zookeeper.zk-auth", ""));
    }

    public String getZKAcls() {
        return getOptional("kylin.env.zookeeper.zk-acl", "world:anyone:rwcda");
    }

    public String getYarnStatusCheckUrl() {
        return getOptional("kylin.job.yarn-app-rest-check-status-url", null);
    }

    // ============================================================================
    // METADATA
    // ============================================================================

    public int getQueryConcurrentRunningThresholdForProject() {
        // by default there's no limitation
        return Integer.parseInt(getOptional("kylin.query.project-concurrent-running-threshold", "0"));
    }

    public int getAsyncQueryMaxConcurrentJobs() {
        // by default there's no limitation
        return Integer.parseInt(getOptional("kylin.query.async-query.max-concurrent-jobs", "0"));
    }

    public boolean isAdminUserExportAllowed() {
        return Boolean.parseBoolean(getOptional("kylin.web.export-allow-admin", TRUE));
    }

    public boolean isNoneAdminUserExportAllowed() {
        return Boolean.parseBoolean(getOptional("kylin.web.export-allow-other", TRUE));
    }

    public StorageURL getMetadataUrl() {
        return StorageURL.valueOf(getOptional("kylin.metadata.url", "kylin_metadata@jdbc"));
    }

    public boolean isMetadataCompressEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.compress.enabled", TRUE));
    }

    public String getSecondStorage() {
        return getOptional("kylin.second-storage.class", null);
    }

    public String getSecondStorageDiagLogMatcher() {
        return getOptional("kylin.second-storage.diag-log-matcher", "*-server.log");
    }

    public int getSecondStorageDiagMaxCompressedFile() {
        return Integer.parseInt(getOptional("kylin.second-storage.diag-max-compressed-file", "2"));
    }

    public String getSecondStorageSshIdentityPath() {
        return getOptional("kylin.second-storage.ssh-identity-path", "~/.ssh/id_rsa");
    }

    public int getSecondStorageLoadDeduplicationWindow() {
        return Integer.parseInt(getOptional("kylin.second-storage.load-deduplication-window", "0"));
    }

    public int getSecondStorageLoadRetry() {
        return Integer.parseInt(getOptional("kylin.second-storage.load-retry", "3"));
    }

    public int getSecondStorageLoadRetryInterval() {
        return Integer.parseInt(getOptional("kylin.second-storage.load-retry-interval", "30000"));
    }

    public boolean getSecondStorageQueryMetricCollect() {
        return Boolean.parseBoolean(getOptional("kylin.second-storage.query-metric-collect", TRUE));
    }

    public int getSecondStorageQueryPushdownLimit() {
        return Integer.parseInt(getOptional("kylin.second-storage.query-pushdown-limit", "0"));
    }

    public boolean getSecondStorageUseLowCardinality() {
        return Boolean.parseBoolean(getOptional("kylin.second-storage.use-low-cardinality", TRUE));
    }

    public long getSecondStorageLowCardinalityNumber() {
        return Long.parseLong(getOptional("kylin.second-storage.low-cardinality-number", "10000"));
    }

    public long getSecondStorageHighCardinalityNumber() {
        return Long.parseLong(getOptional("kylin.second-storage.high-cardinality-number", ONE_HUNDRED_THOUSAND));
    }

    public int getMetadataCacheMaxNum() {
        return Integer.parseInt(getOptional("kylin.metadata.cache.max-num", String.valueOf(Integer.MAX_VALUE)));
    }

    public int getMetadataCacheMaxDuration() {
        return Integer.parseInt(getOptional("kylin.metadata.cache.max-duration", String.valueOf(Integer.MAX_VALUE)));
    }

    public boolean skipRecordJobExecutionTime() {
        return Boolean.parseBoolean(getOptional("kylin.job.skip-record-execution-time", FALSE));
    }

    public boolean isMetadataAuditLogEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.audit-log.enabled", TRUE));
    }

    public long getMetadataAuditLogMaxSize() {
        return Long.parseLong(getOptional("kylin.metadata.audit-log.max-size", "500000"));
    }

    public void setMetadataUrl(String metadataUrl) {
        setProperty("kylin.metadata.url", metadataUrl);
    }

    public String getMetadataUrlPrefix() {
        return getMetadataUrl().getIdentifier();
    }

    public String getMetadataUrlUniqueId() {
        if (KapConfig.CHANNEL_CLOUD.equalsIgnoreCase(getChannel())) {
            return getMetadataUrlPrefix();
        }
        val url = getMetadataUrl();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(url.getIdentifier());
        Optional.ofNullable(url.getParameter("url")).map(value -> value.split("\\?")[0]).map(value -> "_" + value)
                .ifPresent(stringBuilder::append);
        String instanceId = stringBuilder.toString().replaceAll("\\W", "_");
        return instanceId;
    }

    public Map<String, String> getMetadataStoreImpls() {
        Map<String, String> r = Maps.newLinkedHashMap();
        // ref constants in ISourceAware
        r.put("", "org.apache.kylin.common.persistence.metadata.FileMetadataStore");
        r.put("hdfs", "org.apache.kylin.common.persistence.metadata.HDFSMetadataStore");
        r.put("jdbc", "org.apache.kylin.common.persistence.metadata.JdbcMetadataStore");
        r.putAll(getPropertiesByPrefix("kylin.metadata.resource-store-provider.")); // note the naming convention -- http://kylin.apache.org/development/coding_naming_convention.html
        return r;
    }

    public static File getDiagFileName() {
        String uuid = RandomUtil.randomUUIDStr().toUpperCase(Locale.ROOT).substring(0, 6);
        String packageName = DIAG_ID_PREFIX
                + new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss", Locale.getDefault(Locale.Category.FORMAT))
                        .format(new Date())
                + "_" + uuid;
        String workDir = KylinConfigBase.getKylinHomeWithoutWarn();
        String diagPath = "diag_dump/" + packageName;
        File file;
        if (StringUtils.isNotEmpty(workDir)) {
            file = new File(workDir, diagPath);
        } else {
            file = new File(diagPath);
        }
        return file;
    }

    public String getSecurityProfile() {
        return getOptional("kylin.security.profile", "testing");
    }

    public String[] getHdfsMetaStoreFileSystemSchemas() {
        return getOptionalStringArray("kylin.metadata.hdfs-compatible-schemas", //
                new String[] { "hdfs", "maprfs", "s3", "s3a", "wasb", "wasbs", "adl", "adls", "abfs", "abfss", "gs",
                        "oss" });
    }

    public String getMultiPartitionKeyMappingProvider() {
        return getOptional("kylin.model.multi-partition-key-mapping-provider-class",
                "org.apache.kylin.metadata.model.DefaultMultiPartitionKeyMappingProvider");
    }

    public boolean isMultiPartitionEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.model.multi-partition-enabled", FALSE));
    }

    public String[] getCubeDimensionCustomEncodingFactories() {
        return getOptionalStringArray("kylin.metadata.custom-dimension-encodings", new String[0]);
    }

    public Map<String, String> getCubeCustomMeasureTypes() {
        return getPropertiesByPrefix("kylin.metadata.custom-measure-types.");
    }

    public DistributedLockFactory getDistributedLockFactory() {
        String clsName = getOptional("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.common.lock.curator.CuratorDistributedLockFactory");
        return (DistributedLockFactory) ClassUtil.newInstance(clsName);
    }

    public StorageURL getJDBCDistributedLockURL() {
        if (StringUtils.isEmpty(getOptional(KYLIN_METADATA_DISTRIBUTED_LOCK_JDBC_URL))) {
            return getMetadataUrl();
        }
        return StorageURL.valueOf(getOptional(KYLIN_METADATA_DISTRIBUTED_LOCK_JDBC_URL));
    }

    public void setJDBCDistributedLockURL(String url) {
        setProperty(KYLIN_METADATA_DISTRIBUTED_LOCK_JDBC_URL, url);
    }

    public boolean isCheckCopyOnWrite() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.check-copy-on-write", FALSE));
    }

    public boolean isCheckHostname() {
        return Boolean.parseBoolean(getOptional("kylin.env.hostname-check-enabled", TRUE));
    }

    public String getServerIpAddress() {
        // format: ip
        return getOptional("kylin.env.ip-address");
    }

    public String getServerAddress() {
        // Caution: config 'kylin.server.address' is essential in yarn cluster mode.
        // The value may be the address of loadbalancer
        // format: ip:port
        return getOptional("kylin.server.address", getDefaultServerAddress());
    }

    private String getDefaultServerAddress() {
        String hostAddress = AddressUtil.getLocalHostExactAddress();
        String serverPort = getServerPort();
        return String.format(Locale.ROOT, "%s:%s", hostAddress, serverPort);
    }

    public String getServerPort() {
        return getOptional("server.port", "7070");
    }

    public String getChannel() {
        return getOptional("kylin.env.channel", "on-premises");
    }

    public boolean isServerHttpsEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.server.https.enable", FALSE));
    }

    public Boolean isQueryNodeRequestForwardEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.request-forward-enabled", TRUE));
    }

    public int getServerHttpsPort() {
        return Integer.parseInt(getOptional("kylin.server.https.port", "7443"));
    }

    public String getServerHttpsKeyType() {
        return getOptional("kylin.server.https.keystore-type", "JKS");
    }

    public String getServerHttpsKeystore() {
        return getOptional("kylin.server.https.keystore-file", getKylinHome() + "/server/.keystore");
    }

    public String getServerHttpsKeyPassword() {
        return getOptional("kylin.server.https.keystore-password", "changeit");
    }

    public String getServerHttpsKeyAlias() {
        return getOptional("kylin.server.https.key-alias", null);
    }

    public boolean isSemiAutoMode() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.semi-automatic-mode", FALSE));
    }

    public boolean isTableExclusionEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.table-exclusion-enabled", FALSE));
    }

    /**
     * When table exclusion is enabled, this setting ensures the accuracy of query result.
     */
    public boolean isSnapshotPreferred() {
        return Boolean.parseBoolean(getOptional("kylin.query.snapshot-preferred-for-table-exclusion", TRUE));
    }

    public boolean onlyReuseUserDefinedCC() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.only-reuse-user-defined-computed-column", FALSE));
    }

    /**
     * expose computed column in the table metadata and select * queries
     */
    public boolean exposeComputedColumn() {
        return Boolean.parseBoolean(getOptional("kylin.query.metadata.expose-computed-column", FALSE));
    }

    // only for model import
    public boolean validateComputedColumn() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.validate-computed-column", TRUE));
    }

    public String getCalciteQuoting() {
        return getOptional("kylin.query.calcite.extras-props.quoting", "DOUBLE_QUOTE");
    }

    public String getModelExportHost() {
        return getOptional("kylin.model.export.host");
    }

    public int getModelExportPort() {
        return Integer.parseInt(getOptional("kylin.model.export.port", "-1"));
    }

    // ============================================================================
    // DICTIONARY & SNAPSHOT
    // ============================================================================

    public boolean isSnapshotParallelBuildEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.snapshot.parallel-build-enabled", TRUE));
    }

    public int snapshotParallelBuildTimeoutSeconds() {
        return Integer.parseInt(getOptional("kylin.snapshot.parallel-build-timeout-seconds", "3600"));
    }

    public int snapshotPartitionBuildMaxThread() {
        return Integer.parseInt(getOptional("kylin.snapshot.partition-build-max-thread", "10"));
    }

    public int getSnapshotMaxVersions() {
        return Integer.parseInt(getOptional("kylin.snapshot.max-versions", "3"));
    }

    public long getSnapshotVersionTTL() {
        return Long.parseLong(getOptional("kylin.snapshot.version-ttl", "259200000"));
    }

    public int getSnapshotShardSizeMB() {
        return Integer.parseInt(getOptional("kylin.snapshot.shard-size-mb", "128"));
    }

    public boolean isSnapshotManualManagementEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.snapshot.manual-management-enabled", FALSE));
    }

    public boolean isSnapshotAutoRefreshEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.snapshot.auto-refresh-enabled", FALSE));
    }

    public String getSnapshotAutoRefreshCron() {
        return getOptional("kylin.snapshot.auto-refresh-cron", "0 0 0 */1 * ?");
    }

    public int getSnapshotAutoRefreshFetchFilesCount() {
        return Integer.parseInt(getOptional("kylin.snapshot.auto-refresh-fetch-files-count", "1"));
    }

    public int getSnapshotAutoRefreshFetchPartitionsCount() {
        return Integer.parseInt(getOptional("kylin.snapshot.auto-refresh-fetch-partitions-count", "1"));
    }

    public int getSnapshotAutoRefreshMaxConcurrentJobLimit() {
        return Integer.parseInt(getOptional("kylin.snapshot.auto-refresh-max-concurrent-jobs", "20"));
    }

    public long getSnapshotAutoRefreshTaskTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.snapshot.auto-refresh-task-timeout", "30min"),
                TimeUnit.MILLISECONDS);
    }

    public String getSnapshotAutoRefreshDir(String project) {
        return getHdfsWorkingDirectory(project) + SNAPSHOT_AUTO_REFRESH + "/";
    }

    public boolean isSnapshotFirstAutoRefreshEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.snapshot.first-auto-refresh-enabled", FALSE));
    }

    public boolean isSnapshotNullLocationAutoRefreshEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.snapshot.null-location-auto-refresh-enabled", FALSE));
    }

    public int getGlobalDictV2MinHashPartitions() {
        return Integer.parseInt(getOptional("kylin.dictionary.globalV2-min-hash-partitions", "1"));
    }

    public int getGlobalDictV2ThresholdBucketSize() {
        return Integer.parseInt(getOptional("kylin.dictionary.globalV2-threshold-bucket-size", "500000"));
    }

    public double getGlobalDictV2InitLoadFactor() {
        return Double.parseDouble(getOptional("kylin.dictionary.globalV2-init-load-factor", "0.5"));
    }

    public double getGlobalDictV2BucketOverheadFactor() {
        return Double.parseDouble(getOptional("kylin.dictionary.globalV2-bucket-overhead-factor", "1.5"));
    }

    public int getGlobalDictV2MaxVersions() {
        return Integer.parseInt(getOptional("kylin.dictionary.globalV2-max-versions", "3"));
    }

    public long getGlobalDictV2VersionTTL() {
        return Long.parseLong(getOptional("kylin.dictionary.globalV2-version-ttl", "259200000"));
    }

    public long getNullEncodingOptimizeThreshold() {
        return Long.parseLong(getOptional("kylin.dictionary.null-encoding-opt-threshold", "40000000"));
    }

    // ============================================================================
    // CUBE
    // ============================================================================

    public String getSegmentAdvisor() {
        return getOptional("kylin.cube.segment-advisor", "org.apache.kylin.cube.CubeSegmentAdvisor");
    }

    public long getCubeAggrGroupMaxCombination() {
        return Long.parseLong(getOptional("kylin.cube.aggrgroup.max-combination", "4096"));
    }

    public boolean getCubeAggrGroupIsMandatoryOnlyValid() {
        return Boolean.parseBoolean(getOptional("kylin.cube.aggrgroup.is-mandatory-only-valid", TRUE));
    }

    public int getLowFrequencyThreshold() {
        return Integer.parseInt(this.getOptional("kylin.cube.low-frequency-threshold", "0"));
    }

    public int getFrequencyTimeWindowInDays() {
        return Integer.parseInt(this.getOptional("kylin.cube.frequency-time-window", "30"));
    }

    public boolean isBaseCuboidAlwaysValid() {
        return Boolean.parseBoolean(this.getOptional("kylin.cube.aggrgroup.is-base-cuboid-always-valid", TRUE));
    }

    public boolean isBaseIndexAutoUpdate() {
        return Boolean.parseBoolean(this.getOptional("kylin.index.base-index-auto-update", TRUE));
    }

    public double getMergeSegmentStorageThreshold() {
        double thresholdValue = Double.parseDouble(getOptional("kylin.cube.merge-segment-storage-threshold", "0"));
        if (thresholdValue < 0 || thresholdValue > 1) {
            logger.warn("The configuration file is incorrect. The value of[kylin.cube.merge-segment-storage-threshold] "
                    + "cannot be less than 0 or greater than 1.");
            thresholdValue = 0;
        }
        return thresholdValue;
    }

    // ============================================================================
    // JOB
    // ============================================================================

    public String getStreamingJobTmpDir(String project) {
        return getHdfsWorkingDirectoryWithoutScheme() + "streaming/jobs/" + project + "/";
    }

    public String getStreamingJobTmpOutputStorePath(String project, String jobId) {
        return getStreamingJobTmpDir(project) + jobId + "/";
    }

    public String getJobTmpDir(String project, boolean withScheme) {
        if (!withScheme) {
            return getHdfsWorkingDirectoryWithoutScheme() + project + "/job_tmp/";
        }
        return getHdfsWorkingDirectory(project) + "job_tmp/";
    }

    public String getJobTmpDir(String project) {
        return getHdfsWorkingDirectoryWithoutScheme() + project + "/job_tmp/";
    }

    public String getSnapshotCheckPointDir(String project, String jobId) {
        return getHdfsWorkingDirectory(project) + "job_tmp/" + jobId + "/__step_checkpoint_snapshot/";
    }

    public StorageURL getJobTmpMetaStoreUrl(String project, String jobId) {
        Map<String, String> params = new HashMap<>();
        params.put("path", getJobTmpDir(project) + getNestedPath(jobId) + "meta");
        return new StorageURL(getMetadataUrlPrefix(), HDFSMetadataStore.HDFS_SCHEME, params);
    }

    public String getJobTmpOutputStorePath(String project, String jobId) {
        return getJobTmpDir(project) + getNestedPath(jobId) + "/execute_output.json";
    }

    public String getJobTmpArgsDir(String project, String jobId) {
        return getJobTmpDir(project) + getNestedPath(jobId) + "/spark_args.json";
    }

    public String getJobTmpTransactionalTableDir(String project, String jobId) {
        return getJobTmpDir(project) + jobId + "/transactional/";
    }

    public Path getJobTmpShareDir(String project, String jobId) {
        String path = getJobTmpDir(project) + jobId + "/share/";
        return new Path(path);
    }

    public boolean isBuildFilesSeparationEnabled() {
        return !getBuildConf().isEmpty() && !getWritingClusterWorkingDir().isEmpty();
    }

    public String getWriteClusterWorkingDir() {
        return getOptional("kylin.env.write-hdfs-working-dir", "");
    }

    public String getWritingClusterWorkingDir() {
        return getOptional(WRITING_CLUSTER_WORKING_DIR, "");
    }

    // The method returns with a '/' sign at the end, so withSuffix should not start with '/'
    public String getWritingClusterWorkingDir(String withSuffix) {
        // This step will remove the '/' symbol from the end of the writingClusterWorkingDir
        Path writingClusterPath = new Path(getWritingClusterWorkingDir());
        if (!writingClusterPath.isAbsolute()) {
            throw new IllegalArgumentException(
                    "kylin.env.hdfs-write-working-dir must be absolute, but got " + writingClusterPath);
        }

        // make sure path is qualified
        writingClusterPath = makeQualified(writingClusterPath);

        // Whether the configuration WORKING_DIR_PROP_V2 exists or not, append metadata-url prefix
        // Currently, only flat tables are stored independently, so there is no need to consider
        String metaId = getMetadataUrlPrefix().replace(':', '-').replace('/', '-');
        return writingClusterPath + PATH_DELIMITER + metaId + PATH_DELIMITER + withSuffix;
    }

    public Path getFlatTableDir(String project, String dataFlowId, String segmentId) {
        String flatTableDirSuffix = project + "/flat_table/" + dataFlowId + PATH_DELIMITER + segmentId;
        String writingClusterWorkingDir = getWritingClusterWorkingDir();
        if (writingClusterWorkingDir.isEmpty()) {
            return new Path(getHdfsWorkingDirectory() + flatTableDirSuffix);
        }
        return new Path(getWritingClusterWorkingDir(flatTableDirSuffix));
    }

    public Path getFactTableViewDir(String project, String dataflowId, String segmentId) {
        String path = getHdfsWorkingDirectory() + project + "/fact_table_view/" + dataflowId //
                + PATH_DELIMITER + segmentId;
        return new Path(path);
    }

    public Path getJobTmpFlatTableDir(String project, String jobId) {
        String path = getJobTmpDir(project) + jobId + "/flat_table/";
        return new Path(path);
    }

    public Path getJobTmpFactTableViewDir(String project, String jobId) {
        String path = getJobTmpDir(project) + jobId + "/fact_table_view/";
        return new Path(path);
    }

    // a_b => a/b/
    private String getNestedPath(String id) {
        String[] ids = id.split("_");
        StringBuilder builder = new StringBuilder();
        for (String subId : ids) {
            builder.append(subId).append("/");
        }
        return builder.toString();
    }

    public CliCommandExecutor getCliCommandExecutor() {
        CliCommandExecutor exec = new CliCommandExecutor();
        if (getRunAsRemoteCommand()) {
            exec.setRunAtRemote(getRemoteHadoopCliHostname(), getRemoteHadoopCliPort(), getRemoteHadoopCliUsername(),
                    getRemoteHadoopCliPassword());
        }
        return exec;
    }

    public boolean getRunAsRemoteCommand() {
        return Boolean.parseBoolean(getOptional("kylin.job.use-remote-cli"));
    }

    public int getRemoteHadoopCliPort() {
        return Integer.parseInt(getOptional("kylin.job.remote-cli-port", "22"));
    }

    public String getRemoteHadoopCliHostname() {
        return getOptional("kylin.job.remote-cli-hostname");
    }

    public String getRemoteHadoopCliUsername() {
        return getOptional("kylin.job.remote-cli-username");
    }

    public String getRemoteHadoopCliPassword() {
        return getOptional("kylin.job.remote-cli-password");
    }

    public int getRemoteSSHPort() {
        return Integer.parseInt(getOptional("kylin.job.remote-ssh-port", "22"));
    }

    public String getRemoteSSHUsername() {
        return getOptional("kylin.job.ssh-username");
    }

    public String getRemoteSSHPassword() {
        return EncryptUtil.getDecryptedValue(getOptional("kylin.job.ssh-password"));
    }

    public String getCliWorkingDir() {
        return getOptional("kylin.job.remote-cli-working-dir");
    }

    public int getMaxConcurrentJobLimit() {
        return Integer.parseInt(getOptional("kylin.job.max-concurrent-jobs", "20"));
    }

    public int getMaxStreamingConcurrentJobLimit() {
        return Integer.parseInt(getOptional("kylin.streaming.job.max-concurrent-jobs", "10"));
    }

    public Boolean getAutoSetConcurrentJob() {
        if (isDevOrUT()) {
            return Boolean.parseBoolean(getOptional("kylin.job.auto-set-concurrent-jobs", FALSE));
        }
        return Boolean.parseBoolean(getOptional("kylin.job.auto-set-concurrent-jobs", TRUE));
    }

    public double getMaxLocalConsumptionRatio() {
        return Double.parseDouble(getOptional("kylin.job.max-local-consumption-ratio", "0.5"));
    }

    public boolean isCtlJobPriorCrossProj() {
        return Boolean.parseBoolean(getOptional("kylin.job.control-job-priority-cross-project", TRUE));
    }

    public String[] getOverCapacityMailingList() {
        return getOptionalStringArray("kylin.capacity.notification-emails", new String[0]);
    }

    public boolean isOverCapacityNotificationEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.capacity.notification-enabled", FALSE));
    }

    public String getQueryExtensionFactory() {
        String defaultValue = "org.apache.kylin.query.QueryExtension$Factory";
        return getOptional("kylin.extension.query.factory", defaultValue);
    }

    public String getMetadataExtensionFactory() {
        String defaultValue = "org.apache.kylin.metadata.MetadataExtension$Factory";
        return getOptional("kylin.extension.metadata.factory", defaultValue);
    }

    public String getTestMetadataDirectory() {
        return getOptional("kylin.test.metadata.dir", "../examples/test_case_data/localmeta");
    }

    public String getTestDataDirectory() {
        return getOptional("kylin.test.data.dir", "../examples/test_data/");
    }

    public double getOverCapacityThreshold() {
        return Double.parseDouble(getOptional("kylin.capacity.over-capacity-threshold", "80")) / 100;
    }

    public boolean isMailEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-enabled", FALSE));
    }

    public boolean isStarttlsEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-mail-enable-starttls", FALSE));
    }

    public String getSmtpPort() {
        return getOptional("kylin.job.notification-mail-port", "25");
    }

    public String getMailHost() {
        return getOptional("kylin.job.notification-mail-host", "");
    }

    public String getMailUsername() {
        return getOptional("kylin.job.notification-mail-username", "");
    }

    public String getMailPassword() {
        return getOptional("kylin.job.notification-mail-password", "");
    }

    public String getMailSender() {
        return getOptional("kylin.job.notification-mail-sender", "");
    }

    public String[] getAdminDls() {
        return getOptionalStringArray("kylin.job.notification-admin-emails", new String[0]);
    }

    public String[] getJobNotificationStates() {
        return getOptionalStringArray("kylin.job.notification-enable-states", new String[0]);
    }

    public int getJobMetadataPersistRetry() {
        return Integer.parseInt(this.getOptional("kylin.job.metadata-persist-retry", "5"));
    }

    public Boolean getJobMetadataPersistNotificationEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.job.notification-on-metadata-persist", FALSE));
    }

    public int getJobRetry() {
        return Integer.parseInt(getOptional("kylin.job.retry", "0"));
    }

    // retry interval in milliseconds
    public int getJobRetryInterval() {
        return Integer.parseInt(getOptional("kylin.job.retry-interval", "30000"));
    }

    public String[] getJobRetryExceptions() {
        return getOptionalStringArray("kylin.job.retry-exception-classes", new String[0]);
    }

    public String getJobControllerLock() {
        return getOptional("kylin.job.lock", "org.apache.kylin.storage.hbase.util.ZookeeperJobLock");
    }

    public Integer getSchedulerPollIntervalSecond() {
        return Integer.parseInt(getOptional("kylin.job.scheduler.poll-interval-second", "30"));
    }

    public boolean isFlatTableJoinWithoutLookup() {
        return Boolean.parseBoolean(getOptional("kylin.job.flat-table-join-without-lookup", FALSE));
    }

    public String getJobTrackingURLPattern() {
        return getOptional("kylin.job.tracking-url-pattern", "");
    }

    public boolean isJobLogPrintEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.log-print-enabled", TRUE));

    }

    public boolean isDeleteJobTmpWhenRetry() {
        return Boolean.parseBoolean(getOptional("kylin.job.delete-job-tmp-when-retry", FALSE));
    }

    public boolean isSetYarnQueueInTaskEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine-yarn.queue.in.task.enabled", FALSE));
    }

    public List<String> getYarnQueueInTaskAvailable() {
        return Lists.newArrayList(getOptional("kylin.engine-yarn.queue.in.task.available", DEFAULT).split(","));
    }

    public boolean isConcurrencyFetchDataSourceSize() {
        return Boolean.parseBoolean(getOptional("kylin.job.concurrency-fetch-datasource-size-enabled", TRUE));
    }

    public int getConcurrencyFetchDataSourceSizeThreadNumber() {
        return Integer.parseInt(getOptional("kylin.job.concurrency-fetch-datasource-size-thread_number", "10"));
    }

    public boolean isUseBigIntAsTimestampForPartitionColumn() {
        return Boolean.parseBoolean(getOptional("kylin.job.use-bigint-as-timestamp-for-partition-column", FALSE));
    }

    // ============================================================================
    // SOURCE.HIVE
    // ============================================================================

    public int getDefaultSource() {
        return Integer.parseInt(getOptional("kylin.source.default", "9"));
    }

    public Map<Integer, String> getSourceEngines() {
        Map<Integer, String> r = Maps.newLinkedHashMap();
        // ref constants in ISourceAware

        // these sources no longer exists in Newten
        r.put(1, "org.apache.kylin.source.kafka.NSparkKafkaSource");
        r.put(8, "org.apache.kylin.source.jdbc.JdbcSource");
        r.put(9, "org.apache.kylin.engine.spark.source.NSparkDataSource");
        r.put(13, "org.apache.kylin.source.file.FileSource");

        r.putAll(convertKeyToInteger(getPropertiesByPrefix("kylin.source.provider.")));
        return r;
    }

    /**
     * was for route to hive, not used any more
     * @deprecated KYLIN-2195 re-format KylinConfigBase
     */
    @Deprecated
    public String getHiveUrl() {
        return getOptional("kylin.source.hive.connection-url", "");
    }

    /**
     * was for route to hive, not used any more
     * @deprecated KYLIN-2195 re-format KylinConfigBase
     */
    @Deprecated
    public String getHiveUser() {
        return getOptional("kylin.source.hive.connection-user", "");
    }

    /**
     * was for route to hive, not used any more
     * @deprecated KYLIN-2195 re-format KylinConfigBase
     */
    @Deprecated
    public String getHivePassword() {
        return getOptional("kylin.source.hive.connection-password", "");
    }

    public String getHiveDatabaseForIntermediateTable() {
        return this.getOptional("kylin.source.hive.database-for-flat-table", DEFAULT);
    }

    public String getHiveClientMode() {
        return getOptional("kylin.source.hive.client", "cli");
    }

    public String getHiveBeelineParams() {
        return getOptional("kylin.source.hive.beeline-params", "");
    }

    /**
     * Source Name Case Sensitive
     */
    public boolean getSourceNameCaseSensitiveEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.source.name-case-sensitive-enabled", FALSE));
    }

    public int getDefaultVarcharPrecision() {
        int v = Integer.parseInt(getOptional("kylin.source.hive.default-varchar-precision", "4096"));
        if (v < 1) {
            return 4096;
        } else if (v > 65355) {
            return 65535;
        } else {
            return v;
        }
    }

    public int getDefaultCharPrecision() {
        //at most 255 according to https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-CharcharChar
        int v = Integer.parseInt(getOptional("kylin.source.hive.default-char-precision", "255"));
        if (v < 1) {
            return 255;
        } else if (v > 255) {
            return 255;
        } else {
            return v;
        }
    }

    public int getDefaultDecimalPrecision() {
        int v = Integer.parseInt(getOptional("kylin.source.hive.default-decimal-precision", "19"));
        if (v < 1) {
            return 19;
        } else {
            return v;
        }
    }

    public int getDefaultDecimalScale() {
        int v = Integer.parseInt(getOptional("kylin.source.hive.default-decimal-scale", "4"));
        if (v < 1) {
            return 4;
        } else {
            return v;
        }
    }

    // ============================================================================
    // SOURCE.KAFKA
    // ============================================================================

    public String[] getRealizationProviders() {
        return getOptionalStringArray("kylin.metadata.realization-providers", //
                new String[] { "org.apache.kylin.metadata.cube.model.NDataflowManager" });
    }

    public String getKafkaMaxOffsetsPerTrigger() {
        return getOptional("kylin.streaming.kafka-conf.maxOffsetsPerTrigger", "0");
    }

    // ============================================================================
    // SOURCE.JDBC
    // ============================================================================

    public String getJdbcConnectionUrl() {
        return getOptional(KYLIN_SOURCE_JDBC_CONNECTION_URL_KEY);
    }

    public String getJdbcDriver() {
        return getOptional(KYLIN_SOURCE_JDBC_DRIVER_KEY);
    }

    public String getJdbcDialect() {
        return getOptional("kylin.source.jdbc.dialect");
    }

    public String getJdbcUser() {
        return getOptional(KYLIN_SOURCE_JDBC_USER_KEY);
    }

    public String getJdbcPass() {
        return EncryptUtil.getDecryptedValue(getOptional(KYLIN_SOURCE_JDBC_PASS_KEY));
    }

    public boolean getJdbcEnable() {
        return Boolean.parseBoolean(getOptional(KYLIN_SOURCE_JDBC_SOURCE_ENABLE_KEY, FALSE));
    }

    public long getKafkaPollMessageTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.source.kafka.poll-message-timeout-ms", "3000ms"),
                TimeUnit.MILLISECONDS);
    }

    public String getJdbcAdaptorClass() {
        return getOptional("kylin.source.jdbc.adaptor", getJdbcSourceConnector());
    }

    public int getJdbcConnectRetryTimes() {
        return Integer.parseInt(getOptional("kylin.source.jdbc.connect-retry-times", "1"));
    }

    public long getJdbcSleepIntervalBetweenRetry() {
        return TimeUtil.timeStringAs(getOptional("kylin.source.jdbc.connect-retry-sleep-interval", "100ms"),
                TimeUnit.MILLISECONDS);
    }

    public String getJdbcSourceConnector() {
        return getOptional("kylin.source.jdbc.connector-class-name",
                "org.apache.kylin.source.jdbc.DefaultSourceConnector");
    }

    public String getJdbcConvertToLowerCase() {
        return getOptional("kylin.source.jdbc.convert-to-lowercase", FALSE);
    }

    // ============================================================================
    // STORAGE.PARQUET
    // ============================================================================

    public Map<Integer, String> getStorageEngines() {
        Map<Integer, String> r = Maps.newLinkedHashMap();
        // ref constants in IStorageAware
        r.put(20, "org.apache.kylin.storage.ParquetDataStorage");
        r.putAll(convertKeyToInteger(getPropertiesByPrefix("kylin.storage.provider.")));
        return r;
    }

    public int getDefaultStorageEngine() {
        return Integer.parseInt(getOptional("kylin.storage.default", "20"));
    }

    public int getDefaultStorageType() {
        return Integer.parseInt(getOptional("kylin.storage.default-storage-type", "0"));
    }

    private static final String JOB_JAR_NAME_PATTERN = "newten-job(.?)\\.jar";

    private static final String JAR_NAME_PATTERN = "(.*)\\.jar";

    public String getExtraJarsPath() {
        return getOptional("kylin.engine.extra-jars-path", "");
    }

    public String getKylinJobJarPath() {
        final String jobJar = getOptional("kylin.engine.spark.job-jar");
        if (StringUtils.isNotEmpty(jobJar)) {
            return jobJar;
        }
        String kylinHome = getKylinHome();
        if (StringUtils.isEmpty(kylinHome)) {
            return "";
        }
        val jar = FileUtils.findFile(kylinHome + File.separator + "lib", JOB_JAR_NAME_PATTERN);
        if (jar == null) {
            return "";
        }
        return jar.getAbsolutePath();
    }

    public String getStreamingJobJarPath() {
        final String jobJar = getOptional("kylin.streaming.spark.job-jar");
        if (StringUtils.isNotEmpty(jobJar)) {
            return jobJar;
        }
        String kylinHome = getKylinHome();
        if (StringUtils.isEmpty(kylinHome)) {
            return "";
        }
        val jar = FileUtils.findFile(kylinHome + File.separator + "lib", JOB_JAR_NAME_PATTERN);
        if (jar == null) {
            return "";
        }
        return jar.getAbsolutePath();
    }

    public String getHdfsCustomJarPath(String project, String jarType) {
        return getHdfsWorkingDirectory() + "custom/jars/" + project + "/" + jarType + "/";
    }

    public int getStreamingCustomParserLimit() {
        int parserLimit = Integer.parseInt(getOptional("kylin.streaming.custom-parser-limit", "50"));
        return parserLimit < 1 ? 50 : parserLimit;
    }

    public long getStreamingCustomJarSizeMB() {
        return SizeConvertUtil.byteStringAs(getOptional("kylin.streaming.custom-jar-size", "20mb"), ByteUnit.BYTE);
    }

    public String getKylinExtJarsPath() {
        String kylinHome = getKylinHome();
        if (StringUtils.isEmpty(kylinHome)) {
            return "";
        }
        List<File> files = FileUtils.findFiles(kylinHome + File.separator + "lib/ext", JAR_NAME_PATTERN);
        if (CollectionUtils.isEmpty(files)) {
            return "";
        }
        StringBuilder extJar = new StringBuilder();
        for (File file : files) {
            extJar.append(",");
            extJar.append(file.getAbsolutePath());
        }
        return extJar.toString();
    }

    public String getSnapshotBuildClassName() {
        return getOptional("kylin.engine.spark.snapshot-build-class-name",
                "org.apache.kylin.engine.spark.job.SnapshotBuildJob");
    }

    public String getSparkMaster() {
        return getOptional("kylin.engine.spark-conf.spark.master", "yarn").toLowerCase(Locale.ROOT);
    }

    public String getDeployMode() {
        return getOptional("kylin.engine.spark-conf.spark.submit.deployMode", "client").toLowerCase(Locale.ROOT);
    }

    public String getSparkBuildClassName() {
        return getOptional("kylin.engine.spark.build-class-name", "org.apache.kylin.engine.spark.job.SegmentBuildJob");
    }

    public List<String> getSparkBuildConfExtraRules() {
        String rules = getOptional("kylin.engine.spark.build-conf-extra-rules");
        if (StringUtils.isEmpty(rules)) {
            return Collections.<String> emptyList();
        }
        return Lists.newArrayList(rules.split(","));
    }

    public String getSparkTableSamplingClassName() {
        return getOptional("kylin.engine.spark.sampling-class-name",
                "org.apache.kylin.engine.spark.stats.analyzer.TableAnalyzerJob");
    }

    public String getSparkMergeClassName() {
        return getOptional("kylin.engine.spark.merge-class-name", "org.apache.kylin.engine.spark.job.SegmentMergeJob");
    }

    public String getClusterManagerClassName() {
        return getOptional("kylin.engine.spark.cluster-manager-class-name",
                "org.apache.kylin.cluster.YarnClusterManager");
    }

    public long getClusterManagerTimeoutThreshold() {
        return TimeUtil.timeStringAs(getOptional("kylin.engine.cluster-manager-timeout-threshold", "150s"),
                TimeUnit.SECONDS);
    }

    public void overrideSparkJobJarPath(String path) {
        Unsafe.setProperty("kylin.engine.spark.job-jar", path);
    }

    public int getBuildingCacheThreshold() {
        int threshold = Integer.parseInt(getOptional("kylin.engine.spark.cache-threshold", "100"));
        if (threshold <= 0) {
            threshold = Integer.MAX_VALUE;
        }
        return threshold;
    }

    public Map<String, String> getSparkConfigOverride() {
        return getPropertiesByPrefix("kylin.engine.spark-conf.");
    }

    public boolean isSnapshotSpecifiedSparkConf() {
        return Boolean.parseBoolean(getOptional("kylin.engine.snapshot.specified-spark-conf-enabled", FALSE));
    }

    public Map<String, String> getSnapshotBuildingConfigOverride() {
        return getPropertiesByPrefix("kylin.engine.snapshot.spark-conf.");
    }

    public Map<String, String> getAsyncQuerySparkConfigOverride() {
        return getPropertiesByPrefix("kylin.query.async-query.spark-conf.");
    }

    public boolean isCleanSparkUIZombieJob() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.engine.clean-spark-ui-zombie-job-enabled", FALSE));
    }

    public int getSparkUIZombieJobCleanSeconds() {
        return Integer.parseInt(this.getOptional("kylin.query.engine.spark-ui-zombie-job-clean-seconds", "180"));
    }

    public int getSparkEngineMaxRetryTime() {
        return Integer.parseInt(getOptional("kylin.engine.max-retry-time", "3"));
    }

    public double getSparkEngineRetryMemoryGradient() {
        return Double.parseDouble(getOptional("kylin.engine.retry-memory-gradient", "1.5"));
    }

    public double getSparkEngineRetryOverheadMemoryGradient() {
        return Double.parseDouble(getOptional("kylin.engine.retry-overheadMemory-gradient", "0.2"));
    }

    public boolean isAutoSetSparkConf() {
        return Boolean.parseBoolean(getOptional("kylin.spark-conf.auto-prior", TRUE));
    }

    public Double getMaxAllocationResourceProportion() {
        return Double.parseDouble(getOptional("kylin.engine.max-allocation-proportion", "0.9"));
    }

    public int getSparkEngineDriverMemoryTableSampling() {
        return (int) SizeConvertUtil.byteStringAsMb(getOptional("kylin.engine.driver-memory-table-sampling", "1024"));
    }

    public int getSparkEngineDriverMemorySnapshotBuilding() {
        return (int) SizeConvertUtil
                .byteStringAsMb(getOptional("kylin.engine.snapshot.spark-conf.spark.driver.memory", "1024"));
    }

    public int getSparkEngineDriverMemoryBase() {
        return Integer.parseInt(getOptional("kylin.engine.driver-memory-base", "1024"));
    }

    public boolean useDynamicResourcePlan() {
        return Boolean.parseBoolean(getOptional("kylin.engine.dynamic-resource-plan-enabled", FALSE));
    }

    public boolean isSanityCheckEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.sanity-check-enabled", TRUE));
    }

    public boolean isGlobalDictCheckEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.global-dict-check-enabled", FALSE));
    }

    public boolean isGlobalDictAQEEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.global-dict-aqe-enabled", FALSE));
    }

    public String getJdbcSourceName() {
        return getOptional(KYLIN_SOURCE_JDBC_SOURCE_NAME_KEY);
    }

    public int getSparkEngineDriverMemoryMaximum() {
        return Integer.parseInt(getOptional("kylin.engine.driver-memory-maximum", "4096"));
    }

    public int getSparkEngineTaskCoreFactor() {
        return Integer.parseInt(getOptional("kylin.engine.spark.task-core-factor", "3"));
    }

    public String getSparkEngineSampleSplitThreshold() {
        return getOptional("kylin.engine.spark.sample-split-threshold", "256m");
    }

    public Boolean getSparkEngineTaskImpactInstanceEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.spark.task-impact-instance-enabled", TRUE));
    }

    public int getSparkEngineBaseExuctorInstances() {
        return Integer.parseInt(getOptional("kylin.engine.base-executor-instance", "5"));
    }

    public String getSparkEngineExuctorInstanceStrategy() {
        return getOptional("kylin.engine.executor-instance-strategy", "100,2,500,3,1000,4");
    }

    public Double getSparkEngineResourceRequestOverLimitProportion() {
        return Double.parseDouble(getOptional("kylin.engine.resource-request-over-limit-proportion", "1.0"));
    }

    public boolean streamingEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.streaming.enabled", FALSE));
    }

    public Map<String, String> getStreamingSparkConfigOverride() {
        return getPropertiesByPrefix("kylin.streaming.spark-conf.");
    }

    public Map<String, String> getStreamingKafkaConfigOverride() {
        return getPropertiesByPrefix("kylin.streaming.kafka-conf.");
    }

    public String getStreamingTableRefreshInterval() {
        return getOptional("kylin.streaming.table-refresh-interval");
    }

    public String getStreamingJobStatusWatchEnabled() {
        return getOptional("kylin.streaming.job-status-watch-enabled", TRUE);
    }

    public String getStreamingJobRetryEnabled() {
        return getOptional("kylin.streaming.job-retry-enabled", FALSE);
    }

    public int getStreamingJobRetryInterval() {
        return (int) TimeUtil.timeStringAs(getOptional("kylin.streaming.job-retry-interval", "5m"), TimeUnit.MINUTES);
    }

    public int getStreamingJobMaxRetryInterval() {
        return (int) TimeUtil.timeStringAs(getOptional("kylin.streaming.job-retry-max-interval", "30m"),
                TimeUnit.MINUTES);
    }

    public String getStreamingJobWatermark() {
        return getOptional("kylin.streaming.watermark", "");
    }

    public long getStreamingJobExecutionIdCheckInterval() {
        return TimeUtil.timeStringAs(getOptional("kylin.streaming.job-execution-id-check-interval", "1m"),
                TimeUnit.MINUTES);
    }

    public long getStreamingJobMetaRetainedTime() {
        return TimeUtil.timeStringAs(getOptional("kylin.streaming.job-meta-retained-time", "2h"),
                TimeUnit.MILLISECONDS);
    }

    public String getSparkEngineBuildStepsToSkip() {
        return getOptional("kylin.engine.steps.skip", "");
    }

    // ============================================================================
    // ENGINE.SPARK
    // ============================================================================

    public String getHadoopConfDir() {
        return getOptional("kylin.env.hadoop-conf-dir", "");
    }

    // ============================================================================
    // QUERY
    // ============================================================================

    public boolean isRouteToMetadataEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.using-metadata-answer-minmax-of-dimension", FALSE));
    }

    public boolean partialMatchNonEquiJoins() {
        return Boolean.parseBoolean(getOptional("kylin.query.match-partial-non-equi-join-model", FALSE));
    }

    public boolean asyncProfilingEnabled() {
        return !Boolean.parseBoolean(SystemPropertiesCache.getProperty("spark.local", FALSE))
                && Boolean.parseBoolean(getOptional("kylin.query.async-profiler-enabled", TRUE));
    }

    public long asyncProfilingResultTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.query.async-profiler-result-timeout", "60s"),
                TimeUnit.MILLISECONDS);
    }

    public long asyncProfilingProfileTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.query.async-profiler-profile-timeout", "5m"),
                TimeUnit.MILLISECONDS);
    }

    public boolean readSourceWithDefaultParallelism() {
        return Boolean.parseBoolean(getOptional("kylin.query.read-source-with-default-parallelism", FALSE));
    }

    public boolean isHeterogeneousSegmentEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.heterogeneous-segment-enabled", TRUE));
    }

    public boolean isUseTableIndexAnswerNonRawQuery() {
        return Boolean.parseBoolean(getOptional("kylin.query.use-tableindex-answer-non-raw-query", FALSE));
    }

    public boolean isPreferAggIndex() {
        return Boolean.parseBoolean(getOptional("kylin.query.layout.prefer-aggindex", TRUE));
    }

    public boolean isTransactionEnabledInQuery() {
        return Boolean.parseBoolean(getOptional("kylin.query.transaction-enable", FALSE));
    }

    public boolean isConvertCreateTableToWith() {
        return Boolean.parseBoolean(getOptional("kylin.query.convert-create-table-to-with", FALSE));
    }

    public boolean isConvertSumExpressionEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.convert-sum-expression-enabled", FALSE));
    }

    public boolean isEnhancedAggPushDownEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.enhanced-agg-pushdown-enabled", FALSE));
    }

    public boolean isOptimizedSumCastDoubleRuleEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.optimized-sum-cast-double-rule-enabled", TRUE));
    }

    public boolean isQueryFilterReductionEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.filter-reduction-enabled", TRUE));
    }

    public boolean isConvertCountDistinctExpressionEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.convert-count-distinct-expression-enabled", FALSE));
    }

    public long getQueryMemoryLimitDuringCollect() {
        return Long.parseLong(getOptional("kylin.query.memory-limit-during-collect-mb", "5400"));
    }

    /**
     * Rule is usually singleton as static field, the configuration of this property is like:
     * RuleClassName1#FieldName1,RuleClassName2#FieldName2,...
     */
    public List<String> getCalciteAddRule() {
        String rules = getOptional("kylin.query.calcite.add-rule");
        if (StringUtils.isEmpty(rules)) {
            return Lists.newArrayList("io.kyligence.kap.query.optrule.ExtensionOlapJoinRule#INSTANCE");
        }
        return Lists.newArrayList(rules.split(","));
    }

    /**
     * Rule is usually singleton as static field, the configuration of this property is like:
     * RuleClassName1#FieldName1,RuleClassName2#FieldName2,...
     */
    public List<String> getCalciteRemoveRule() {
        String rules = getOptional("kylin.query.calcite.remove-rule");
        if (StringUtils.isEmpty(rules)) {
            return Lists.newArrayList("io.kyligence.kap.query.optrule.OLAPJoinRule#INSTANCE");
        }
        return Lists.newArrayList(rules.split(","));
    }

    public boolean isReplaceColCountWithCountStar() {
        return Boolean.parseBoolean(getOptional("kylin.query.replace-count-column-with-count-star", FALSE));
    }

    // Select star on large table is too slow for BI, add limit by default if missing
    // https://issues.apache.org/jira/browse/KYLIN-2649
    public int getForceLimit() {
        return Integer.parseInt(getOptional("kylin.query.force-limit", "-1"));
    }

    // If return empty result for select star query
    // https://olapio.atlassian.net/browse/KE-23663
    public boolean getEmptyResultForSelectStar() {
        return Boolean.parseBoolean(getOptional("kylin.query.return-empty-result-on-select-star", FALSE));
    }

    /**
     * the threshold for query result caching
     * query result will only be cached if the result is below the threshold
     * the size of the result is counted by its cells (rows * columns)
     *
     * @return
     */
    public long getLargeQueryThreshold() {
        return Integer.parseInt(getOptional("kylin.query.large-query-threshold", String.valueOf(1000000)));
    }

    public int[] getSparkEngineDriverMemoryStrategy() {
        String[] dft = { "2", "20", "100" };
        return getOptionalIntArray("kylin.engine.driver-memory-strategy", dft);
    }

    public int getLoadCounterCapacity() {
        return Integer.parseInt(getOptional("kylin.query.load-counter-capacity", "50"));
    }

    public long getLoadCounterPeriodSeconds() {
        return TimeUtil.timeStringAs(getOptional("kylin.query.load-counter-period-seconds", "3s"), TimeUnit.SECONDS);
    }

    /**
     * The threshold for cartesian product partition number is
     * executor instance num * executor core num * cartesian-partition-num-threshold-factor
     *
     * @return
     */
    public int getCartesianPartitionNumThresholdFactor() {
        return Integer
                .parseInt(getOptional("kylin.query.cartesian-partition-num-threshold-factor", String.valueOf(100)));
    }

    public String[] getQueryInterceptors() {
        return getOptionalStringArray("kylin.query.interceptors", new String[0]);
    }

    public long getQueryDurationCacheThreshold() {
        return Long.parseLong(this.getOptional("kylin.query.cache-threshold-duration", String.valueOf(2000)));
    }

    public long getQueryScanCountCacheThreshold() {
        return Long.parseLong(this.getOptional("kylin.query.cache-threshold-scan-count", String.valueOf(10 * 1024)));
    }

    public long getQueryScanBytesCacheThreshold() {
        return Long.parseLong(this.getOptional("kylin.query.cache-threshold-scan-bytes", String.valueOf(1024 * 1024)));
    }

    public boolean isQueryCacheEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.cache-enabled", TRUE));
    }

    public boolean isSchemaCacheEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.schema-cache-enabled", FALSE));
    }

    public boolean isDataFrameCacheEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.dataframe-cache-enabled", TRUE));
    }

    public boolean enableReplaceDynamicParams() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.replace-dynamic-params-enabled", FALSE));
    }

    // ============================================================================
    // Cache
    // ============================================================================

    public boolean isRedisEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.cache.redis.enabled", FALSE));
    }

    public boolean isRedisClusterEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.cache.redis.cluster-enabled", FALSE));
    }

    public String getRedisHosts() {
        return getOptional("kylin.cache.redis.hosts", "localhost:6379");
    }

    public String getRedisPassword() {
        return getOptional("kylin.cache.redis.password", "");
    }

    //EX is second and PX is milliseconds
    public String getRedisExpireTimeUnit() {
        String expireTimeUnit = getOptional("kylin.cache.redis.expire-time-unit", "EX");
        if (!expireTimeUnit.equals("EX") && !expireTimeUnit.equals("PX")) {
            expireTimeUnit = "EX";
        }
        return expireTimeUnit;
    }

    public long getRedisExpireTime() {
        return Long.parseLong(getOptional("kylin.cache.redis.expire-time", "86400"));
    }

    public long getRedisExpireTimeForException() {
        return Long.parseLong(getOptional("kylin.cache.redis.exception-expire-time", "600"));
    }

    public int getRedisConnectionTimeout() {
        return Integer.parseInt(getOptional("kylin.cache.redis.connection-timeout", "2000"));
    }

    public int getRedisSoTimeout() {
        return Integer.parseInt(getOptional("kylin.cache.redis.so-timeout", "2000"));
    }

    public int getRedisMaxAttempts() {
        return Integer.parseInt(getOptional("kylin.cache.redis.max-attempts", "20"));
    }

    public int getRedisMaxTotal() {
        return Integer.parseInt(getOptional("kylin.cache.redis.max-total", "8"));
    }

    public int getRedisMaxIdle() {
        return Integer.parseInt(getOptional("kylin.cache.redis.max-idle", "8"));
    }

    public int getRedisMinIdle() {
        return Integer.parseInt(getOptional("kylin.cache.redis.min-idle", "0"));
    }

    public int getRedisScanKeysBatchCount() {
        return Integer.parseInt(getOptional("kylin.cache.redis.batch-count", ONE_HUNDRED_THOUSAND));
    }

    // Memcached
    public boolean isMemcachedEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.cache.memcached.enabled", FALSE));
    }

    public String getMemcachedHosts() {
        return getOptional("kylin.cache.memcached.hosts", "localhost:11211");
    }

    public long getMemcachedOpTimeout() {
        return Long.parseLong(getOptional("kylin.cache.memcached.option.timeout", "500"));
    }

    public int getMaxChunkSize() {
        return Integer.parseInt(getOptional("kylin.cache.memcached.max-chunk-size", "1024"));
    }

    public int getMaxObjectSize() {
        return Integer.parseInt(getOptional("kylin.cache.memcached.max-object-size", "1048576"));
    }

    public boolean isEnableCompression() {
        return Boolean.parseBoolean((getOptional("kylin.cache.memcached.is-enable-compression", TRUE)));
    }

    public long getMaxWaitMillis() {
        return Long.parseLong(getOptional("kylin.cache.redis.max-wait", "300000"));
    }

    public String getEhCacheConfigPath() {
        return getOptional("kylin.cache.config", "classpath:ehcache.xml");
    }

    public boolean isQueryIgnoreUnknownFunction() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.ignore-unknown-function", FALSE));
    }

    public boolean isQueryMatchPartialInnerJoinModel() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.match-partial-inner-join-model", FALSE));
    }

    /**
     * if FALSE,
     * non-equi-inner join will be transformed to inner join and filter
     * left join will be transformed runtime-join
     *
     * @return
     */
    public boolean isQueryNonEquiJoinModelEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.non-equi-join-model-enabled", FALSE));
    }

    public String getQueryAccessController() {
        return getOptional("kylin.query.access-controller", null);
    }

    public int getDimCountDistinctMaxCardinality() {
        return Integer.parseInt(getOptional("kylin.query.max-dimension-count-distinct", "5000000"));
    }

    public boolean getAutoModelViewEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.auto-model-view-enabled", FALSE));
    }

    public Map<String, String> getUDFs() {
        Map<String, String> udfMap = Maps.newLinkedHashMap();
        udfMap.put("regexp_like", "org.apache.kylin.query.udf.otherUdf.RegexpLikeUDF");
        udfMap.put("rlike", "org.apache.kylin.query.udf.otherUdf.RlikeUDF");
        udfMap.put("if", "org.apache.kylin.query.udf.otherUdf.IfUDF");
        udfMap.put("version", "org.apache.kylin.query.udf.VersionUDF");
        udfMap.put("bitmap_function", "org.apache.kylin.query.udf.BitmapUDF");
        udfMap.put("concat", "org.apache.kylin.query.udf.stringUdf.ConcatUDF");
        udfMap.put("concat_ws", "org.apache.kylin.query.udf.stringUdf.ConcatwsUDF");
        udfMap.put("massin", "org.apache.kylin.query.udf.MassInUDF");
        udfMap.put("initcapb", "org.apache.kylin.query.udf.stringUdf.InitCapbUDF");
        udfMap.put("substr", "org.apache.kylin.query.udf.stringUdf.SubStrUDF");
        udfMap.put("left", "org.apache.kylin.query.udf.stringUdf.LeftUDF");
        udfMap.put("date_part", "org.apache.kylin.query.udf.dateUdf.DatePartUDF");
        udfMap.put("date_trunc", "org.apache.kylin.query.udf.dateUdf.DateTruncUDF");
        udfMap.put("datediff", "org.apache.kylin.query.udf.dateUdf.DateDiffUDF");
        udfMap.put("unix_timestamp", "org.apache.kylin.query.udf.dateUdf.UnixTimestampUDF");
        udfMap.put("length", "org.apache.kylin.query.udf.stringUdf.LengthUDF");
        udfMap.put("repeat", "org.apache.kylin.query.udf.stringUdf.RepeatUDF");
        udfMap.put("to_timestamp", "org.apache.kylin.query.udf.formatUdf.ToTimestampUDF");
        udfMap.put("to_date", "org.apache.kylin.query.udf.formatUdf.ToDateUDF");
        udfMap.put("to_char", "org.apache.kylin.query.udf.formatUdf.ToCharUDF");
        udfMap.put("date_format", "org.apache.kylin.query.udf.formatUdf.DateFormatUDF");
        udfMap.put("instr", "org.apache.kylin.query.udf.stringUdf.InStrUDF");
        udfMap.put("strpos", "org.apache.kylin.query.udf.stringUdf.StrPosUDF");
        udfMap.put("ifnull", "org.apache.kylin.query.udf.nullHandling.IfNullUDF");
        udfMap.put("nvl", "org.apache.kylin.query.udf.nullHandling.NvlUDF");
        udfMap.put("isnull", "org.apache.kylin.query.udf.nullHandling.IsNullUDF");
        udfMap.put("split_part", "org.apache.kylin.query.udf.stringUdf.SplitPartUDF");
        udfMap.put("spark_leaf_function", "org.apache.kylin.query.udf.SparkLeafUDF");
        udfMap.put("spark_string_function", "org.apache.kylin.query.udf.SparkStringUDF");
        udfMap.put("spark_misc_function", "org.apache.kylin.query.udf.SparkMiscUDF");
        udfMap.put("spark_time_function", "org.apache.kylin.query.udf.SparkTimeUDF");
        udfMap.put("spark_math_function", "org.apache.kylin.query.udf.SparkMathUDF");
        udfMap.put("spark_other_function", "org.apache.kylin.query.udf.SparkOtherUDF");
        udfMap.put("tableau_string_func", "org.apache.kylin.query.udf.stringUdf.TableauStringUDF");
        udfMap.put("size", "org.apache.kylin.query.udf.SizeUDF");
        Map<String, String> overrideUdfMap = getPropertiesByPrefix("kylin.query.udf.");
        udfMap.putAll(overrideUdfMap);
        return udfMap;
    }

    public int getSlowQueryDefaultDetectIntervalSeconds() {
        int intervalSec = Integer.parseInt(getOptional("kylin.query.slowquery-detect-interval", "3"));
        if (intervalSec < 1) {
            logger.warn("Slow query detect interval less than 1 sec, set to 1 sec.");
            intervalSec = 1;
        }
        return intervalSec;
    }

    public int getQueryTimeoutSeconds() {
        int time = Integer.parseInt(this.getOptional("kylin.query.timeout-seconds", "300"));
        if (time < 5) {
            logger.warn("Query timeout seconds less than 5 sec, set to 5 sec.");
            time = 5;
        }
        return time;
    }

    public String getQueryVIPRole() {
        return String.valueOf(getOptional("kylin.query.vip-role", ""));
    }

    public boolean isPushDownEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.pushdown-enabled", TRUE));
    }

    public String getPushDownRunnerClassName() {
        return getOptional("kylin.query.pushdown.runner-class-name", "");
    }

    public String getPushDownRunnerClassNameWithDefaultValue() {
        String pushdownRunner = getPushDownRunnerClassName();
        if (StringUtils.isEmpty(pushdownRunner)) {
            pushdownRunner = "org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl";
        }
        return pushdownRunner;
    }

    public String[] getTableDetectorTransformers() {
        String value = getOptional("kylin.query.table-detect-transformers");
        return value == null
                ? new String[] { POWER_BI_CONVERTER, "org.apache.kylin.query.util.DefaultQueryTransformer",
                        "org.apache.kylin.query.util.EscapeTransformer" }
                : getOptionalStringArray("kylin.query.table-detect-transformers", new String[0]);
    }

    public String[] getQueryTransformers() {
        String value = getOptional("kylin.query.transformers");
        return value == null ? new String[] { POWER_BI_CONVERTER, "org.apache.kylin.query.util.DefaultQueryTransformer",
                "org.apache.kylin.query.util.EscapeTransformer", "org.apache.kylin.query.util.ConvertToComputedColumn",
                "org.apache.kylin.query.util.KeywordDefaultDirtyHack", "org.apache.kylin.query.security.RowFilter" }
                : getOptionalStringArray("kylin.query.transformers", new String[0]);
    }

    public String getPartitionCheckRunnerClassName() {
        return getOptional("kylin.query.pushdown.partition-check.runner-class-name", "");
    }

    public String getDefaultPartitionCheckerClassName() {
        String partitionCheckRunner = getPartitionCheckRunnerClassName();
        if (StringUtils.isEmpty(partitionCheckRunner)) {
            partitionCheckRunner = "org.apache.kylin.query.pushdown.PushDownRunnerSparkImpl";
        }
        return partitionCheckRunner;
    }

    public boolean isPushdownQueryCacheEnabled() {
        // KAP#12784 disable all push-down caches, even if the pushdown result is cached, it won't be used
        // Thus this config is set to FALSE by default
        // you may need to change the default value if the pushdown cache issue KAP#13060 is resolved
        return Boolean.parseBoolean(this.getOptional("kylin.query.pushdown.cache-enabled", FALSE));
    }

    public boolean isAutoSetPushDownPartitions() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.pushdown.auto-set-shuffle-partitions-enabled", TRUE));
    }

    public int getBaseShufflePartitionSize() {
        return Integer.parseInt(this.getOptional("kylin.query.pushdown.base-shuffle-partition-size", "48"));
    }

    public String getHiveMetastoreExtraClassPath() {
        return getOptional("kylin.query.pushdown.hive-extra-class-path", "");
    }

    public String getJdbcUrl() {
        return getOptional("kylin.query.pushdown.jdbc.url", "");
    }

    public String getJdbcDriverClass() {
        return getOptional("kylin.query.pushdown.jdbc.driver", "");
    }

    public String getJdbcUsername() {
        return getOptional("kylin.query.pushdown.jdbc.username", "");
    }

    public String getJdbcPassword() {
        return getOptional("kylin.query.pushdown.jdbc.password", "");
    }

    public int getPoolMaxTotal() {
        return Integer.parseInt(this.getOptional("kylin.query.pushdown.jdbc.pool-max-total", "8"));
    }

    public int getPoolMaxIdle() {
        return Integer.parseInt(this.getOptional("kylin.query.pushdown.jdbc.pool-max-idle", "8"));
    }

    public int getPoolMinIdle() {
        return Integer.parseInt(this.getOptional("kylin.query.pushdown.jdbc.pool-min-idle", "0"));
    }

    public boolean isAclTCREnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.security.acl-tcr-enabled", TRUE));
    }

    public boolean inferFiltersEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.job.infer-filters-enabled", FALSE));
    }

    public boolean isEscapeDefaultKeywordEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.escape-default-keyword", FALSE));
    }

    public String getQueryRealizationFilter() {
        return getOptional("kylin.query.realization-filter", null);
    }

    public int getQueryRealizationChooserThreadMaxNum() {
        return Integer.parseInt(this.getOptional("kylin.query.realization.chooser.thread-max-num", "50"));
    }

    public int getQueryRealizationChooserThreadCoreNum() {
        return Integer.parseInt(this.getOptional("kylin.query.realization.chooser.thread-core-num", "5"));
    }

    /**
     * Extras calcite properties to config Calcite connection
     */
    public Map<String, String> getCalciteExtrasProperties() {
        return getPropertiesByPrefix("kylin.query.calcite.extras-props.");
    }

    public String getCalciteConformance() {
        return this.getOptional("kylin.query.calcite.extras-props.conformance", DEFAULT);
    }

    public boolean isExecuteAsEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.query-with-execute-as", FALSE));
    }

    public boolean isJoinMatchOptimizationEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.join-match-optimization-enabled", FALSE));
    }

    // ============================================================================
    // SERVER
    // ============================================================================

    public String getServerMode() {
        return this.getOptional("kylin.server.mode", "all");
    }

    public String getMetadataStoreType() {
        if (!isJobNode()) {
            return this.getOptional("kylin.server.store-type", "hdfs");
        } else {
            return "jdbc";
        }
    }

    public boolean isJobNode() {
        return !StringUtils.equals(ClusterConstant.ServerModeEnum.QUERY.getName(), getServerMode());
    }

    public boolean isQueryNode() {
        return !StringUtils.equals(ClusterConstant.ServerModeEnum.JOB.getName(), getServerMode());
    }

    public boolean isJobNodeOnly() {
        return StringUtils.equals(ClusterConstant.ServerModeEnum.JOB.getName(), getServerMode());
    }

    public boolean isQueryNodeOnly() {
        return StringUtils.equals(ClusterConstant.ServerModeEnum.QUERY.getName(), getServerMode());
    }

    public boolean isAllNode() {
        return ClusterConstant.ALL.equals(getServerMode());
    }

    public String[] getAllModeServers() {
        return this.getSystemStringArray("kylin.server.cluster-mode-all", new String[0]);
    }

    public String[] getQueryModeServers() {
        return this.getSystemStringArray("kylin.server.cluster-mode-query", new String[0]);
    }

    public String[] getJobModeServers() {
        return this.getSystemStringArray("kylin.server.cluster-mode-job", new String[0]);
    }

    public List<String> getAllServers() {
        List<String> allServers = Lists.newArrayList();

        allServers.addAll(Arrays.asList(getAllModeServers()));
        allServers.addAll(Arrays.asList(getQueryModeServers()));
        allServers.addAll(Arrays.asList(getJobModeServers()));

        return allServers;
    }

    public Boolean getStreamingChangeMeta() {
        return Boolean.parseBoolean(this.getOptional("kylin.server.streaming-change-meta", FALSE));
    }

    public String[] getPushDownConverterClassNames() {
        return getOptionalStringArray("kylin.query.pushdown.converter-class-names",
                new String[] { "org.apache.kylin.source.adhocquery.DoubleQuotePushDownConverter", POWER_BI_CONVERTER,
                        "org.apache.kylin.query.util.KeywordDefaultDirtyHack",
                        "org.apache.kylin.query.util.RestoreFromComputedColumn",
                        "org.apache.kylin.query.security.RowFilter",
                        "org.apache.kylin.query.security.HackSelectStarWithColumnACL",
                        "org.apache.kylin.query.util.SparkSQLFunctionConverter" });
    }

    public String getQueryIndexMatchRules() {
        return getOptional("kylin.query.index-match-rules", "");
    }

    public boolean useTableIndexAnswerSelectStarEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.use-tableindex-answer-select-star.enabled", FALSE));
    }

    @ThirdPartyDependencies({
            @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager", classes = {
                    "StaticAuthenticationProvider", "StaticUserGroupService", "StaticUserService" }) })
    public int getServerUserCacheExpireSeconds() {
        return Integer.parseInt(this.getOptional("kylin.server.auth-user-cache.expire-seconds", "300"));
    }

    public String getExternalAclProvider() {
        return getOptional("kylin.server.external-acl-provider", "");
    }

    public String getLDAPUserSearchBase() {
        return getOptional("kylin.security.ldap.user-search-base", "");
    }

    public String getLDAPGroupSearchBase() {
        return getOptional("kylin.security.ldap.user-group-search-base", "");
    }

    public String getLDAPAdminRole() {
        return getOptional("kylin.security.acl.admin-role", "");
    }

    public int getRowFilterLimit() {
        return Integer.parseInt(getOptional("kylin.security.acl.row-filter-limit-threshold", "100"));
    }

    public String getSuperAdminUsername() {
        return getOptional("kylin.security.acl.super-admin-username", "admin");
    }

    public boolean isDataPermissionDefaultEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.security.acl.data-permission-default-enabled", TRUE));
    }

    // ============================================================================
    // WEB
    // ============================================================================

    public String getTimeZone() {
        String timeZone = getOptional("kylin.web.timezone", "");
        if (StringUtils.isEmpty(timeZone)) {
            return TimeZone.getDefault().getID();
        }

        return timeZone;
    }

    // ============================================================================
    // RESTCLIENT
    // ============================================================================

    public int getRestClientDefaultMaxPerRoute() {
        return Integer.parseInt(this.getOptional("kylin.restclient.connection.default-max-per-route", "20"));
    }

    public int getRestClientMaxTotal() {
        return Integer.parseInt(this.getOptional("kylin.restclient.connection.max-total", "200"));
    }

    // ============================================================================
    // FAVORITE QUERY
    // ============================================================================
    public int getFavoriteQueryAccelerateThreshold() {
        return Integer.parseInt(this.getOptional("kylin.favorite.query-accelerate-threshold", "20"));
    }

    public boolean getFavoriteQueryAccelerateTipsEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.favorite.query-accelerate-tips-enable", TRUE));
    }

    public int getAutoMarkFavoriteInterval() {
        return Integer.parseInt(this.getOptional("kylin.favorite.auto-mark-detection-interval-minutes", "60")) * 60;
    }

    public int getFavoriteStatisticsCollectionInterval() {
        return Integer.parseInt(this.getOptional("kylin.favorite.statistics-collection-interval-minutes", "60")) * 60;
    }

    public int getFavoriteAccelerateBatchSize() {
        return Integer.parseInt(this.getOptional("kylin.favorite.batch-accelerate-size", "500"));
    }

    public int getFavoriteImportSqlMaxSize() {
        return Integer.parseInt(this.getOptional("kylin.favorite.import-sql-max-size", "1000"));
    }

    // unit of minute
    public long getQueryHistoryScanPeriod() {
        return Long.parseLong(this.getOptional("kylin.favorite.query-history-scan-period-minutes", "60")) * 60 * 1000L;
    }

    // unit of month
    public long getQueryHistoryMaxScanInterval() {
        return Integer.parseInt(this.getOptional("kylin.favorite.query-history-max-scan-interval", "1")) * 30 * 24 * 60
                * 60 * 1000L;
    }

    public int getAutoCheckAccStatusBatchSize() {
        return Integer.parseInt(this.getOptional("kylin.favorite.auto-check-accelerate-batch-size", "100"));
    }

    /**
     * metric
     */
    public String getCodahaleMetricsReportClassesNames() {
        return getOptional("kylin.metrics.reporter-classes", "JsonFileMetricsReporter,JmxMetricsReporter");
    }

    public String getMetricsFileLocation() {
        return getOptional("kylin.metrics.file-location", "/tmp/report.json");
    }

    public Long getMetricsReporterFrequency() {
        return Long.parseLong(getOptional("kylin.metrics.file-frequency", "5000"));
    }

    public String getHdfsWorkingDirectory(String project) {
        if (project != null) {
            return new Path(getHdfsWorkingDirectory(), project).toString() + "/";
        } else {
            return getHdfsWorkingDirectory();
        }
    }

    public String getWorkingDirectoryWithConfiguredFs(String project) {
        String engineWriteFs = getEngineWriteFs();
        if (StringUtils.isEmpty(engineWriteFs)) {
            return getHdfsWorkingDirectory(project);
        }
        // convert scheme from defaultFs to configured Fs
        engineWriteFs = new Path(engineWriteFs, getHdfsWorkingDirectoryWithoutScheme()).toString();
        if (project != null) {
            engineWriteFs = new Path(engineWriteFs, project).toString() + "/";
        }
        return engineWriteFs;
    }

    private String getReadHdfsWorkingDirectory() {
        if (StringUtils.isNotEmpty(getParquetReadFileSystem())) {
            Path workingDir = new Path(getHdfsWorkingDirectory());
            return new Path(getParquetReadFileSystem(), Path.getPathWithoutSchemeAndAuthority(workingDir)).toString()
                    + "/";
        }

        return getHdfsWorkingDirectory();
    }

    public String getReadHdfsWorkingDirectory(String project) {
        if (StringUtils.isNotEmpty(getParquetReadFileSystem())) {
            Path workingDir = new Path(getHdfsWorkingDirectory(project));
            return new Path(getParquetReadFileSystem(), Path.getPathWithoutSchemeAndAuthority(workingDir)).toString()
                    + "/";
        }

        return getHdfsWorkingDirectory(project);
    }

    public String getJdbcHdfsWorkingDirectory() {
        if (StringUtils.isNotEmpty(getJdbcFileSystem())) {
            Path workingDir = new Path(getReadHdfsWorkingDirectory());
            return new Path(getJdbcFileSystem(), Path.getPathWithoutSchemeAndAuthority(workingDir)).toString() + "/";
        }

        return getReadHdfsWorkingDirectory();
    }

    public String getBuildConf() {
        return getOptional("kylin.engine.submit-hadoop-conf-dir", "");
    }

    public String getParquetReadFileSystem() {
        return getOptional("kylin.storage.columnar.file-system", "");
    }

    public String getJdbcFileSystem() {
        return getOptional("kylin.storage.columnar.jdbc-file-system", "");
    }

    public String getPropertiesWhiteList() {
        return getOptional("kylin.web.properties.whitelist",
                "kylin.web.timezone,kylin.env,kylin.security.profile,kylin.source.default,"
                        + "metadata.semi-automatic-mode,kylin.cube.aggrgroup.is-base-cuboid-always-valid,"
                        + "kylin.htrace.show-gui-trace-toggle,kylin.web.export-allow-admin,kylin.web.export-allow-other");
    }

    public Boolean isCalciteInClauseEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.calcite-in-clause-enabled", TRUE));
    }

    public Boolean isCalciteConvertMultipleColumnsIntoOrEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.calcite-convert-multiple-columns-in-to-or-enabled", TRUE));
    }

    public Boolean isEnumerableRulesEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.calcite.enumerable-rules-enabled", FALSE));
    }

    public boolean isReduceExpressionsRulesEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.calcite.reduce-rules-enabled", TRUE));
    }

    public boolean isAggregatePushdownEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.calcite.aggregate-pushdown-enabled", FALSE));
    }

    public int getCalciteBindableCacheSize() {
        return Integer.parseInt(getOptional("kylin.query.calcite.bindable.cache.maxSize", "10"));
    }

    public int getCalciteBindableCacheConcurrencyLevel() {
        return Integer.parseInt(getOptional("kylin.query.calcite.bindable.cache.concurrencyLevel", "5"));
    }

    public int getEventPollIntervalSecond() {
        return Integer.parseInt(getOptional("kylin.job.event.poll-interval-second", "60"));
    }

    public int getIndexOptimizationLevel() {
        return Integer.parseInt(getOptional("kylin.index.optimization-level", "2"));
    }

    public double getLayoutSimilarityThreshold() {
        return safeParseDouble(getOptional("kylin.index.similarity-ratio-threshold"), SIMILARITY_THRESHOLD);
    }

    public long getSimilarityStrategyRejectThreshold() {
        return safeParseLong(getOptional("kylin.index.beyond-similarity-bias-threshold"), REJECT_SIMILARITY_THRESHOLD);
    }

    public boolean isIncludedStrategyConsiderTableIndex() {
        return Boolean.parseBoolean(getOptional("kylin.index.include-strategy.consider-table-index", TRUE));
    }

    public boolean isLowFreqStrategyConsiderTableIndex() {
        return Boolean.parseBoolean(getOptional("kylin.index.frequency-strategy.consider-table-index", TRUE));
    }

    public long getExecutableSurvivalTimeThreshold() {
        return TimeUtil.timeStringAs(getOptional("kylin.garbage.storage.executable-survival-time-threshold", "30d"),
                TimeUnit.MILLISECONDS);
    }

    public long getSourceUsageSurvivalTimeThreshold() {
        return TimeUtil.timeStringAs(getOptional("kylin.garbage.storage.sourceusage-survival-time-threshold", "90d"),
                TimeUnit.MILLISECONDS);
    }

    public long getStorageQuotaSize() {
        return ((Double) (Double.parseDouble(getOptional("kylin.storage.quota-in-giga-bytes", "10240")) * 1024 * 1024
                * 1024)).longValue();
    }

    public long getSourceUsageQuota() {
        Double d = Double.parseDouble(getOptional("kylin.storage.source-quota-in-giga-bytes", "-1"));

        return d >= 0 ? ((Double) (d * 1024 * 1024 * 1024)).longValue() : -1;
    }

    public long getCuboidLayoutSurvivalTimeThreshold() {
        return TimeUtil.timeStringAs(getOptional("kylin.garbage.storage.cuboid-layout-survival-time-threshold", "7d"),
                TimeUnit.MILLISECONDS);
    }

    public boolean getJobDataLoadEmptyNotificationEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-on-empty-data-load", FALSE));
    }

    public boolean getJobErrorNotificationEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-on-job-error", FALSE));
    }

    public Long getStorageResourceSurvivalTimeThreshold() {
        return TimeUtil.timeStringAs(this.getOptional("kylin.storage.resource-survival-time-threshold", "7d"),
                TimeUnit.MILLISECONDS);
    }

    public Boolean getTimeMachineEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.storage.time-machine-enabled", FALSE));
    }

    public boolean getJobSourceRecordsChangeNotificationEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-on-source-records-change", FALSE));
    }

    public int getMetadataBackupCountThreshold() {
        return Integer.parseInt(getOptional("kylin.metadata.backup-count-threshold", "7"));
    }

    public boolean getMetadataBackupFromSystem() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.backup-from-sys", TRUE));
    }

    public int getSchedulerLimitPerMinute() {
        return Integer.parseInt(getOptional("kylin.scheduler.schedule-limit-per-minute", "10"));
    }

    public Integer getSchedulerJobTimeOutMinute() {
        return Integer.parseInt(getOptional("kylin.scheduler.schedule-job-timeout-minute", "0"));
    }

    public Long getRateLimitPermitsPerMinute() {
        return Long.parseLong(this.getOptional("kylin.ratelimit.permits-per-minutes", "10"));
    }

    public boolean getSmartModeBrokenModelDeleteEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.broken-model-deleted-on-smart-mode", FALSE));
    }

    public boolean isMetadataKeyCaseInSensitiveEnabled() {
        boolean enabled = Boolean.parseBoolean(getOptional("kylin.metadata.key-case-insensitive", FALSE));
        if (enabled && !"testing".equals(getSecurityProfile())) {
            logger.warn("Property kylin.metadata.key-case-insensitive is not suitable for current profile {}, "
                    + "available profile is testing", getSecurityProfile());
            return false;
        }

        return enabled;
    }

    public boolean isNeedCollectLookupTableInfo() {
        return Boolean.parseBoolean(getOptional("kylin.engine.need-collect-lookup-table-info", TRUE));
    }

    public long getCountLookupTableMaxTime() {
        return Long.parseLong(getOptional("kylin.engine.count.lookup-table-max-time", "600000"));
    }

    public long getLookupTableCountDefaultValue() {
        return Long.parseLong(getOptional("kylin.engine.lookup-table-default-count-value", "1000000000"));
    }

    public int getPersistFlatTableThreshold() {
        return Integer.parseInt(getOptional("kylin.engine.persist-flattable-threshold", "1"));
    }

    public boolean isPersistFlatTableEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.persist-flattable-enabled", TRUE));
    }

    public boolean isFlatTableRedistributionEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.redistribution-flattable-enabled", FALSE));
    }

    public boolean isPersistFlatViewEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.persist-flatview", FALSE));
    }

    public boolean isPersistFlatUseSnapshotEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.persist-flat-use-snapshot-enabled", TRUE));
    }

    public boolean isBuildExcludedTableEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.build-excluded-table", FALSE));
    }

    public boolean isBuildCheckPartitionColEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.check-partition-col-enabled", TRUE));
    }

    public boolean isShardingJoinOptEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.storage.columnar.expose-sharding-trait", TRUE));
    }

    public int getQueryPartitionSplitSizeMB() {
        return Integer.parseInt(getOptional("kylin.storage.columnar.partition-split-size-mb", "64"));
    }

    public String getStorageProvider() {
        return getOptional("kylin.storage.provider", "org.apache.kylin.common.storage.DefaultStorageProvider");
    }

    @ThirdPartyDependencies({
            @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager", classes = {
                    "StaticAuthenticationProvider" }) })
    public int getServerUserCacheMaxEntries() {
        return Integer.parseInt(this.getOptional("kylin.server.auth-user-cache.max-entries", "100"));
    }

    public String getStreamingBaseJobsLocation() {
        return getOptional("kylin.engine.streaming-jobs-location", getHdfsWorkingDirectory() + "/streaming/jobs");
    }

    public Boolean getStreamingMetricsEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.engine.streaming-metrics-enabled", FALSE));
    }

    public long getStreamingSegmentMergeInterval() {
        return TimeUtil.timeStringAs(getOptional("kylin.engine.streaming-segment-merge-interval", "60s"),
                TimeUnit.SECONDS);
    }

    public long getStreamingSegmentCleanInterval() {
        return TimeUtil.timeStringAs(getOptional("kylin.engine.streaming-segment-clean-interval", "2h"),
                TimeUnit.HOURS);
    }

    public double getStreamingSegmentMergeRatio() {
        return Double.parseDouble(getOptional("kylin.engine.streaming-segment-merge-ratio", "1.5"));
    }

    public long getStreamingJobStatsSurvivalThreshold() {
        return TimeUtil.timeStringAs(getOptional("kylin.streaming.jobstats.survival-time-threshold", "7d"),
                TimeUnit.DAYS);
    }

    public Boolean getTriggerOnce() {
        return Boolean.parseBoolean(getOptional("kylin.engine.streaming-trigger-once", FALSE));
    }

    public String getLogSparkDriverPropertiesFile() {
        return getLogPropertyFile("spark-driver-log4j.xml");
    }

    public String getLogSparkExecutorPropertiesFile() {
        return getLogPropertyFile("spark-executor-log4j.xml");
    }

    public String getLogSparkStreamingDriverPropertiesFile() {
        return getLogPropertyFile("spark-streaming-driver-log4j.xml");
    }

    public String getLogSparkStreamingExecutorPropertiesFile() {
        return getLogPropertyFile("spark-streaming-executor-log4j.xml");
    }

    public String getLogSparkAppMasterPropertiesFile() {
        return getLogPropertyFile("spark-appmaster-log4j.xml");
    }

    public String getAsyncProfilerFiles() throws IOException {
        String kylinHome = getKylinHomeWithoutWarn();
        File libX64 = new File(kylinHome + "/lib/" + ASYNC_PROFILER_LIB_LINUX_X64);
        File libArm64 = new File(kylinHome + "/lib/" + ASYNC_PROFILER_LIB_LINUX_ARM64);
        return libX64.getCanonicalPath() + "," + libArm64.getCanonicalPath();
    }

    private String getLogPropertyFile(String filename) {
        String parentFolder;
        if (isDevEnv()) {
            parentFolder = Paths.get(getKylinHomeWithoutWarn(), "build", "conf").toString();
        } else if (Files.exists(Paths.get(getKylinHomeWithoutWarn(), "conf", filename))) {
            parentFolder = Paths.get(getKylinHomeWithoutWarn(), "conf").toString();
        } else {
            parentFolder = Paths.get(getKylinHomeWithoutWarn(), "server", "conf").toString();
        }
        return parentFolder + File.separator + filename;
    }

    public long getLoadHiveTablenameIntervals() {
        return Long.parseLong(getOptional("kylin.source.load-hive-tablename-interval-seconds", "3600"));
    }

    public boolean getLoadHiveTablenameEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.source.load-hive-tablename-enabled", TRUE));
    }

    //Kerberos
    public boolean getKerberosProjectLevelEnable() {
        return Boolean.parseBoolean(getOptional("kylin.kerberos.project-level-enabled", FALSE));
    }

    public boolean getTableAccessFilterEnable() {
        return Boolean.parseBoolean(getOptional("kylin.source.hive.table-access-filter-enabled", FALSE));
    }

    public String[] getHiveDatabases() {
        return getOptionalStringArray("kylin.source.hive.databases", new String[0]);
    }

    private double safeParseDouble(String value, double defaultValue) {
        double result = defaultValue;
        if (StringUtils.isEmpty(value)) {
            return result;
        }
        try {
            result = Double.parseDouble(value.trim());
        } catch (Exception e) {
            logger.error("Detect a malformed double value, set to a default value {}", defaultValue);
        }
        return result;
    }

    private long safeParseLong(String value, long defaultValue) {
        long result = defaultValue;
        if (StringUtils.isEmpty(value)) {
            return result;
        }
        try {
            result = Long.parseLong(value.trim());
        } catch (Exception e) {
            logger.error("Detect a malformed long value, set to a default value {}", defaultValue);
        }
        return result;
    }

    public Boolean isSmartModelEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.env.smart-mode-enabled", FALSE));
    }

    public Integer getProposingThreadNum() {
        return Integer.parseInt(getOptional("kylin.env.smart-thread-num", "10"));
    }

    public boolean getSkipCorrReduceRule() {
        return Boolean.parseBoolean(getOptional("kylin.smart.conf.skip-corr-reduce-rule", FALSE));
    }

    public String getEngineWriteFs() {
        String engineWriteFs = getOptional("kylin.env.engine-write-fs", "");
        return StringHelper.dropSuffix(engineWriteFs, File.separator);
    }

    public boolean isAllowedProjectAdminGrantAcl() {
        String option = getOptional("kylin.security.allow-project-admin-grant-acl", TRUE);
        return !FALSE.equals(option);
    }

    public boolean isAllowedNonAdminGenerateQueryDiagPackage() {
        return Boolean.parseBoolean(getOptional("kylin.security.allow-non-admin-generate-query-diag-package", TRUE));
    }

    public String getStreamingBaseCheckpointLocation() {
        return getOptional("kylin.engine.streaming-checkpoint-location",
                getHdfsWorkingDirectory() + "/streaming/checkpoint");
    }

    public boolean isTrackingUrlIpAddressEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.job.tracking-url-ip-address-enabled", TRUE));
    }

    public boolean getEpochCheckerEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.server.leader-race.enabled", TRUE));
    }

    public long getEpochExpireTimeSecond() {
        return Long.parseLong(getOptional("kylin.server.leader-race.heart-beat-timeout", "60"));
    }

    public long getEpochCheckerIntervalSecond() {
        return Long.parseLong(getOptional("kylin.server.leader-race.heart-beat-interval", "30"));
    }

    public double getEpochRenewTimeoutRate() {
        return Double.parseDouble(getOptional("kylin.server.leader-race.heart-beat-timeout-rate", "0.8"));
    }

    public long getDiscoveryClientTimeoutThreshold() {
        return TimeUtil.timeStringAs(getOptional("kylin.server.discovery-client-timeout-threshold", "3s"),
                TimeUnit.SECONDS);
    }

    public int getRenewEpochWorkerPoolSize() {
        return Integer.parseInt(getOptional("kylin.server.renew-epoch-pool-size", "3"));
    }

    public int getRenewEpochBatchSize() {
        return Integer.parseInt(getOptional("kylin.server.renew-batch-size", "10"));
    }

    public boolean getJStackDumpTaskEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.task.jstack-dump-enabled", TRUE));
    }

    public long getJStackDumpTaskPeriod() {
        return Long.parseLong(getOptional("kylin.task.jstack-dump-interval-minutes", "10"));
    }

    public long getJStackDumpTaskLogsMaxNum() {
        return Math.max(1L, Long.parseLong(getOptional("kylin.task.jstack-dump-log-files-max-count", "20")));
    }

    public StorageURL getStreamingStatsUrl() {
        if (StringUtils.isEmpty(getOptional(KYLIN_STREAMING_STATS_URL))) {
            return getMetadataUrl();
        }
        return StorageURL.valueOf(getOptional(KYLIN_STREAMING_STATS_URL));
    }

    public void setStreamingStatsUrl(String streamingStatsUrl) {
        setProperty(KYLIN_STREAMING_STATS_URL, streamingStatsUrl);
    }

    public StorageURL getQueryHistoryUrl() {
        if (StringUtils.isEmpty(getOptional(KYLIN_QUERY_HISTORY_URL))) {
            return getMetadataUrl();
        }
        return StorageURL.valueOf(getOptional(KYLIN_QUERY_HISTORY_URL));
    }

    public void setQueryHistoryUrl(String queryHistoryUrl) {
        setProperty(KYLIN_QUERY_HISTORY_URL, queryHistoryUrl);
    }

    public int getQueryHistoryMaxSize() {
        return Integer.parseInt(getOptional("kylin.query.queryhistory.max-size", "10000000"));
    }

    public int getQueryHistoryProjectMaxSize() {
        return Integer.parseInt(getOptional("kylin.query.queryhistory.project-max-size", "1000000"));
    }

    public int getQueryHistorySingleDeletionSize() {
        return Integer.parseInt(getOptional("kylin.query.queryhistory.single-deletion-size", "2000"));
    }

    public long getQueryHistorySurvivalThreshold() {
        return TimeUtil.timeStringAs(getOptional("kylin.query.queryhistory.survival-time-threshold", "30d"),
                TimeUnit.MILLISECONDS);
    }

    public int getQueryHistoryBufferSize() {
        return Integer.parseInt(getOptional("kylin.query.queryhistory.buffer-size", "500"));
    }

    public long getQueryHistorySchedulerInterval() {
        return TimeUtil.timeStringAs(getOptional("kylin.query.queryhistory.scheduler-interval", "3s"),
                TimeUnit.SECONDS);
    }

    public int getQueryHistoryAccelerateBatchSize() {
        return Integer.parseInt(this.getOptional("kylin.favorite.query-history-accelerate-batch-size", "1000"));
    }

    public int getQueryHistoryStatMetaUpdateBatchSize() {
        return Integer.parseInt(this.getOptional("kylin.query.query-history-stat-batch-size", "1000"));
    }

    public int getQueryHistoryAccelerateMaxSize() {
        return Integer
                .parseInt(this.getOptional("kylin.favorite.query-history-accelerate-max-size", ONE_HUNDRED_THOUSAND));
    }

    public int getQueryHistoryStatMetaUpdateMaxSize() {
        return Integer
                .parseInt(this.getOptional("kylin.query.query-history-stat-update-max-size", ONE_HUNDRED_THOUSAND));
    }

    public long getQueryHistoryAccelerateInterval() {
        return TimeUtil.timeStringAs(this.getOptional("kylin.favorite.query-history-accelerate-interval", "60m"),
                TimeUnit.MINUTES);
    }

    public long getQueryHistoryStatMetaUpdateInterval() {
        return TimeUtil.timeStringAs(this.getOptional("kylin.query.query-history-stat-interval", "30m"),
                TimeUnit.MINUTES);
    }

    public int getQueryHistoryDownloadMaxSize() {
        return Integer.parseInt(this.getOptional("kylin.query.query-history-download-max-size", ONE_HUNDRED_THOUSAND));
    }

    public int getQueryHistoryDownloadBatchSize() {
        return Integer.parseInt(this.getOptional("kylin.query.query-history-download-batch-size", "20000"));
    }

    public long getQueryHistoryDownloadTimeoutSeconds() {
        return TimeUtil.timeStringAs(this.getOptional("kylin.query.query-history-download-timeout-seconds", "300s"),
                TimeUnit.SECONDS);
    }

    public long getAsyncQueryResultRetainDays() {
        return TimeUtil.timeStringAs(this.getOptional("kylin.query.async.result-retain-days", "7d"), TimeUnit.DAYS);
    }

    public boolean isUniqueAsyncQueryYarnQueue() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.unique-async-query-yarn-queue-enabled", FALSE));
    }

    public String getAsyncQuerySparkYarnQueue() {
        return getOptional("kylin.query.async-query.spark-conf.spark.yarn.queue", DEFAULT);
    }

    public String getAsyncQueryHadoopConfDir() {
        return getOptional("kylin.query.async-query.submit-hadoop-conf-dir", "");
    }

    public String getExternalCatalogClass() {
        return getOptional("kylin.use.external.catalog", "");
    }

    public String getExternalSharedStateClass() {
        return getOptional("kylin.use.external.sharedState", "org.apache.spark.sql.kylin.external.KylinSharedState");
    }

    public String getExternalSessionStateBuilderClass() {
        return getOptional("kylin.use.external.sessionStateBuilder",
                "org.apache.spark.sql.kylin.external.KylinSessionStateBuilder");
    }

    public Boolean isSparderAsync() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.init-sparder-async", TRUE));
    }

    public boolean getRandomAdminPasswordEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.random-admin-password.enabled", TRUE));
    }

    public long getCatchUpInterval() {
        return TimeUtil.timeStringAs(getOptional("kylin.metadata.audit-log.catchup-interval", "5s"), TimeUnit.SECONDS);
    }

    public long getUpdateJobInfoTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.job.update-job-info-timeout", "30s"), TimeUnit.MILLISECONDS);
    }

    public long getCatchUpTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.metadata.audit-log.catchup-timeout", "2s"), TimeUnit.SECONDS);
    }

    public long getCatchUpMaxTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.metadata.audit-log.catchup-max-timeout", "60s"),
                TimeUnit.SECONDS);
    }

    public long getUpdateEpochTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.server.leader-race.update-heart-beat-timeout", "30s"),
                TimeUnit.SECONDS);
    }

    public boolean isQueryEscapedLiteral() {
        return Boolean.parseBoolean(getOptional("kylin.query.parser.escaped-string-literals", FALSE));
    }

    public boolean isSessionSecureRandomCreateEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.web.session.secure-random-create-enabled", FALSE));
    }

    public boolean isSessionJdbcEncodeEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.web.session.jdbc-encode-enabled", FALSE));
    }

    public String getSpringStoreType() {
        return getOptional("spring.session.store-type", "");
    }

    public int getJdbcSessionMaxInactiveInterval() {
        return Integer.parseInt(getOptional("spring.session.timeout", "3600"));
    }

    public int getCapacitySampleRows() {
        return Integer.parseInt(getOptional("kylin.capacity.sample-rows", "1000"));
    }

    public String getUserPasswordEncoder() {
        return getOptional("kylin.security.user-password-encoder",
                "org.apache.kylin.rest.security.CachedBCryptPasswordEncoder");
    }

    public int getRecommendationPageSize() {
        return Integer.parseInt(getOptional("kylin.model.recommendation-page-size", "500"));
    }

    // Guardian Process
    public boolean isGuardianEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.guardian.enabled", FALSE));
    }

    private long getConfigItemSeconds(String configItem, long defaultLongValue, long rangeStart, long rangeEnd) {
        long resultValue = defaultLongValue;
        try {
            resultValue = TimeUtil.timeStringAs(
                    getOptional(configItem, String.format(Locale.ROOT, "%dS", defaultLongValue)), TimeUnit.SECONDS);
        } catch (Exception e) {
            return resultValue;
        }

        return rangeStart <= resultValue && resultValue <= rangeEnd ? resultValue : defaultLongValue;
    }

    private int getConfigItemIntValue(String configItem, int defaultIntValue, int rangeStart, int rangeEnd) {
        int resultValue = defaultIntValue;
        try {
            resultValue = Integer.parseInt(getOptional(configItem, String.valueOf(defaultIntValue)));
        } catch (Exception e) {
            return resultValue;
        }

        return rangeStart <= resultValue && resultValue <= rangeEnd ? resultValue : defaultIntValue;
    }

    private double getConfigItemDoubleValue(String configItem, double defaultDoubleValue, double rangeStart,
            double rangeEnd) {
        double resultValue = defaultDoubleValue;
        try {
            resultValue = Integer.parseInt(getOptional(configItem, String.valueOf(defaultDoubleValue)));
        } catch (Exception e) {
            return resultValue;
        }

        return rangeStart <= resultValue && resultValue <= rangeEnd ? resultValue : defaultDoubleValue;
    }

    public long getGuardianCheckInterval() {
        return getConfigItemSeconds("kylin.guardian.check-interval", MINUTE, MINUTE, 60 * MINUTE);
    }

    public long getGuardianCheckInitDelay() {
        return getConfigItemSeconds("kylin.guardian.check-init-delay", 5 * MINUTE, MINUTE, 60 * MINUTE);
    }

    public boolean isGuardianHAEnabled() {
        return !FALSE.equalsIgnoreCase(getOptional("kylin.guardian.ha-enabled", TRUE));
    }

    public long getGuardianHACheckInterval() {
        return getConfigItemSeconds("kylin.guardian.ha-check-interval", MINUTE, MINUTE, 60 * MINUTE);
    }

    public long getGuardianHACheckInitDelay() {
        return getConfigItemSeconds("kylin.guardian.ha-check-init-delay", 5 * MINUTE, MINUTE, 60 * MINUTE);
    }

    public String getGuardianHealthCheckers() {
        return getOptional("kylin.guardian.checkers",
                "org.apache.kylin.tool.daemon.checker.KEProcessChecker,org.apache.kylin.tool.daemon.checker.FullGCDurationChecker,org.apache.kylin.tool.daemon.checker.KEStatusChecker");
    }

    public int getGuardianFullGCCheckFactor() {
        return getConfigItemIntValue("kylin.guardian.full-gc-check-factor", 5, 1, 60);
    }

    public boolean isFullGCRatioBeyondRestartEnabled() {
        return !FALSE.equalsIgnoreCase(getOptional("kylin.guardian.full-gc-duration-ratio-restart-enabled", TRUE));
    }

    public double getGuardianFullGCRatioThreshold() {
        return getConfigItemDoubleValue("kylin.guardian.full-gc-duration-ratio-threshold", 75.0, 0.0, 100.0);
    }

    public boolean isDowngradeOnFullGCBusyEnable() {
        return !FALSE.equalsIgnoreCase(getOptional("kylin.guardian.downgrade-on-full-gc-busy-enabled", TRUE));
    }

    public double getGuardianFullGCHighWatermark() {
        return getConfigItemDoubleValue("kylin.guardian.full-gc-busy-high-watermark", 40.0, 0.0, 100.0);
    }

    public double getGuardianFullGCLowWatermark() {
        return getConfigItemDoubleValue("kylin.guardian.full-gc-busy-low-watermark", 20.0, 0.0, 100.0);
    }

    public int getGuardianApiFailThreshold() {
        return getConfigItemIntValue("kylin.guardian.api-fail-threshold", 5, 1, 100);
    }

    public boolean isSparkFailRestartKeEnabled() {
        return !FALSE.equalsIgnoreCase(getOptional("kylin.guardian.restart-spark-fail-restart-enabled", TRUE));
    }

    public int getGuardianSparkFailThreshold() {
        return getConfigItemIntValue("kylin.guardian.restart-spark-fail-threshold", 3, 1, 100);
    }

    public int getDowngradeParallelQueryThreshold() {
        return getConfigItemIntValue("kylin.guardian.downgrade-mode-parallel-query-threshold", 10, 0, 100);
    }

    public boolean isSlowQueryKillFailedRestartKeEnabled() {
        return !FALSE.equalsIgnoreCase(getOptional("kylin.guardian.kill-slow-query-fail-restart-enabled", TRUE));
    }

    public Integer getGuardianSlowQueryKillFailedThreshold() {
        return getConfigItemIntValue("kylin.guardian.kill-slow-query-fail-threshold", 3, 1, 100);
    }

    @ThirdPartyDependencies({
            @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager", classes = {
                    "AuthenticationClient" }) })
    public Long getLightningClusterId() {
        return Long.parseLong(getOptional("kylin.lightning.cluster-id", "0"));
    }

    @ThirdPartyDependencies({
            @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager", classes = {
                    "AuthenticationClient" }) })
    public Long getLightningWorkspaceId() {
        return Long.parseLong(getOptional("kylin.lightning.workspace-id", "0"));
    }

    @ThirdPartyDependencies({
            @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager", classes = {
                    "AuthenticationClient" }) })
    public String getLightningServerZkNode() {
        return getOptional("kylin.lightning.server.zookeeper-node", "/kylin/management");
    }

    public String getSparkLogExtractor() {
        return getOptional("kylin.tool.spark-log-extractor", "org.apache.kylin.tool.YarnSparkLogExtractor");
    }

    public String getLicenseExtractor() {
        return getOptional("kylin.tool.license-extractor", "org.apache.kylin.rest.service.DefaultLicenseExtractor");

    }

    public String getMountSparkLogDir() {
        return getOptional("kylin.tool.mount-spark-log-dir", "");
    }

    public boolean cleanDiagTmpFile() {
        return Boolean.parseBoolean(getOptional("kylin.tool.clean-diag-tmp-file", FALSE));
    }

    public int getTurnMaintainModeRetryTimes() {
        return Integer.parseInt(getOptional("kylin.tool.turn-on-maintainmodel-retry-times", "3"));
    }

    public int getSuggestModelSqlLimit() {
        return Integer.parseInt(getOptional("kylin.model.suggest-model-sql-limit", "200"));
    }

    public int getSuggestModelSqlInterval() {
        return Integer.parseInt(getOptional("kylin.model.suggest-model-sql-interval", "10"));
    }

    public String getIntersectFilterOrSeparator() {
        return getOptional("kylin.query.intersect.separator", "|");
    }

    public int getBitmapValuesUpperBound() {
        return Integer.parseInt(getOptional("kylin.query.bitmap-values-upper-bound", "10000000"));
    }

    public String getUIProxyLocation() {
        return getOptional("kylin.query.ui.proxy-location", KYLIN_ROOT);
    }

    public boolean isHistoryServerEnable() {
        return Boolean.parseBoolean(getOptional("kylin.history-server.enable", FALSE));
    }

    public String getJobFinishedNotifierUrl() {
        return getOptional("kylin.job.finished-notifier-url", null);
    }

    public String getJobFinishedNotifierUsername() {
        return getOptional("kylin.job.finished-notifier-username");
    }

    public String getJobFinishedNotifierPassword() {
        return EncryptUtil.getDecryptedValue(getOptional("kylin.job.finished-notifier-password"));
    }

    public int getMaxModelDimensionMeasureNameLength() {
        return Integer.parseInt(getOptional("kylin.model.dimension-measure-name.max-length", "300"));
    }

    public int getAuditLogBatchSize() {
        return Integer.parseInt(getOptional("kylin.metadata.audit-log.batch-size", "5000"));
    }

    public int getAuditLogBatchTimeout() {
        return (int) TimeUtil.timeStringAs(getOptional("kylin.metadata.audit-log.batch-timeout", "30s"),
                TimeUnit.SECONDS);
    }

    public long getDiagTaskTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.diag.task-timeout", "180s"), TimeUnit.SECONDS);
    }

    public ImmutableSet<String> getDiagTaskTimeoutBlackList() {
        String lists = getOptional("kylin.diag.task-timeout-black-list", "METADATA,LOG").toUpperCase(Locale.ROOT);
        return ImmutableSet.copyOf(lists.split(","));
    }

    public boolean isMetadataOnlyForRead() {
        return Boolean.parseBoolean(getOptional("kylin.env.metadata.only-for-read", FALSE));
    }

    public String getGlobalDictV2StoreImpl() {
        return getOptional("kylin.dictionary.globalV2-store-class-name", "org.apache.spark.dict.NGlobalDictHDFSStore");
    }

    public boolean isV2DictEnable() {
        return Boolean.parseBoolean(getOptional("kylin.build.is-v2dict-enable", TRUE));
    }

    public boolean isV3DictEnable() {
        return Boolean.parseBoolean(getOptional("kylin.build.is-v3dict-enable", FALSE));
    }

    public boolean isConvertV3DictEnable() {
        return Boolean.parseBoolean(getOptional("kylin.build.is-convert-v3dict-enable", FALSE));
    }

    public String getV3DictDBName() {
        return getOptional("kylin.build.v3dict-db-name", DEFAULT);
    }

    public String getLogLocalWorkingDirectory() {
        return getOptional("kylin.engine.log.local-working-directory", "");
    }

    public String[] getJobResourceLackIgnoreExceptionClasses() {
        return getOptionalStringArray("kylin.job.resource-lack-ignore-exception-classes",
                new String[] { "com.amazonaws.services.s3.model.AmazonS3Exception" });
    }

    @ThirdPartyDependencies({
            @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager", classes = {
                    "StaticAuthenticationProvider" }) })
    public String getAADUsernameClaim() {
        return getOptional("kylin.server.aad-username-claim", "upn");
    }

    @ThirdPartyDependencies({
            @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager", classes = {
                    "StaticAuthenticationProvider" }) })
    public String getAADClientId() {
        return getOptional("kylin.server.aad-client-id", "");
    }

    @ThirdPartyDependencies({
            @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager", classes = {
                    "StaticAuthenticationProvider" }) })
    public String getAADTenantId() {
        return getOptional("kylin.server.aad-tenant-id", "");
    }

    @ThirdPartyDependencies({
            @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager", classes = {
                    "StaticAuthenticationProvider" }) })
    public int getAADTokenClockSkewSeconds() {
        return Integer.parseInt(this.getOptional("kylin.server.aad-token-clock-skew-seconds", "0"));
    }

    @ThirdPartyDependencies({
            @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager", classes = {
                    "StaticAuthenticationProvider" }) })
    public String getOktaOauth2Issuer() {
        return getOptional("kylin.server.okta-oauth2-issuer", "");
    }

    @ThirdPartyDependencies({
            @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager", classes = {
                    "StaticAuthenticationProvider" }) })
    public String getOktaClientId() {
        return getOptional("kylin.server.okta-client-id", "");
    }

    public long buildResourceStateCheckInterval() {
        return TimeUtil.timeStringAs(getOptional("kylin.build.resource.state-check-interval-seconds", "1s"),
                TimeUnit.SECONDS);
    }

    public int buildResourceConsecutiveIdleStateNum() {
        return Integer.parseInt(getOptional("kylin.build.resource.consecutive-idle-state-num", "3"));
    }

    public double buildResourceLoadRateThreshold() {
        return Double.parseDouble(getOptional("kylin.build.resource.load-rate-threshold", "10"));
    }

    public boolean skipFreshAlluxio() {
        return Boolean.parseBoolean(getOptional("kylin.build.skip-fresh-alluxio", FALSE));
    }

    public Set<String> getUserDefinedNonCustomProjectConfigs() {
        String configs = getOptional("kylin.server.non-custom-project-configs");
        if (StringUtils.isEmpty(configs)) {
            return Sets.newHashSet();
        }
        return Sets.newHashSet(configs.split(","));
    }

    public Set<String> getNonCustomProjectConfigs() {
        val allConfigNameSet = getUserDefinedNonCustomProjectConfigs();
        allConfigNameSet.addAll(NonCustomProjectLevelConfig.listAllConfigNames());
        return allConfigNameSet;
    }

    public String getDiagObfLevel() {
        return getOptional("kylin.diag.obf.level", "OBF").toUpperCase(Locale.ROOT);
    }

    public boolean isDimensionRangeFilterEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.storage.columnar.dimension-range-filter-enabled", TRUE));
    }

    public int getSegmentExecMaxThreads() {
        return Integer.parseInt(getOptional("kylin.engine.segment-exec-max-threads", "200"));
    }

    public boolean isSegmentParallelBuildEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.segment-exec-parallel-enabled", FALSE));
    }

    public boolean isEmbeddedEnable() {
        return Boolean.parseBoolean(getOptional("kylin.storage.columnar.file-system.journal.embedded-enable", FALSE));
    }

    public String getSystemProfileExtractor() {
        return getOptional("kylin.tool.system-profile-extractor",
                "org.apache.kylin.tool.LightningSystemProfileExtractor");
    }

    public boolean isCharDisplaySizeEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.char-display-size-enabled", TRUE));
    }

    public boolean isPrometheusMetricsEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metrics.prometheus-enabled", TRUE));
    }

    public boolean getCheckResourceEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.build.resource.check-enabled", FALSE));
    }

    public long getCheckResourceTimeLimit() {
        return Long.parseLong(getOptional("kylin.build.resource.check-retry-limit-minutes", "30"));
    }

    public boolean isBatchGetRowAclEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.batch-get-row-acl-enabled", FALSE));
    }

    public boolean isAdaptiveSpanningTreeEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.adaptive-spanning-tree-enabled", FALSE));
    }

    public double getAdaptiveSpanningTreeThreshold() {
        return Double.parseDouble(getOptional("kylin.engine.adaptive-spanning-tree-threshold", "0.5d"));
    }

    public int getAdaptiveSpanningTreeBatchSize() {
        return Integer.parseInt(getOptional("kylin.engine.adaptive-spanning-tree-batch-size", "10"));
    }

    public boolean isIndexColumnFlatTableEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.index-column-flattable-enabled", FALSE));
    }

    public boolean isInferiorFlatTableEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.inferior-flattable-enabled", FALSE));
    }

    public String getInferiorFlatTableStorageLevel() {
        return getOptional("kylin.engine.inferior-flattable-storage-level", "MEMORY_AND_DISK");
    }

    public int getInferiorFlatTableGroupFactor() {
        return Integer.parseInt(getOptional("kylin.engine.inferior-flattable-group-factor", "20"));
    }

    public int getInferiorFlatTableDimensionFactor() {
        return Integer.parseInt(getOptional("kylin.engine.inferior-flattable-dimension-factor", "10"));
    }

    public int getFlatTableCoalescePartitionNum() {
        return Integer.parseInt(getOptional("kylin.engine.flattable-coalesce-partition-num", "-1"));
    }

    public boolean isNeedReplayConsecutiveLog() {
        return Boolean.parseBoolean(getOptional("kylin.auditlog.replay-need-consecutive-log", TRUE));
    }

    public int getReplayWaitMaxRetryTimes() {
        return Integer.parseInt(getOptional("kylin.auditlog.replay-wait-max-retry-times", "3"));
    }

    public long getReplayWaitMaxTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.auditlog.replay-wait-max-timeout", "100ms"),
                TimeUnit.MILLISECONDS);
    }

    public long getEventualReplayDelayItemTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.auditlog.replay-eventual-delay-item-timeout", "5m"),
                TimeUnit.MILLISECONDS);
    }

    public int getEventualReplayDelayItemBatch() {
        return Integer.parseInt(getOptional("kylin.auditlog.replay-eventual-delay-item-batch", "1000"));
    }

    public boolean skipCheckFlatTable() {
        return Boolean.parseBoolean(getOptional("kylin.model.skip-check-flattable", FALSE));
    }

    public boolean isQueryExceptionCacheEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.exception-cache-enabled", FALSE));
    }

    public int getQueryExceptionCacheThresholdTimes() {
        return Integer.parseInt(this.getOptional("kylin.query.exception-cache-threshold-times", "2"));
    }

    public int getQueryExceptionCacheThresholdDuration() {
        return Integer.parseInt(this.getOptional("kylin.query.exception-cache-threshold-duration", "2000"));
    }

    public boolean isQueryBlacklistEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.blacklist-enabled", FALSE));
    }

    public boolean isSkipEmptySegments() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.skip-empty-segments", TRUE));
    }

    public long getEhcacheTimeToIdleSecondsForException() {
        return Long.parseLong(getOptional("kylin.cache.ehcache.exception-time-to-idle-seconds", "600"));
    }

    public boolean isSkipBasicAuthorization() {
        return Boolean.parseBoolean(getOptional("kap.authorization.skip-basic-authorization", FALSE));
    }

    public boolean isReadTransactionalTableEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.build.resource.read-transactional-table-enabled", TRUE));
    }

    public String getFlatTableStorageFormat() {
        return this.getOptional("kylin.source.hive.flat-table-storage-format", "SEQUENCEFILE");
    }

    public String getFlatTableFieldDelimiter() {
        return this.getOptional("kylin.source.hive.flat-table-field-delimiter", "\u001F");
    }

    public long[] getMetricsQuerySlaSeconds() {
        return getOptionalLongArray("kylin.metrics.query.sla.seconds", new String[] { "3", "15", "60" });
    }

    public long[] getMetricsJobSlaMinutes() {
        return getOptionalLongArray("kylin.metrics.job.sla.minutes", new String[] { "30", "60", "300" });
    }

    public boolean isSpark3ExecutorPrometheusEnabled() {
        return Boolean
                .parseBoolean(getOptional("kylin.storage.columnar.spark-conf.spark.ui.prometheus.enabled", FALSE));
    }

    public String getSpark3DriverPrometheusServletClass() {
        return this.getOptional("kylin.storage.columnar.spark-conf.spark.metrics.conf.*.sink.prometheusServlet.class",
                "");
    }

    public String getSpark3DriverPrometheusServletPath() {
        return this.getOptional("kylin.storage.columnar.spark-conf.spark.metrics.conf.*.sink.prometheusServlet.path",
                "");
    }

    protected final long[] getOptionalLongArray(String prop, String[] dft) {
        String[] strArray = getOptionalStringArray(prop, dft);
        long[] longArray;
        try {
            longArray = Arrays.stream(strArray).mapToLong(Long::parseLong).toArray();
        } catch (NumberFormatException ex) {
            logger.warn("NumberFormatException, prop={}", prop, ex);
            longArray = Arrays.stream(dft).mapToLong(Long::parseLong).toArray();
        }
        return longArray;
    }

    public String getUpdateTopNTime() {
        return getOptional("kylin.smart.update-topn-time", "23:00");
    }

    public boolean getUsingUpdateFrequencyRule() {
        return Boolean.parseBoolean(getOptional("kylin.smart.frequency-rule-enable", TRUE));
    }

    public long getUpdateTopNTimeGap() {
        return Long.parseLong(getOptional("kylin.smart.update-topn-time-gap", "3600000"));
    }

    public String getRecommendationCostMethod() {
        return getOptional("kylin.smart.update-cost-method", "HIT_COUNT");
    }

    public int getEventBusHandleThreadCount() {
        return Integer.parseInt(getOptional("kylin.env.eventbus-handle-count", "100"));
    }

    public long getMetadataCheckDuration() {
        return Long.parseLong(getOptional("kylin.env.health-check-interval", "3000"));
    }

    public boolean isMeasureNameCheckEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.model.measure-name-check-enabled", TRUE));
    }

    public boolean checkModelDependencyHealthy() {
        return Boolean.parseBoolean(getOptional("kylin.model.check-model-dependency-health", FALSE));
    }

    public boolean isTableFastReload() {
        return Boolean.parseBoolean(getOptional("kylin.table.fast-reload-enabled", TRUE));
    }

    public boolean isUnitOfWorkSimulationEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.env.unitofwork-simulation-enabled", FALSE));
    }

    public ForceToTieredStorage getSystemForcedToTieredStorage() {
        int i = Integer.parseInt(getOptional("kylin.second-storage.route-when-ch-fail", "0"));
        return ForceToTieredStorage.values()[i];
    }

    public ForceToTieredStorage getProjectForcedToTieredStorage() {
        int i = Integer.parseInt(getOptional("kylin.second-storage.route-when-ch-fail"));
        return ForceToTieredStorage.values()[i];
    }

    public long getClusterManagerHealthCheckMaxTimes() {
        return Long.parseLong(getOptional("kylin.engine.cluster-manager-health-check-max-times", "10"));
    }

    public long getClusterManagerHealCheckIntervalSecond() {
        return Long.parseLong(getOptional("kylin.engine.cluster-manager-heal-check-interval-second", "120"));
    }

    public boolean isRemoveLdapCustomSecurityLimitEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.security.remove-ldap-custom-security-limit-enabled", FALSE));
    }

    public String getSparkBuildJobHandlerClassName() {
        return getOptional("kylin.engine.spark.build-job-handler-class-name",
                "org.apache.kylin.engine.spark.job.DefaultSparkBuildJobHandler");
    }

    public String getBuildJobProgressReporter() {
        return getOptional("kylin.engine.spark.build-job-progress-reporter",
                "org.apache.kylin.engine.spark.job.RestfulJobProgressReport");
    }

    public String getBuildJobEnviromentAdaptor() {
        return getOptional("kylin.engine.spark.build-job-enviroment-adaptor",
                "org.apache.kylin.engine.spark.job.DefaultEnviromentAdaptor");
    }

    public boolean useDynamicS3RoleCredentialInTable() {
        return Boolean.parseBoolean(getOptional("kylin.env.use-dynamic-S3-role-credential-in-table", FALSE));

    }

    public String getJobCallbackLanguage() {
        return getOptional("kylin.job.callback-language", "en");
    }

    public Integer getMaxResultRows() {
        return Integer.parseInt(this.getOptional("kylin.query.max-result-rows", "0"));
    }

    public boolean isBigQueryPushDown() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.big-query-pushdown", FALSE));
    }

    public Integer getLoadHiveTableWaitSparderSeconds() {
        return Integer.parseInt(this.getOptional("kylin.source.load-hive-table-wait-sparder-seconds", "900"));
    }

    public Integer getLoadHiveTableWaitSparderIntervals() {
        return Integer.parseInt(this.getOptional("kylin.source.load-hive-table-wait-sparder-interval-seconds", "10"));
    }

    public int getJobTagMaxSize() {
        return Integer.parseInt(this.getOptional("kylin.job.tag-max-size", "1024"));
    }

    public String getJobSchedulerMode() {
        return getOptional("kylin.engine.job-scheduler-mode", "DAG");
    }

    public String getKylinEngineSegmentOnlineMode() {
        return getOptional("kylin.engine.segment-online-mode", SegmentOnlineMode.DFS.toString());
    }

    public int getSecondStorageLoadThreadsPerJob() {
        int process = Integer.parseInt(getOptional("kylin.second-storage.load-threads-per-job", "3"));
        if (process <= 0) {
            process = 1;
        }
        return process;
    }

    public int getSecondStorageCommitThreadsPerJob() {
        int process = Integer.parseInt(getOptional("kylin.second-storage.commit-threads-per-job", "10"));
        if (process <= 0) {
            process = 1;
        }
        return process;
    }

    public long getSecondStorageWaitIndexBuildSecond() {
        return Long.parseLong(getOptional("kylin.second-storage.wait-index-build-second", "10"));
    }

    public String getSecondStorageJDBCKeepAliveTimeout() {
        return getOptional("kylin.second-storage.jdbc-keep-alive-timeout", "600000");
    }

    public String getSecondStorageJDBCSocketTimeout() {
        return getOptional("kylin.second-storage.jdbc-socket-timeout", "600000");
    }

    public String getSecondStorageJDBCExtConfig() {
        return getOptional("kylin.second-storage.jdbc-ext-config", "connect_timeout=3");
    }

    public long getRoutineOpsTaskTimeOut() {
        return TimeUtil.timeStringAs(getOptional("kylin.metadata.ops-cron-timeout", "4h"), TimeUnit.MILLISECONDS);
    }

    public boolean buildJobProfilingEnabled() {
        return !Boolean.parseBoolean(System.getProperty("spark.local", FALSE))
                && Boolean.parseBoolean(getOptional("kylin.engine.async-profiler-enabled", TRUE));
    }

    public long buildJobProfilingResultTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.engine.async-profiler-result-timeout", "60s"),
                TimeUnit.MILLISECONDS);
    }

    public long buildJobProfilingProfileTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.engine.async-profiler-profile-timeout", "5m"),
                TimeUnit.MILLISECONDS);
    }

    // getNestedPath() brought in a / at the end
    public String getJobTmpProfilerFlagsDir(String project, String jobId) {
        return getJobTmpDir(project) + getNestedPath(jobId) + "profiler_flags";
    }

    public boolean exposeAllModelRelatedColumns() {
        return Boolean.parseBoolean(getOptional("kylin.model.tds-expose-all-model-related-columns", TRUE));
    }

    public boolean exposeModelJoinKey() {
        return Boolean.parseBoolean(getOptional("kylin.model.tds-expose-model-join-key", TRUE));
    }

    public boolean skipCheckTds() {
        return Boolean.parseBoolean(getOptional("kylin.model.skip-check-tds", TRUE));
    }

    public boolean isHdfsMetricsPeriodicCalculationEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metrics.hdfs-periodic-calculation-enabled", TRUE));
    }

    public long getHdfsMetricsPeriodicCalculationInterval() {
        return TimeUtil.timeStringAs(getOptional("kylin.metrics.hdfs-periodic-calculation-interval", "5m"),
                TimeUnit.MILLISECONDS);
    }

    public String getHdfsMetricsDir(String metricFile) {
        return getHdfsWorkingDirectory() + METRICS + metricFile;
    }

    public boolean isSkipResourceCheck() {
        return Boolean.parseBoolean(getOptional("kylin.build.resource.skip-resource-check", FALSE));
    }

    public int getSecondStorageSkippingIndexGranularity() {
        int granularity = Integer.parseInt(getOptional("kylin.second-storage.skipping-index.granularity", "3"));
        return granularity <= 0 ? 3 : granularity;
    }

    public String getSecondStorageSkippingIndexBloomFilter() {
        return getOptional("kylin.second-storage.skipping-index.bloom-filter", "0.025");
    }

    public int getSecondStorageSkippingIndexSet() {
        int size = Integer.parseInt(getOptional("kylin.second-storage.skipping-index.set", "100"));
        return size <= 0 ? 100 : size;
    }

    public boolean getSecondStorageIndexAllowNullableKey() {
        return Boolean.parseBoolean(getOptional("kylin.second-storage.allow-nullable-skipping-index", TRUE));
    }

    public int getSecondStorageWaitLockTimeout() {
        return Integer.parseInt(getOptional("kylin.second-storage.wait-lock-timeout", "180"));
    }

    public boolean isBuildSegmentOverlapEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.build.segment-overlap-enabled", FALSE));
    }

    public boolean isJobTmpDirALLPermissionEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.job-tmp-dir-all-permission-enabled", FALSE));
    }

    public boolean isProjectMergeWithBloatEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.project-merge-with-bloat-enabled", "true"));
    }

    public int getProjectMergeRuleBloatThreshold() {
        return Integer.parseInt(getOptional("kylin.query.project-merge-bloat-threshold", "0"));
    }

    public boolean isStorageQuotaEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.storage.check-quota-enabled", FALSE));
    }

    public boolean skipShardPruningForInExpr() {
        return Boolean.parseBoolean(getOptional("kylin.query.skip-shard-pruning-for-in", FALSE));
    }

    public boolean isDDLEnabled() {
        return isDDLLogicalViewEnabled() || isDDLHiveEnabled();
    }

    public boolean isDDLLogicalViewEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.source.ddl.logical-view.enabled", FALSE));
    }

    public boolean isDDLHiveEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.source.ddl.hive.enabled", FALSE));
    }

    public String getDDLLogicalViewDB() {
        return getOptional("kylin.source.ddl.logical-view.database", "KYLIN_LOGICAL_VIEW");
    }

    public int getDDLLogicalViewCatchupInterval() {
        return Integer.parseInt(getOptional("kylin.source.ddl.logical-view-catchup-interval", "60"));
    }

    public boolean isDataCountCheckEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.build.data-count-check-enabled", FALSE));
    }

    public boolean isNonStrictCountCheckAllowed() {
        return Boolean.parseBoolean(getOptional("kylin.build.allow-non-strict-count-check", FALSE));
    }

    public long queryDiagnoseCollectionTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.query.diagnose-collection-timeout", "30s"),
                TimeUnit.MILLISECONDS);
    }

    public boolean queryDiagnoseEnable() {
        return !Boolean.parseBoolean(System.getProperty("spark.local", FALSE))
                && Boolean.parseBoolean(getOptional("kylin.query.diagnose-enabled", TRUE));
    }

    public long getMaxMeasureSegmentPrunerBeforeDays() {
        return Long.parseLong(getOptional("kylin.query.max-measure-segment-pruner-before-days", "-1"));
    }

    public int getQueryConcurrentRunningThresholdForPushDown() {
        return Integer.parseInt(getOptional("kylin.query.pushdown-concurrent-running-threshold", "10"));
    }

    // ============================================================================
    // Cost based index Planner
    // ============================================================================

    public boolean enableCostBasedIndexPlanner() {
        // If enable the cost based planner, will recommend subset of index layouts from the index rule.
        return Boolean.parseBoolean(getOptional("kylin.index.costbased.enabled", FALSE));
    }

    public int getCostBasedPlannerGreedyAlgorithmAutoThreshold() {
        return Integer.parseInt(getOptional("kylin.index.costbased.algorithm-threshold-greedy", "8"));
    }

    public int getCostBasedPlannerGeneticAlgorithmAutoThreshold() {
        return Integer.parseInt(getOptional("kylin.index.costbased.algorithm-threshold-genetic", "23"));
    }

    public double getCostBasedPlannerExpansionRateThreshold() {
        return Double.parseDouble(getOptional("kylin.index.costbased.expansion-threshold", "15.0"));
    }

    public double getCostBasedPlannerBPUSMinBenefitRatio() {
        return Double.parseDouble(getOptional("kylin.index.costbased.bpus-min-benefit-ratio", "0.01"));
    }

    public int getStatsHLLPrecision() {
        return Integer.parseInt(getOptional("kylin.index.costbased.sampling-hll-precision", "14"));
    }

    public double getJobCuboidSizeRatio() {
        return Double.parseDouble(getOptional("kylin.index.costbased.model-size-estimate-ratio", "0.25"));
    }

    public double getJobCuboidSizeCountDistinctRatio() {
        return Double.parseDouble(getOptional("kylin.index.costbased.model-size-estimate-countdistinct-ratio", "0.5"));
    }

    public double getJobCuboidSizeTopNRatio() {
        return Double.parseDouble(getOptional("kylin.index.costbased.model-size-estimate-topn-ratio", "0.5"));
    }

    public int getJobPerReducerHLLCuboidNumber() {
        return Integer.parseInt(getOptional("kylin.index.costbased.per-reducer-hll-cuboid-number", "100"));
    }

    public int getJobHLLMaxReducerNumber() {
        return Integer.parseInt(getOptional("kylin.index.costbased.hll-max-reducer-number", "1"));
    }

    public boolean isTableLoadThresholdEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.table.load-threshold-enabled", TRUE));
    }

    public boolean isIndexEnableOperatorDesign() {
        return Boolean.parseBoolean(getOptional("kylin.index.enable-operator-design", FALSE));
    }

    public int getQueryFilterCollectInterval() {
        return Integer.parseInt(getOptional("kylin.query.filter.collect-interval", "1800"));
    }

    public boolean isBloomCollectFilterEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.bloom.collect-filter.enabled", TRUE));
    }

    public boolean isCollectQueryMetricsEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.collect-metrics.enabled", TRUE));
    }

    public boolean isBloomBuildEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.bloom.build.enabled", FALSE));
    }

    public int getBloomBuildColumnMaxNum() {
        return Integer.parseInt(getOptional("kylin.bloom.build.column.max-size", "3"));
    }

    public String getBloomBuildColumn() {
        return getOptional("kylin.bloom.build.column", "");
    }

    public int getBloomBuildColumnNvd() {
        return Integer.parseInt(getOptional("kylin.bloom.build.column.nvd", "200000"));
    }

    public int getAutoShufflePartitionMultiple() {
        return Integer.parseInt(getOptional("kylin.query.pushdown.auto-set-shuffle-partitions-multiple", "3"));
    }

    public int getAutoShufflePartitionTimeOut() {
        return Integer.parseInt(getOptional("kylin.query.pushdown.auto-set-shuffle-partitions-timeout", "30"));
    }

    public boolean isKylinMultiTenantEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.multi-tenant.enabled", FALSE));
    }

    public long getKylinMultiTenantRouteTaskTimeOut() {
        return TimeUtil.timeStringAs(getOptional("kylin.multi-tenant.route-task-timeout", "30min"),
                TimeUnit.MILLISECONDS);
    }
}
