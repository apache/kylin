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
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * An abstract class to encapsulate access to a set of 'properties'.
 * Subclass can override methods in this class to extend the content of the 'properties',
 * with some override values for example.
 */
abstract public class KylinConfigBase implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(KylinConfigBase.class);

    /*
     * DON'T DEFINE CONSTANTS FOR PROPERTY KEYS!
     * 
     * For 1), no external need to access property keys, all accesses are by public methods.
     * For 2), it's cumbersome to maintain constants at top and code at bottom.
     * For 3), key literals usually appear only once.
     */

    public static String getKylinHome() {
        String kylinHome = System.getenv("KYLIN_HOME");
        if (StringUtils.isEmpty(kylinHome)) {
            logger.warn("KYLIN_HOME was not set");
        }
        return kylinHome;
    }

    public static String getSparkHome() {
        String sparkHome = System.getenv("SPARK_HOME");
        if (StringUtils.isNotEmpty(sparkHome)) {
            logger.info("SPARK_HOME was set to " + sparkHome);
            return sparkHome;
        }

        sparkHome = System.getProperty("SPARK_HOME");
        if (StringUtils.isNotEmpty(sparkHome)) {
            logger.info("SPARK_HOME was set to " + sparkHome);
            return sparkHome;
        }

        return getKylinHome() + File.separator + "spark";
    }

    // backward compatibility check happens when properties is loaded or updated
    static BackwardCompatibilityConfig BCC = new BackwardCompatibilityConfig();

    // ============================================================================

    volatile Properties properties = new Properties();

    public KylinConfigBase() {
        this(new Properties());
    }

    public KylinConfigBase(Properties props) {
        this.properties = BCC.check(props);
    }

    protected KylinConfigBase(Properties props, boolean force) {
        this.properties = force ? props : BCC.check(props);
    }

    final protected String getOptional(String prop) {
        return getOptional(prop, null);
    }

    protected String getOptional(String prop, String dft) {
        final String property = System.getProperty(prop);
        return property != null ? property : properties.getProperty(prop, dft);
    }

    protected Properties getAllProperties() {
        return properties;
    }

    final protected Map<String, String> getPropertiesByPrefix(String prefix) {
        Map<String, String> result = Maps.newLinkedHashMap();
        for (Entry<Object, Object> entry : getAllProperties().entrySet()) {
            String key = (String) entry.getKey();
            if (key.startsWith(prefix)) {
                result.put(key.substring(prefix.length()), (String) entry.getValue());
            }
        }
        return result;
    }

    final protected String[] getOptionalStringArray(String prop, String[] dft) {
        final String property = getOptional(prop);
        if (!StringUtils.isBlank(property)) {
            return property.split("\\s*,\\s*");
        } else {
            return dft;
        }
    }

    final protected int[] getOptionalIntArray(String prop, String[] dft) {
        String[] strArray = getOptionalStringArray(prop, dft);
        int[] intArray = new int[strArray.length];
        for (int i = 0; i < strArray.length; i++) {
            intArray[i] = Integer.parseInt(strArray[i]);
        }
        return intArray;
    }

    final public String getRequired(String prop) {
        String r = getOptional(prop);
        if (StringUtils.isEmpty(r)) {
            throw new IllegalArgumentException("missing '" + prop + "' in conf/kylin.properties");
        }
        return r;
    }

    /**
     * Use with care, properties should be read-only. This is for testing mostly.
     */
    final public void setProperty(String key, String value) {
        logger.info("Kylin Config was updated with " + key + " : " + value);
        properties.setProperty(BCC.check(key), value);
    }

    final protected void reloadKylinConfig(Properties properties) {
        this.properties = BCC.check(properties);
    }

    private Map<Integer, String> convertKeyToInteger(Map<String, String> map) {
        Map<Integer, String> result = Maps.newLinkedHashMap();
        for (Entry<String, String> entry : map.entrySet()) {
            result.put(Integer.valueOf(entry.getKey()), entry.getValue());
        }
        return result;
    }

    public String toString() {
        return getMetadataUrl();
    }

    // ============================================================================
    // ENV
    // ============================================================================

    public boolean isDevEnv() {
        return "DEV".equals(getOptional("kylin.env", "DEV"));
    }

    public String getDeployEnv() {
        return getOptional("kylin.env", "DEV");
    }

    public String getHdfsWorkingDirectory() {
        String root = getRequired("kylin.env.hdfs-working-dir");
        if (!root.endsWith("/")) {
            root += "/";
        }

        // make sure path qualified
        if (!root.contains("://")) {
            if (!root.startsWith("/"))
                root = "hdfs:///" + root;
            else
                root = "hdfs://" + root;
        }

        return new StringBuffer(root).append(StringUtils.replaceChars(getMetadataUrlPrefix(), ':', '-')).append("/").toString();
    }

    // ============================================================================
    // METADATA
    // ============================================================================

    public String getMetadataUrl() {
        return getOptional("kylin.metadata.url");
    }

    // for test only
    public void setMetadataUrl(String metadataUrl) {
        setProperty("kylin.metadata.url", metadataUrl);
    }

    public String getMetadataUrlPrefix() {
        String metadataUrl = getMetadataUrl();
        String defaultPrefix = "kylin_metadata";

        int cut = metadataUrl.lastIndexOf('@');
        if (cut > 0) {
            return metadataUrl.substring(0, cut);
        }
        return defaultPrefix;
    }

    public String[] getRealizationProviders() {
        return getOptionalStringArray("kylin.metadata.realization-providers", //
                new String[] { "org.apache.kylin.cube.CubeManager", "org.apache.kylin.storage.hybrid.HybridManager" });
    }

    public String[] getCubeDimensionCustomEncodingFactories() {
        return getOptionalStringArray("kylin.metadata.custom-dimension-encodings", new String[0]);
    }

    public Map<String, String> getCubeCustomMeasureTypes() {
        return getPropertiesByPrefix("kylin.metadata.custom-measure-types.");
    }

    // ============================================================================
    // DICTIONARY & SNAPSHOT
    // ============================================================================

    public boolean isUseForestTrieDictionary() {
        return Boolean.parseBoolean(getOptional("kylin.dictionary.use-forest-trie", "true"));
    }

    public int getTrieDictionaryForestMaxTrieSizeMB() {
        return Integer.parseInt(getOptional("kylin.dictionary.forest-trie-max-mb", "500"));
    }

    public int getCachedDictMaxEntrySize() {
        return Integer.parseInt(getOptional("kylin.dictionary.max-cache-entry", "3000"));
    }

    public boolean isGrowingDictEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.dictionary.growing-enabled", "false"));
    }

    public int getAppendDictEntrySize() {
        return Integer.parseInt(getOptional("kylin.dictionary.append-entry-size", "10000000"));
    }

    public int getAppendDictMaxVersions() {
        return Integer.parseInt(getOptional("kylin.dictionary.append-max-versions", "3"));
    }

    public int getAppendDictVersionTTL() {
        return Integer.parseInt(getOptional("kylin.dictionary.append-version-ttl", "259200000"));
    }

    public int getCachedSnapshotMaxEntrySize() {
        return Integer.parseInt(getOptional("kylin.snapshot.max-cache-entry", "500"));
    }

    public int getTableSnapshotMaxMB() {
        return Integer.parseInt(getOptional("kylin.snapshot.max-mb", "300"));
    }

    // ============================================================================
    // CUBE
    // ============================================================================

    public double getJobCuboidSizeRatio() {
        return Double.parseDouble(getOptional("kylin.cube.size-estimate-ratio", "0.25"));
    }

    @Deprecated
    public double getJobCuboidSizeMemHungryRatio() {
        return Double.parseDouble(getOptional("kylin.cube.size-estimate-memhungry-ratio", "0.05"));
    }

    public double getJobCuboidSizeCountDistinctRatio() {
        return Double.parseDouble(getOptional("kylin.cube.size-estimate-countdistinct-ratio", "0.05"));
    }

    public String getCubeAlgorithm() {
        return getOptional("kylin.cube.algorithm", "auto");
    }

    public double getCubeAlgorithmAutoThreshold() {
        return Double.parseDouble(getOptional("kylin.cube.algorithm.layer-or-inmem-threshold", "7"));
    }

    public int getCubeAlgorithmAutoMapperLimit() {
        return Integer.parseInt(getOptional("kylin.cube.algorithm.inmem-split-limit", "500"));
    }

    public int getCubeAlgorithmInMemConcurrentThreads() {
        return Integer.parseInt(getOptional("kylin.cube.algorithm.inmem-concurrent-threads", "1"));
    }

    public boolean isIgnoreCubeSignatureInconsistency() {
        return Boolean.parseBoolean(getOptional("kylin.cube.ignore-signature-inconsistency", "false"));
    }

    public int getCubeAggrGroupMaxCombination() {
        return Integer.parseInt(getOptional("kylin.cube.aggrgroup.max-combination", "4096"));
    }

    public boolean getCubeAggrGroupIsMandatoryOnlyValid() {
        return Boolean.parseBoolean(getOptional("kylin.cube.aggrgroup.is-mandatory-only-valid", "false"));
    }

    public int getMaxBuildingSegments() {
        return Integer.parseInt(getOptional("kylin.cube.max-building-segments", "10"));
    }

    public boolean allowCubeAppearInMultipleProjects() {
        return Boolean.parseBoolean(getOptional("kylin.cube.allow-appear-in-multiple-projects", "false"));
    }

    // ============================================================================
    // JOB
    // ============================================================================

    public CliCommandExecutor getCliCommandExecutor() throws IOException {
        CliCommandExecutor exec = new CliCommandExecutor();
        if (getRunAsRemoteCommand()) {
            exec.setRunAtRemote(getRemoteHadoopCliHostname(), getRemoteHadoopCliPort(), getRemoteHadoopCliUsername(), getRemoteHadoopCliPassword());
        }
        return exec;
    }

    public String getKylinJobLogDir() {
        return getOptional("kylin.job.log-dir", "/tmp/kylin/logs");
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

    public String getCliWorkingDir() {
        return getOptional("kylin.job.remote-cli-working-dir");
    }

    public boolean isEmptySegmentAllowed() {
        return Boolean.parseBoolean(getOptional("kylin.job.allow-empty-segment", "true"));
    }

    public int getMaxConcurrentJobLimit() {
        return Integer.parseInt(getOptional("kylin.job.max-concurrent-jobs", "10"));
    }

    public int getCubingInMemSamplingPercent() {
        int percent = Integer.parseInt(this.getOptional("kylin.job.sampling-percentage", "100"));
        percent = Math.max(percent, 1);
        percent = Math.min(percent, 100);
        return percent;
    }

    public String getHiveDependencyFilterList() {
        return this.getOptional("kylin.job.dependency-filter-list", "[^,]*hive-exec[^,]*?\\.jar" + "|" + "[^,]*hive-metastore[^,]*?\\.jar" + "|" + "[^,]*hive-hcatalog-core[^,]*?\\.jar");
    }

    public boolean isMailEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-enabled", "false"));
    }

    public boolean isStarttlsEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-mail-enable-starttls", "false"));
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
        return getOptionalStringArray("kylin.job.notification-admin-emails", null);
    }

    public int getJobRetry() {
        return Integer.parseInt(this.getOptional("kylin.job.retry", "0"));
    }

    public int getCubeStatsHLLPrecision() {
        return Integer.parseInt(getOptional("kylin.job.sampling-hll-precision", "14"));
    }

    public String getJobControllerLock() {
        return getOptional("kylin.job.lock", "org.apache.kylin.storage.hbase.util.ZookeeperJobLock");
    }

    public Map<Integer, String> getSchedulers() {
        Map<Integer, String> r = Maps.newLinkedHashMap();
        r.put(0, "org.apache.kylin.job.impl.threadpool.DefaultScheduler");
        r.put(2, "org.apache.kylin.job.impl.threadpool.DistributedScheduler");
        r.putAll(convertKeyToInteger(getPropertiesByPrefix("kylin.job.scheduler.provider.")));
        return r;
    }

    public Integer getSchedulerType() {
        return Integer.parseInt(getOptional("kylin.job.scheduler.default", "0"));
    }

    public Integer getErrorRecordThreshold() {
        return Integer.parseInt(getOptional("kylin.job.error-record-threshold", "0"));
    }

    // ============================================================================
    // SOURCE.HIVE
    // ============================================================================

    public Map<Integer, String> getSourceEngines() {
        Map<Integer, String> r = Maps.newLinkedHashMap();
        // ref constants in ISourceAware
        r.put(0, "org.apache.kylin.source.hive.HiveSource");
        r.put(1, "org.apache.kylin.source.kafka.KafkaSource");
        r.putAll(convertKeyToInteger(getPropertiesByPrefix("kylin.source.provider.")));
        return r;
    }

    /**
     * was for route to hive, not used any more
     */
    @Deprecated
    public String getHiveUrl() {
        return getOptional("kylin.source.hive.connection-url", "");
    }

    /**
     * was for route to hive, not used any more
     */
    @Deprecated
    public String getHiveUser() {
        return getOptional("kylin.source.hive.connection-user", "");
    }

    /**
     * was for route to hive, not used any more
     */
    @Deprecated
    public String getHivePassword() {
        return getOptional("kylin.source.hive.connection-password", "");
    }

    public Map<String, String> getHiveConfigOverride() {
        return getPropertiesByPrefix("kylin.source.hive.config-override.");
    }

    public String getOverrideHiveTableLocation(String table) {
        return getOptional("kylin.source.hive.table-location." + table.toUpperCase());
    }

    public boolean isHiveKeepFlatTable() {
        return Boolean.parseBoolean(this.getOptional("kylin.source.hive.keep-flat-table", "false"));
    }

    public String getHiveDatabaseForIntermediateTable() {
        return this.getOptional("kylin.source.hive.database-for-flat-table", "default");
    }

    public boolean isHiveRedistributeEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.source.hive.redistribute-flat-table", "true"));
    }

    public String getHiveClientMode() {
        return getOptional("kylin.source.hive.client", "cli");
    }

    public String getHiveBeelineParams() {
        return getOptional("kylin.source.hive.beeline-params", "");
    }

    public String getFlatHiveTableClusterByDictColumn() {
        return getOptional("kylin.source.hive.flat-table-cluster-by-dict-column");
    }

    // ============================================================================
    // SOURCE.KAFKA
    // ============================================================================

    public Map<String, String> getKafkaConfigOverride() {
        return getPropertiesByPrefix("kylin.source.kafka.config-override.");
    }

    // ============================================================================
    // STORAGE.HBASE
    // ============================================================================

    public Map<Integer, String> getStorageEngines() {
        Map<Integer, String> r = Maps.newLinkedHashMap();
        // ref constants in IStorageAware
        r.put(0, "org.apache.kylin.storage.hbase.HBaseStorage");
        r.put(1, "org.apache.kylin.storage.hybrid.HybridStorage");
        r.put(2, "org.apache.kylin.storage.hbase.HBaseStorage");
        r.putAll(convertKeyToInteger(getPropertiesByPrefix("kylin.storage.provider.")));
        return r;
    }

    public int getDefaultStorageEngine() {
        return Integer.parseInt(getOptional("kylin.storage.default", "2"));
    }

    public String getStorageUrl() {
        return getOptional("kylin.storage.url");
    }

    public String getHBaseClusterFs() {
        return getOptional("kylin.storage.hbase.cluster-fs", "");
    }

    public String getHBaseClusterHDFSConfigFile() {
        return getOptional("kylin.storage.hbase.cluster-hdfs-config-file", "");
    }

    private static final Pattern COPROCESSOR_JAR_NAME_PATTERN = Pattern.compile("kylin-coprocessor-(.+)\\.jar");
    private static final Pattern JOB_JAR_NAME_PATTERN = Pattern.compile("kylin-job-(.+)\\.jar");

    public String getCoprocessorLocalJar() {
        final String coprocessorJar = getOptional("kylin.storage.hbase.coprocessor-local-jar");
        if (StringUtils.isNotEmpty(coprocessorJar)) {
            return coprocessorJar;
        }
        String kylinHome = getKylinHome();
        if (StringUtils.isEmpty(kylinHome)) {
            throw new RuntimeException("getCoprocessorLocalJar needs KYLIN_HOME");
        }
        return getFileName(kylinHome + File.separator + "lib", COPROCESSOR_JAR_NAME_PATTERN);
    }

    public void overrideCoprocessorLocalJar(String path) {
        logger.info("override " + "kylin.storage.hbase.coprocessor-local-jar" + " to " + path);
        System.setProperty("kylin.storage.hbase.coprocessor-local-jar", path);
    }

    private static String getFileName(String homePath, Pattern pattern) {
        File home = new File(homePath);
        SortedSet<String> files = Sets.newTreeSet();
        if (home.exists() && home.isDirectory()) {
            File[] listFiles = home.listFiles();
            if (listFiles != null) {
                for (File file : listFiles) {
                    final Matcher matcher = pattern.matcher(file.getName());
                    if (matcher.matches()) {
                        files.add(file.getAbsolutePath());
                    }
                }
            }
        }
        if (files.isEmpty()) {
            throw new RuntimeException("cannot find " + pattern.toString() + " in " + homePath);
        } else {
            return files.last();
        }
    }

    public int getHBaseRegionCountMin() {
        return Integer.parseInt(getOptional("kylin.storage.hbase.min-region-count", "1"));
    }

    public int getHBaseRegionCountMax() {
        return Integer.parseInt(getOptional("kylin.storage.hbase.max-region-count", "500"));
    }

    public float getHBaseHFileSizeGB() {
        return Float.parseFloat(getOptional("kylin.storage.hbase.hfile-size-gb", "2.0"));
    }

    public boolean getQueryRunLocalCoprocessor() {
        return Boolean.parseBoolean(getOptional("kylin.storage.hbase.run-local-coprocessor", "false"));
    }

    public double getQueryCoprocessorMemGB() {
        return Double.parseDouble(this.getOptional("kylin.storage.hbase.coprocessor-mem-gb", "3.0"));
    }

    public boolean getQueryCoprocessorSpillEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.storage.partition.aggr-spill-enabled", "true"));
    }

    public long getPartitionMaxScanBytes() {
        long value = Long.parseLong(this.getOptional("kylin.storage.partition.max-scan-bytes", String.valueOf(3L * 1024 * 1024 * 1024)));
        return value > 0 ? value : Long.MAX_VALUE;
    }

    public int getQueryCoprocessorTimeoutSeconds() {
        return Integer.parseInt(this.getOptional("kylin.storage.hbase.coprocessor-timeout-seconds", "0"));
    }

    public int getQueryScanFuzzyKeyMax() {
        return Integer.parseInt(this.getOptional("kylin.storage.hbase.max-fuzzykey-scan", "200"));
    }

    public int getQueryStorageVisitScanRangeMax() {
        return Integer.valueOf(this.getOptional("kylin.storage.hbase.max-visit-scanrange", "1000000"));
    }

    public String getDefaultIGTStorage() {
        return getOptional("kylin.storage.hbase.gtstorage", "org.apache.kylin.storage.hbase.cube.v2.CubeHBaseEndpointRPC");
    }

    public int getHBaseScanCacheRows() {
        return Integer.parseInt(this.getOptional("kylin.storage.hbase.scan-cache-rows", "1024"));
    }

    public float getKylinHBaseRegionCut() {
        return Float.valueOf(getOptional("kylin.storage.hbase.region-cut-gb", "5.0"));
    }

    public int getHBaseScanMaxResultSize() {
        return Integer.parseInt(this.getOptional("kylin.storage.hbase.max-scan-result-bytes", "" + (5 * 1024 * 1024))); // 5 MB
    }

    public String getHbaseDefaultCompressionCodec() {
        return getOptional("kylin.storage.hbase.compression-codec", "none");
    }

    public String getHbaseDefaultEncoding() {
        return getOptional("kylin.storage.hbase.rowkey-encoding", "FAST_DIFF");
    }

    public int getHbaseDefaultBlockSize() {
        return Integer.valueOf(getOptional("kylin.storage.hbase.block-size-bytes", "1048576"));
    }

    public int getHbaseSmallFamilyBlockSize() {
        return Integer.valueOf(getOptional("kylin.storage.hbase.small-family-block-size-bytes", "65536"));
    }

    public String getKylinOwner() {
        return this.getOptional("kylin.storage.hbase.owner-tag", "");
    }

    public boolean getCompressionResult() {
        return Boolean.parseBoolean(getOptional("kylin.storage.hbase.endpoint-compress-result", "true"));
    }

    public int getHBaseMaxConnectionThreads() {
        return Integer.parseInt(getOptional("kylin.storage.hbase.max-hconnection-threads", "2048"));
    }

    public int getHBaseCoreConnectionThreads() {
        return Integer.parseInt(getOptional("kylin.storage.hbase.core-hconnection-threads", "2048"));
    }

    public long getHBaseConnectionThreadPoolAliveSeconds() {
        return Long.parseLong(getOptional("kylin.storage.hbase.hconnection-threads-alive-seconds", "60"));
    }

    // ============================================================================
    // ENGINE.MR
    // ============================================================================

    public Map<Integer, String> getJobEngines() {
        Map<Integer, String> r = Maps.newLinkedHashMap();
        // ref constants in IEngineAware
        r.put(0, "org.apache.kylin.engine.mr.MRBatchCubingEngine"); //IEngineAware.ID_MR_V1
        r.put(2, "org.apache.kylin.engine.mr.MRBatchCubingEngine2"); //IEngineAware.ID_MR_V2
        r.put(4, "org.apache.kylin.engine.spark.SparkBatchCubingEngine2"); //IEngineAware.ID_SPARK
        r.putAll(convertKeyToInteger(getPropertiesByPrefix("kylin.engine.provider.")));
        return r;
    }

    public int getDefaultCubeEngine() {
        return Integer.parseInt(getOptional("kylin.engine.default", "2"));
    }

    public String getKylinJobJarPath() {
        final String jobJar = getOptional("kylin.engine.mr.job-jar");
        if (StringUtils.isNotEmpty(jobJar)) {
            return jobJar;
        }
        String kylinHome = getKylinHome();
        if (StringUtils.isEmpty(kylinHome)) {
            return "";
        }
        return getFileName(kylinHome + File.separator + "lib", JOB_JAR_NAME_PATTERN);
    }

    public void overrideMRJobJarPath(String path) {
        logger.info("override " + "kylin.engine.mr.job-jar" + " to " + path);
        System.setProperty("kylin.engine.mr.job-jar", path);
    }

    public String getKylinJobMRLibDir() {
        return getOptional("kylin.engine.mr.lib-dir", "");
    }

    public Map<String, String> getMRConfigOverride() {
        return getPropertiesByPrefix("kylin.engine.mr.config-override.");
    }

    public Map<String, String> getSparkConfigOverride() {
        return getPropertiesByPrefix("kylin.engine.spark-conf.");
    }

    public double getDefaultHadoopJobReducerInputMB() {
        return Double.parseDouble(getOptional("kylin.engine.mr.reduce-input-mb", "500"));
    }

    public double getDefaultHadoopJobReducerCountRatio() {
        return Double.parseDouble(getOptional("kylin.engine.mr.reduce-count-ratio", "1.0"));
    }

    public int getHadoopJobMinReducerNumber() {
        return Integer.parseInt(getOptional("kylin.engine.mr.min-reducer-number", "1"));
    }

    public int getHadoopJobMaxReducerNumber() {
        return Integer.parseInt(getOptional("kylin.engine.mr.max-reducer-number", "500"));
    }

    public int getHadoopJobMapperInputRows() {
        return Integer.parseInt(getOptional("kylin.engine.mr.mapper-input-rows", "1000000"));
    }

    //UHC: ultra high cardinality columns, contain the ShardByColumns and the GlobalDictionaryColumns
    public int getUHCReducerCount() {
        return Integer.parseInt(getOptional("kylin.engine.mr.uhc-reducer-count", "1"));
    }

    public boolean isBuildDictInReducerEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.mr.build-dict-in-reducer", "true"));
    }

    public String getYarnStatusCheckUrl() {
        return getOptional("kylin.engine.mr.yarn-check-status-url", null);
    }

    public int getYarnStatusCheckIntervalSeconds() {
        return Integer.parseInt(getOptional("kylin.engine.mr.yarn-check-interval-seconds", "10"));
    }

    // ============================================================================
    // ENGINE.SPARK
    // ============================================================================

    public String getHadoopConfDir() {
        return getOptional("kylin.env.hadoop-conf-dir", "");
    }

    public String getSparkAdditionalJars() {
        return getOptional("kylin.engine.spark.additional-jars", "");
    }

    public float getSparkRDDPartitionCutMB() {
        return Float.valueOf(getOptional("kylin.engine.spark.rdd-partition-cut-mb", "10.0"));
    }

    public int getSparkMinPartition() {
        return Integer.valueOf(getOptional("kylin.engine.spark.min-partition", "1"));
    }

    public int getSparkMaxPartition() {
        return Integer.valueOf(getOptional("kylin.engine.spark.max-partition", "5000"));
    }

    public boolean isSparkSanityCheckEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.spark.sanity-check-enabled", "false"));
    }

    // ============================================================================
    // QUERY
    // ============================================================================

    //check KYLIN-1684, in most cases keep the default value
    public boolean isSkippingEmptySegments() {
        return Boolean.valueOf(getOptional("kylin.query.skip-empty-segments", "true"));
    }

    public boolean isStreamAggregateEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.stream-aggregate-enabled", "true"));
    }

    @Deprecated //Limit is good even it's large. This config is meaning less since we already have scan threshold 
    public int getStoragePushDownLimitMax() {
        return Integer.parseInt(getOptional("kylin.query.max-limit-pushdown", "10000"));
    }

    @Deprecated
    public int getScanThreshold() {
        return Integer.parseInt(getOptional("kylin.query.scan-threshold", "10000000"));
    }

    public long getQueryMaxScanBytes() {
        long value = Long.parseLong(getOptional("kylin.query.max-scan-bytes", "0"));
        return value > 0 ? value : Long.MAX_VALUE;
    }

    public int getLargeQueryThreshold() {
        return Integer.parseInt(getOptional("kylin.query.large-query-threshold", String.valueOf(1000000)));
    }

    public int getDerivedInThreshold() {
        return Integer.parseInt(getOptional("kylin.query.derived-filter-translation-threshold", "20"));
    }

    public int getBadQueryStackTraceDepth() {
        return Integer.parseInt(getOptional("kylin.query.badquery-stacktrace-depth", "10"));
    }

    public int getBadQueryHistoryNum() {
        return Integer.parseInt(getOptional("kylin.query.badquery-history-number", "10"));
    }

    public int getBadQueryDefaultAlertingSeconds() {
        return Integer.parseInt(getOptional("kylin.query.badquery-alerting-seconds", "90"));
    }

    public int getBadQueryDefaultDetectIntervalSeconds() {
        return Integer.parseInt(getOptional("kylin.query.badquery-detect-interval", "60"));
    }

    public boolean getBadQueryPersistentEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.badquery-persistent-enabled", "true"));
    }

    public String[] getQueryTransformers() {
        return getOptionalStringArray("kylin.query.transformers", new String[0]);
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

    public boolean isQuerySecureEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.security-enabled", "true"));
    }

    public boolean isQueryCacheEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.cache-enabled", "true"));
    }

    public boolean isQueryIgnoreUnknownFunction() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.ignore-unknown-function", "false"));
    }

    public String getQueryAccessController() {
        return getOptional("kylin.query.access-controller", null);
    }

    public int getDimCountDistinctMaxCardinality() {
        return Integer.parseInt(getOptional("kylin.query.max-dimension-count-distinct", "5000000"));
    }

    public Map<String, String> getUDFs() {
        Map<String, String> udfMap = getPropertiesByPrefix("kylin.query.udf.");
        return udfMap;
    }

    public int getQueryTimeoutSeconds() {
        return Integer.parseInt(this.getOptional("kylin.query.timeout-seconds", "0"));
    }

    // ============================================================================
    // SERVER
    // ============================================================================

    public String getServerMode() {
        return this.getOptional("kylin.server.mode", "all");
    }

    public String[] getRestServers() {
        return getOptionalStringArray("kylin.server.cluster-servers", new String[0]);
    }

    public String getClusterName() {
        return this.getOptional("kylin.server.cluster-name", getMetadataUrlPrefix());
    }

    public String getInitTasks() {
        return getOptional("kylin.server.init-tasks");
    }

    public int getWorkersPerServer() {
        //for sequence sql use
        return Integer.parseInt(getOptional("kylin.server.sequence-sql.workers-per-server", "1"));
    }

    public long getSequenceExpireTime() {
        return Long.valueOf(this.getOptional("kylin.server.sequence-sql.expire-time", "86400000"));//default a day
    }

    public boolean getQueryMetricsEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.server.query-metrics-enabled", "false"));
    }

    public int[] getQueryMetricsPercentilesIntervals() {
        String[] dft = { "60", "300", "3600" };
        return getOptionalIntArray("kylin.server.query-metrics-percentiles-intervals", dft);
    }

    // ============================================================================
    // WEB
    // ============================================================================

    public String getTimeZone() {
        return getOptional("kylin.web.timezone", "PST");
    }

    public boolean isWebCrossDomainEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.web.cross-domain-enabled", "true"));
    }

    //ResourceStore Impl
    /*public String getResourceStoreImpl() {
        return getOptional("kylin.metadata.default-resource-store-impl", "org.apache.kylin.storage.hbase.HBaseResourceStore");
    }*/

    public Map<String, String> getResourceStoreImpls() {
        Map<String, String> r = Maps.newLinkedHashMap();
        // ref constants in ISourceAware
        r.put("", "org.apache.kylin.common.persistence.FileResourceStore");
        r.put("hbase", "org.apache.kylin.storage.hbase.HBaseResourceStore");
        r.put("hdfs", "org.apache.kylin.storage.hdfs.HDFSResourceStore");
        r.putAll(getPropertiesByPrefix("kylin.resource.store.provider."));
        return r;
    }

    public String getResourceStoreImpl() {
        String metadataUrl = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        int cut = metadataUrl.indexOf('@');
        String key = cut < 0 ? "" : metadataUrl.substring(cut + 1);
        return getResourceStoreImpls().get(key);
    }

}
